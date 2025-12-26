package kafkaconnector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

type eventConsumer struct {
	*kafka.Consumer
	stopCh     chan bool
	errCh      chan error
	retryCount int
	retryDelay int // seconds
	opt        *EventOption
}

func newConsumer(opt *EventOption) (*eventConsumer, error) {
	conf := kafka.ConfigMap{
		"bootstrap.servers":        opt.GetServers(),
		"broker.address.family":    "v4",
		"group.id":                 opt.GroupId,
		"session.timeout.ms":       opt.SessionTimeout,
		"auto.offset.reset":        opt.OffsetReset,
		"enable.auto.offset.store": opt.OffsetAutoStore,
	}
	if opt.MoreOptions != nil && len(opt.MoreOptions) > 0 {
		for k, v := range opt.MoreOptions {
			conf[k] = v
		}
	}

	c, err := kafka.NewConsumer(&conf)
	if err != nil {
		return nil, err
	}

	cc := &eventConsumer{
		opt:        opt,
		stopCh:     make(chan bool),
		errCh:      make(chan error, 10),
		retryCount: defaultRetryCount,
		retryDelay: defaultRetryDelay,
	}
	cc.Consumer = c

	return cc, nil
}

type eventCallback func(ctx context.Context, msg *kafka.Message) error

func (ec *EventConnector) ConsumeEvent(ctx context.Context, topics []string, callback eventCallback) error {
	logger := zerolog.Ctx(ctx)
	if topics == nil || len(topics) == 0 {
		logger.Error().Msg("no topics parameter when calling ConsumeEvent function")
		return errors.New("topics list must be passed into this function")

	}
	if callback == nil {
		logger.Error().Msg("no callback parameter when calling ConsumeEvent function")
		return errors.New("callback must be passed into this function")
	}
	logger.Debug().Strs("topics", topics).Msg("start consuming events")

	err := ec.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		logger.Error().Err(err).Msg("subscribe topics failed, no retry, just return error to upstream")
		return err
	}
	logger.Debug().Strs("topics", topics).Msg("successfully subscribed")

	for {
		select {
		case <-ec.consumer.stopCh:
			logger.Warn().Msg("received stop signal, consumer stopped")
			return nil
		case chErr := <-ec.consumer.errCh:
			logger.Error().Msg("error occurred, return error to upstream caller")
			return chErr
		default:
			msg, err := ec.consumer.ReadMessage(-1)
			if err != nil {
				logger.Error().Err(err).Msg("error occurred when read message")
				ec.consumer.errCh <- err
				return err
			}

			// call msg callback function
			err = ec.executeCallbackWithRetry(ctx, msg, callback)
			if err != nil {
				// callback failed, what strategy do we do?
				// todo
				logger.Error().Err(err).Msg("callback() returned error")
				ec.consumer.errCh <- err
			}

			// commit to store
			_, err = ec.consumer.StoreMessage(msg)
			if err != nil {
				logger.Error().Err(err).Msg("commit msg failed")
				ec.consumer.errCh <- err
			}
		}
	}
}

func (ec *EventConnector) HandleError(ctx context.Context) {
	logger := zerolog.Ctx(ctx)
	logger.Debug().Msg("start consumer's error handle function")
	for {
		select {
		case <-ec.consumer.stopCh:
			return
		case err := <-ec.consumer.errCh:
			logger.Error().Err(err).Msg("received error msg from channel")
			// what to do?
		}
	}
}

func (ec *EventConnector) executeCallbackWithRetry(ctx context.Context, msg *kafka.Message, callback eventCallback) error {
	// simply execute callback
	logger := zerolog.Ctx(ctx)
	key := msg.Key
	logger.Debug().
		Str("key", string(key)).
		Int("retry", ec.opt.RetryCount).
		Dur("delay", ec.opt.RetryDelay).
		Msg("start executing consumer callback function")

	delay := ec.opt.RetryDelay
	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for i := 0; i < ec.opt.RetryCount; i++ {
		err := callback(ctx, msg)
		if err == nil {
			logger.Debug().Str("key", string(key)).Msg("successfully consumed kafka msg")
			return nil
		}
		logger.Warn().Str("key", string(key)).Int("retryTimes", i).Msg("execute callback failed, waiting for retry")
		<-ticker.C // equals time.sleep()
	}

	// callback failed after retries, send this msg to Dead Letter Queue
	logger.Error().Str("key", string(key)).Msg("callback parse msg failed, now try to send it to DLQ topic")
	err := ec.sendToDLQ(ctx, msg)
	if err != nil {
		logger.Error().Str("key", string(key)).Msg("failed to send this msg to DLQ, terrible bug happened")
		return err
	}

	return nil
}

func (ec *EventConnector) sendToDLQ(ctx context.Context, msg *kafka.Message) error {
	logger := zerolog.Ctx(ctx)
	key := msg.Key
	val := msg.Value
	topic := msg.TopicPartition.Topic

	logger.Warn().Str("key", string(key)).Str("topic", *topic).Msg("send this failed msg to DLQ")

	// process headers
	reqHeaders := make(map[string]string)
	for _, header := range msg.Headers {
		reqHeaders[header.Key] = string(header.Value)
	}

	event := &Event{
		Topic:   ec.opt.DLQTopic,
		Key:     string(key),
		Val:     val,
		Headers: reqHeaders,
	}

	_, err := ec.SendEvent(ctx, event)
	if err != nil {
		logger.Error().Err(err).Msg("send msg to DLQ failed")
		return err
	}

	logger.Warn().Str("key", string(key)).Str("topic", *topic).Msg("successfully sent failed msg to DLQ")

	return nil
}

type partitionWorker struct {
	id     string // format: "topic-partitionID"
	msgCh  chan *kafka.Message
	doneCh chan struct{}
}

// ConsumeEventConcurrently consumes messages from multiple topics concurrently.
// It creates a dedicated worker goroutine for each Topic+Partition combination.
// This ensures maximum parallelism (up to the number of partitions) while maintaining
// sequential processing order within a specific partition.
func (ec *EventConnector) ConsumeEventConcurrently(ctx context.Context, topics []string, callback eventCallback) error {
	logger := zerolog.Ctx(ctx)
	if topics == nil || len(topics) == 0 {
		logger.Error().Msg("no topics parameter when calling ConsumeEventConcurrently function")
		return errors.New("topics list must be passed into this function")
	}
	if callback == nil {
		logger.Error().Msg("no callback parameter when calling ConsumeEventConcurrently function")
		return errors.New("callback must be passed into this function")
	}

	logger.Debug().Strs("topics", topics).Msg("start consuming events concurrently")

	err := ec.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		logger.Error().Err(err).Msg("subscribe topics failed, no retry, just return error to upstream")
		return err
	}
	logger.Debug().Strs("topics", topics).Msg("successfully subscribed")

	// We do NOT pre-create workers here anymore.
	// Workers will be created lazily in runDispatcher based on assigned partitions.
	workers := make(map[string]*partitionWorker)
	var wg sync.WaitGroup

	// Main dispatcher loop - reads messages and routes to partition workers
	dispatcherDone := make(chan error, 1)

	// We pass the WaitGroup to the dispatcher so it can Add(1) when creating new workers
	go func() {
		dispatcherDone <- ec.runDispatcher(ctx, workers, callback, &wg)
	}()

	// Wait for dispatcher to finish (either by stop signal or error)
	dispatchErr := <-dispatcherDone

	// Signal all workers to stop by closing their message channels
	logger.Info().Int("activeWorkers", len(workers)).Msg("stopping all partition workers")
	for _, worker := range workers {
		close(worker.msgCh)
	}

	// Wait for all workers to finish processing remaining messages
	wg.Wait()
	logger.Info().Msg("all partition workers have stopped")

	return dispatchErr
}

// runDispatcher reads messages from Kafka and routes them to the appropriate partition worker
// It creates new workers dynamically when a message from a new partition is received.
func (ec *EventConnector) runDispatcher(ctx context.Context, workers map[string]*partitionWorker, callback eventCallback, wg *sync.WaitGroup) error {
	logger := zerolog.Ctx(ctx)

	bufferSize := ec.opt.WorkerBufferSize
	if bufferSize <= 0 {
		bufferSize = defaultWorkerBufferSize
	}

	for {
		select {
		case <-ec.consumer.stopCh:
			logger.Warn().Msg("dispatcher received stop signal")
			return nil
		case chErr := <-ec.consumer.errCh:
			logger.Error().Err(chErr).Msg("dispatcher received error from channel")
			return chErr
		default:
			// Read message
			msg, err := ec.consumer.ReadMessage(time.Second * 1)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				logger.Error().Err(err).Msg("error occurred when reading message")
				return err
			}

			// Identify the partition
			topic := *msg.TopicPartition.Topic
			partition := msg.TopicPartition.Partition

			// Create a unique key for this specific partition
			// e.g., "my-topic-0", "my-topic-1"
			workerKey := fmt.Sprintf("%s-%d", topic, partition)

			// Check if we already have a worker for this partition
			worker, exists := workers[workerKey]
			if !exists {
				logger.Info().Str("workerId", workerKey).Msg("spawning new worker for partition")

				worker = &partitionWorker{
					id:     workerKey,
					msgCh:  make(chan *kafka.Message, bufferSize),
					doneCh: make(chan struct{}),
				}
				workers[workerKey] = worker

				wg.Add(1)
				go ec.runPartitionWorker(ctx, worker, callback, wg)
			}

			// Send message to worker channel
			select {
			case <-ec.consumer.stopCh:
				logger.Warn().Msg("dispatcher stopped while sending message to worker")
				return nil
			case worker.msgCh <- msg:
				// Message sent to worker
			}
		}
	}
}

// runPartitionWorker processes messages for a single partition sequentially
func (ec *EventConnector) runPartitionWorker(ctx context.Context, worker *partitionWorker, callback eventCallback, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(worker.doneCh)

	// Create a logger with the worker ID context
	logger := zerolog.Ctx(ctx).With().Str("workerId", worker.id).Logger()
	logger.Debug().Msg("partition worker started")

	for msg := range worker.msgCh {
		// Process message with retry logic
		// Note: Since this worker is exclusive to one partition,
		// processing is sequential and safe for offset commits.
		err := ec.executeCallbackWithRetry(ctx, msg, callback)
		if err != nil {
			logger.Error().Err(err).Str("key", string(msg.Key)).Msg("callback() returned error")
			// We continue to the next message even if callback failed (after retries/DLQ)
		}

		// Commit to store
		_, err = ec.consumer.StoreMessage(msg)
		if err != nil {
			logger.Error().Err(err).Str("key", string(msg.Key)).Msg("commit msg failed")
		}
	}

	logger.Debug().Msg("partition worker stopped")
}
