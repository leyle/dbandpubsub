package kafkaconnector

import (
	"context"
	"errors"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"time"
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

// topicWorker holds the channel and control structures for a single topic's worker
type topicWorker struct {
	topic  string
	msgCh  chan *kafka.Message
	doneCh chan struct{}
}

// ConsumeEventConcurrently consumes messages from multiple topics concurrently.
// Each topic gets its own worker goroutine, allowing parallel processing across topics.
// Messages within the same topic are still processed sequentially (one by one).
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

	// Create workers for each topic
	workers := make(map[string]*topicWorker)
	var wg sync.WaitGroup

	bufferSize := ec.opt.WorkerBufferSize
	if bufferSize <= 0 {
		bufferSize = defaultWorkerBufferSize
	}

	for _, topic := range topics {
		worker := &topicWorker{
			topic:  topic,
			msgCh:  make(chan *kafka.Message, bufferSize),
			doneCh: make(chan struct{}),
		}
		workers[topic] = worker

		wg.Add(1)
		go ec.runTopicWorker(ctx, worker, callback, &wg)
	}

	logger.Info().Int("workerCount", len(workers)).Msg("started topic workers")

	// Main dispatcher loop - reads messages and routes to topic workers
	dispatcherDone := make(chan error, 1)
	go func() {
		dispatcherDone <- ec.runDispatcher(ctx, workers)
	}()

	// Wait for dispatcher to finish (either by stop signal or error)
	dispatchErr := <-dispatcherDone

	// Signal all workers to stop by closing their message channels
	for _, worker := range workers {
		close(worker.msgCh)
	}

	// Wait for all workers to finish processing remaining messages
	wg.Wait()
	logger.Info().Msg("all topic workers have stopped")

	return dispatchErr
}

// runDispatcher reads messages from Kafka and routes them to the appropriate topic worker
func (ec *EventConnector) runDispatcher(ctx context.Context, workers map[string]*topicWorker) error {
	logger := zerolog.Ctx(ctx)

	for {
		select {
		case <-ec.consumer.stopCh:
			logger.Warn().Msg("dispatcher received stop signal")
			return nil
		case chErr := <-ec.consumer.errCh:
			logger.Error().Err(chErr).Msg("dispatcher received error from channel")
			return chErr
		default:
			msg, err := ec.consumer.ReadMessage(time.Second * 1) // Use timeout to allow checking stop signal
			if err != nil {
				// Check if it's a timeout (no message available)
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue // No message, loop again to check stop signal
				}
				logger.Error().Err(err).Msg("error occurred when reading message")
				return err
			}

			// Route message to appropriate topic worker
			topic := *msg.TopicPartition.Topic
			worker, exists := workers[topic]
			if !exists {
				logger.Warn().Str("topic", topic).Msg("received message for unknown topic, skipping")
				continue
			}

			// Send message to worker channel (may block if buffer is full)
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

// runTopicWorker processes messages for a single topic sequentially
func (ec *EventConnector) runTopicWorker(ctx context.Context, worker *topicWorker, callback eventCallback, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(worker.doneCh)

	logger := zerolog.Ctx(ctx).With().Str("workerTopic", worker.topic).Logger()
	logger.Info().Msg("topic worker started")

	for msg := range worker.msgCh {
		// Process message with retry logic
		err := ec.executeCallbackWithRetry(ctx, msg, callback)
		if err != nil {
			logger.Error().Err(err).Str("key", string(msg.Key)).Msg("callback() returned error")
			// Don't stop the worker, continue processing next message
		}

		// Commit to store
		_, err = ec.consumer.StoreMessage(msg)
		if err != nil {
			logger.Error().Err(err).Str("key", string(msg.Key)).Msg("commit msg failed")
		}
	}

	logger.Info().Msg("topic worker stopped")
}
