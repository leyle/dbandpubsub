package kafkaconnector

import (
	"context"
	"github.com/rs/zerolog"
	"time"
)

type EventConnector struct {
	opt         *EventOption
	producer    *eventProducer
	consumer    *eventConsumer
	adminClient *eventAdminClient
}

func NewEventConnector(opt *EventOption) (*EventConnector, error) {
	parseOption(opt)

	ec := &EventConnector{
		opt: opt,
	}

	// initial producer
	producer, err := newProducer(opt)
	if err != nil {
		return nil, err
	}
	ec.producer = producer

	// initial consumer
	consumer, err := newConsumer(opt)
	if err != nil {
		return nil, err
	}
	ec.consumer = consumer

	// initial admin client
	if opt.NeedAdmin {
		admin, err := newEventAdminClient(opt)
		if err != nil {
			return nil, err
		}
		ec.adminClient = admin
	}

	return ec, nil
}

func (ec *EventConnector) Stop(ctx context.Context) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("try to stop kafka client")

	// Signal consumer to stop gracefully first
	select {
	case ec.consumer.stopCh <- true:
		logger.Debug().Msg("sent stop signal to consumer")
	default:
		logger.Debug().Msg("stop channel already closed or full")
	}

	ec.consumer.Close()
	ec.producer.Close()
	if ec.opt.NeedAdmin {
		ec.adminClient.Close()
	}
}

func (ec *EventConnector) Reconnect(ctx context.Context) (*EventConnector, error) {
	logger := zerolog.Ctx(ctx)
	logger.Info().Msg("try to stop and reconnect kafka")
	ec.Stop(ctx)

	opt := ec.opt
	newEC, err := NewEventConnector(opt)
	if err != nil {
		logger.Error().Err(err).Msg("failed to reconnect to kafka")
		return nil, err
	}

	return newEC, nil
}

func parseOption(opt *EventOption) {
	opt.err = nil

	if opt.RetryCount <= 0 {
		opt.RetryCount = defaultRetryCount
	}

	if opt.RetryDelay <= time.Duration(1)*time.Second {
		opt.RetryDelay = time.Duration(defaultRetryDelay) * time.Second
	}

	if opt.Timeout <= time.Duration(1)*time.Second {
		opt.Timeout = time.Duration(defaultAdminTimeout) * time.Second
	}

	if opt.SessionTimeout <= 0 {
		opt.SessionTimeout = defaultSessionTimeout
	}

	if opt.OffsetReset == "" {
		opt.OffsetReset = defaultOffsetRest
	}

	if opt.DLQTopic == "" {
		opt.DLQTopic = defaultDQLTopic
	}
}
