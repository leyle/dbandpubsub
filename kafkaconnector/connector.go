package kafkaconnector

import (
	"context"
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
	ec.consumer.Close()
	ec.producer.Close()
	ec.adminClient.Close()
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
