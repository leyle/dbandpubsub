package kafkaconnector

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

type eventProducer struct {
	*kafka.Producer
	opt *EventOption
}

func newProducer(opt *EventOption) (*eventProducer, error) {
	conf := kafka.ConfigMap{
		"bootstrap.servers": opt.GetServers(),
		"message.max.bytes": defaultRequestMsgSize,
	}
	if opt.MoreOptions != nil && len(opt.MoreOptions) > 0 {
		for k, v := range opt.MoreOptions {
			conf[k] = v
		}
	}

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		return nil, err
	}

	ep := &eventProducer{
		opt: opt,
	}
	ep.Producer = p

	return ep, nil
}

func (ec *EventConnector) SendEvent(ctx context.Context, event *Event) (*kafka.Message, error) {
	if event == nil || event.Topic == "" || event.Key == "" || event.Val == nil {
		return nil, errors.New("invalid event parameter")
	}

	logger := zerolog.Ctx(ctx)
	logger.Debug().Str("topic", event.Topic).Str("key", event.Key).Msg("start sending event")

	tp := kafka.TopicPartition{
		Topic:     &event.Topic,
		Partition: kafka.PartitionAny,
	}

	msg := &kafka.Message{
		TopicPartition: tp,
		Key:            []byte(event.Key),
		Value:          event.Val,
	}

	if event.Headers != nil && len(event.Headers) > 0 {
		var headers []kafka.Header
		for k, v := range event.Headers {
			header := kafka.Header{
				Key:   k,
				Value: []byte(v),
			}
			headers = append(headers, header)
		}
		msg.Headers = headers
	}

	// send it to kafka synchronously
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := ec.producer.Produce(msg, deliveryChan)
	if err != nil {
		logger.Error().Err(err).Msg("send event failed")
		return nil, err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		logger.Error().Err(m.TopicPartition.Error).Msg("sending event received failure response")
		return nil, m.TopicPartition.Error
	}

	logger.Debug().Str("topic", event.Topic).Str("key", event.Key).Msg("successfully sent event")
	return m, nil
}

func (ec *EventConnector) ForwardEvent(ctx context.Context, msg *kafka.Message) error {
	// send it to kafka synchronously
	logger := zerolog.Ctx(ctx)
	topic := *msg.TopicPartition.Topic
	key := msg.Key
	logger.Debug().Str("topic", topic).Str("key", string(key)).Msg("start forwarding kafka msg")

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := ec.producer.Produce(msg, deliveryChan)
	if err != nil {
		logger.Error().Err(err).Msg("send event failed")
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		logger.Error().Err(m.TopicPartition.Error).Msg("sending event received failure response")
		return m.TopicPartition.Error
	}

	logger.Debug().Str("topic", topic).Str("key", string(key)).Msg("successfully forwarded kafka msg")
	return nil
}
