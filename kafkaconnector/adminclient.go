package kafkaconnector

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

type eventAdminClient struct {
	*kafka.AdminClient
	opt *EventOption
}

func newEventAdminClient(opt *EventOption) (*eventAdminClient, error) {
	if opt.Timeout <= 0 {
		opt.Timeout = time.Duration(defaultAdminTimeout) * time.Second
	}

	conf := kafka.ConfigMap{
		"bootstrap.servers": opt.GetServers(),
	}

	if len(opt.MoreOptions) > 0 {
		for k, v := range opt.MoreOptions {
			conf[k] = v
		}
	}

	client, err := kafka.NewAdminClient(&conf)
	if err != nil {
		return nil, err
	}

	ca := &eventAdminClient{
		AdminClient: client,
		opt:         opt,
	}

	return ca, err
}

func (ec *EventConnector) CreateTopics(ctx context.Context, topics []string) error {
	logger := zerolog.Ctx(ctx)
	exist := ec.opt.NeedAdmin
	if !exist {
		logger.Warn().Msg("no admin client instance")
		return errors.New("no admin client instance")
	}

	logger.Debug().Strs("topic", topics).Msg("start create kafka topics")

	results, err := ec.adminClient.CreateTopics(
		ctx,
		ec.adminClient.setTopicSpecification(topics),
		kafka.SetAdminRequestTimeout(ec.opt.Timeout),
	)
	if err != nil {
		logger.Error().Err(err).Msg("create kafka topics failed")
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			emsg := result.Error.Error()
			if strings.Contains(emsg, "exist") {
				logger.Warn().Msgf("create topic, topic has created, %s", emsg)
			} else {
				logger.Error().Err(result.Error).Str("topic", result.Topic).Msg("create topic failed")
				return result.Error
			}
		}
	}

	logger.Debug().Strs("topic", topics).Msg("successfully created kafka topics")

	return nil
}

func (eac *eventAdminClient) setTopicSpecification(topics []string) []kafka.TopicSpecification {
	var ts []kafka.TopicSpecification
	for _, t := range topics {
		oneTS := kafka.TopicSpecification{
			Topic:             t,
			NumPartitions:     eac.opt.PartitionNum,
			ReplicationFactor: eac.opt.ReplicationFactor,
		}
		ts = append(ts, oneTS)
	}
	return ts
}
