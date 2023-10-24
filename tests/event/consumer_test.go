package event

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leyle/dbandpubsub/kafkaconnector"
	"github.com/rs/zerolog"
	"testing"
)

func TestConnectorConsumer(t *testing.T) {
	baseCtx := NewCfgAndCtx()

	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}

	ctx := baseCtx.NewReq()

	err = ec.ConsumeEvent(ctx, consumeTopics, eventCallback)
	if err != nil {
		t.Fatal(err)
	}

	ec.Stop(ctx)

	t.Log("done")
}

func eventCallback(ctx context.Context, msg *kafka.Message) error {
	logger := zerolog.Ctx(ctx)

	logger.Debug().Str("topic", *msg.TopicPartition.Topic).Bytes("key", msg.Key).Msg("callback executed")

	return nil
	// return errors.New("client error")
}
