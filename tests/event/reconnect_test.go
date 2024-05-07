package event

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leyle/dbandpubsub/kafkaconnector"
	"github.com/rs/zerolog"
	"testing"
)

func TestReconnect(t *testing.T) {
	baseCtx := NewCfgAndCtx()
	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}

	ctx := baseCtx.NewReq()

	go ec.HandleError(ctx)

	err = ec.ConsumeEvent(ctx, consumeTopics, returnErrEventCallback)
	if err != nil {
		// reconnect event connector
		newEC, err := ec.Reconnect(ctx)
		// t.Fatal(err)
		if err != nil {
			t.Error(err)
		}
		t.Log("reconnect to kafka")
		t.Log(newEC)
	}

	ec.Stop(ctx)

	t.Log("done")
}

func returnErrEventCallback(ctx context.Context, msg *kafka.Message) error {
	logger := zerolog.Ctx(ctx)

	logger.Debug().Str("topic", *msg.TopicPartition.Topic).Bytes("key", msg.Key).Msg("callback executed")

	return errors.New("raised error")
}
