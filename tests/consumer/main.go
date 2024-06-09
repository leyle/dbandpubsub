package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leyle/dbandpubsub/kafkaconnector"
	"github.com/rs/zerolog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// test consumer disconnect and reconnect cases

func main() {
	baseCtx := NewCfgAndCtx()

	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// ensure the topic exists
	err = ec.CreateTopics(baseCtx, consumeTopics)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := baseCtx.NewReq()

	go infiniteRunConsumer(ctx, ec)

	// err = ec.ConsumeEvent(ctx, consumeTopics, eventCallback)
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	ec.Stop(ctx)
	fmt.Println("done")
}

func infiniteRunConsumer(ctx context.Context, ec *kafkaconnector.EventConnector) {
	logger := zerolog.Ctx(ctx)
	for {
		logger.Debug().Msg("try/retry to start consumer...")
		err := startConsumer(ctx, ec)
		if err != nil {
			logger.Error().Err(err).Msg("start consumer failed, waiting for retry")
		}
		time.Sleep(2 * time.Second)
	}
}

func startConsumer(ctx context.Context, ec *kafkaconnector.EventConnector) error {
	logger := zerolog.Ctx(ctx)
	err := ec.ConsumeEvent(ctx, consumeTopics, eventCallback)
	if err != nil {
		logger.Error().Err(err).Msg("consume event failed")
		return err
	}

	return nil
}

func eventCallback(ctx context.Context, msg *kafka.Message) error {
	logger := zerolog.Ctx(ctx)

	logger.Debug().Str("topic", *msg.TopicPartition.Topic).Bytes("key", msg.Key).Msg("callback executed")

	return nil
}
