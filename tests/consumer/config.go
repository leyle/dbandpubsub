package main

import (
	"fmt"

	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
)

var (
	brokers = []string{
		"k0.dev.test:9092",
		"k1.dev.test:9093",
		"k2.dev.test:9094",
	}
	groupId       = "g-dev-consumer-test"
	consumeTopics = []string{"dev-cdi-consumer-test"}
	produceTopics = map[string]string{
		"test": consumeTopics[0],
	}
)

var opt = &kafkaconnector.EventOption{
	Brokers:           brokers,
	GroupId:           groupId,
	NeedAdmin:         true,
	PartitionNum:      3,
	ReplicationFactor: 3,
	ConsumeTopics:     consumeTopics,
	ProduceTopics:     produceTopics,
}

func generateEvent() *kafkaconnector.Event {
	key := objectid.GetObjectId()
	val := fmt.Sprintf("val: %s", key)
	event := &kafkaconnector.Event{
		Topic: produceTopics["test"],
		Key:   key,
		Val:   []byte(val),
	}
	return event
}
