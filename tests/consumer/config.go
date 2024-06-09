package main

import (
	"fmt"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
)

var (
	brokers = []string{
		"k0.x1c.pymom.com:9092",
		"k1.x1c.pymom.com:9192",
		"k2.x1c.pymom.com:9292",
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
