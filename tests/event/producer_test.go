package event

import (
	"fmt"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
	"testing"
)

var (
	brokers = []string{
		"k0.dev.kafka.pymom.com:9092",
		"k1.dev.kafka.pymom.com:9192",
		"k2.dev.kafka.pymom.com:9292",
	}
	groupId       = "g-client-kafka"
	consumeTopics = []string{"cdi-topic", "email-topic", "cdi-event-request"}
	// consumeTopics = []string{"cdi-event-request"}
	produceTopics = map[string]string{
		"cdi":   "cdi-topic",
		"email": "email-topic",
	}
)

var opt = &kafkaconnector.EventOption{
	Brokers:           brokers,
	GroupId:           groupId,
	NeedAdmin:         true,
	PartitionNum:      3,
	ReplicationFactor: 1,
	ConsumeTopics:     consumeTopics,
	ProduceTopics:     produceTopics,
}

func generateEvent() *kafkaconnector.Event {
	key := objectid.GetObjectId()
	val := fmt.Sprintf("val: %s", key)
	event := &kafkaconnector.Event{
		Topic: produceTopics["cdi"],
		Key:   key,
		Val:   []byte(val),
	}

	return event
}

func TestConnectorProducer(t *testing.T) {
	baseCtx := NewCfgAndCtx()

	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}

	ctx := baseCtx.NewReq()

	event := generateEvent()
	t.Logf("topic[%s]|key[%s]", event.Topic, event.Key)
	result, err := ec.SendEvent(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(result)
}
