package event

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
)

var (
	brokers = []string{
		"k0.dev.test:9092",
		"k1.dev.test:9093",
		"k2.dev.test:9094",
		// "k0.dev.kafka.pymom.com:9092",
		// "k1.dev.kafka.pymom.com:9192",
		// "k2.dev.kafka.pymom.com:9292",
		// "b-3-public.cdimsktestnet.grii65.c2.kafka.ap-east-1.amazonaws.com:9194",
		// "b-1-public.cdimsktestnet.grii65.c2.kafka.ap-east-1.amazonaws.com:9194",
		// "b-2-public.cdimsktestnet.grii65.c2.kafka.ap-east-1.amazonaws.com:9194",
	}
	groupId = "g-client-kafka"
	// consumeTopics = []string{"cdi-topic", "email-topic", "cdi-event-request"}
	// consumeTopics = []string{"cdi-create-topic-in-public"}
	// consumeTopics = []string{"cdi-event-request"}
	consumeTopics = []string{"dev-cdi-consumer-test"}
	produceTopics = map[string]string{
		// "cdi":   "dev-cdi-topic",
		"cdi":   consumeTopics[0],
		"email": "dev-email-topic",
	}

	tlsCert = "/tmp/kafkatls/public.pem"
	tlsKey  = "/tmp/kafkatls/private.pem"
)

var opt = &kafkaconnector.EventOption{
	Brokers:           brokers,
	GroupId:           groupId,
	NeedAdmin:         true,
	PartitionNum:      3,
	ReplicationFactor: 3,
	ConsumeTopics:     consumeTopics,
	ProduceTopics:     produceTopics,
	// MoreOptions:       generateTLSOption(),
}

func generateTLSOption() map[string]kafka.ConfigValue {
	tlsOpt := make(map[string]kafka.ConfigValue)

	tlsOpt["security.protocol"] = "SSL"
	tlsOpt["ssl.certificate.location"] = tlsCert
	tlsOpt["ssl.key.location"] = tlsKey
	tlsOpt["ssl.key.password"] = "testnet"

	return tlsOpt
}

func generateEvent() *kafkaconnector.Event {
	key := objectid.GetObjectId()
	val := fmt.Sprintf("val: %s", key)
	event := &kafkaconnector.Event{
		Topic: produceTopics["cdi"],
		// Topic: "cdi-create-topic-in-public",
		Key: key,
		Val: []byte(val),
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
