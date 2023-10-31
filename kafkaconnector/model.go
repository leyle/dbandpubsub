package kafkaconnector

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
	"time"
)

const (
	defaultSessionTimeout = 6000 // millisecond
	defaultAdminTimeout   = 30   // seconds
	defaultOffsetRest     = "earliest"
	defaultRetryCount     = 3
	defaultRetryDelay     = 5 // seconds
	defaultDQLTopic       = "dead-letter-queue"
)

type EventOption struct {
	Brokers []string

	// consumer options
	GroupId         string
	SessionTimeout  int
	OffsetReset     string
	OffsetAutoStore bool

	// admin client options
	NeedAdmin         bool
	PartitionNum      int
	ReplicationFactor int
	Timeout           time.Duration // seconds

	// topic infos
	ConsumeTopics []string
	ProduceTopics map[string]string

	// consumer retry info
	DLQTopic   string        // dead letter queue
	RetryCount int           // default is 3
	RetryDelay time.Duration // default is 5 seconds

	MoreOptions map[string]kafka.ConfigValue

	err error
}

func (opt *EventOption) GetServers() string {
	return strings.Join(opt.Brokers, ",")
}

type Event struct {
	Topic   string
	Key     string
	Val     []byte
	Headers map[string]string
}
