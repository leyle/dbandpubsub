package kafkaconnector

import (
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	defaultSessionTimeout     = 6000 // millisecond
	defaultAdminTimeout       = 30   // seconds
	defaultOffsetRest         = "earliest"
	defaultRetryCount         = 3
	defaultRetryDelay         = 5 // seconds
	defaultDQLTopic           = "dead-letter-queue"
	defaultRequestMsgSize     = 10_000_000 // ~ 10m
	defaultWorkerBufferSize   = 100        // buffer size for topic worker channels
	defaultPartitionKeyHeader = "dataId"   // default header name for custom partition key
	defaultPartitionCacheTTL  = 5 * time.Minute
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

	// concurrent consumer options
	WorkerBufferSize int // buffer size for per-topic worker channels, default is 100

	// producer partition key options
	// PartitionKeyHeader specifies which header to use for custom partition calculation.
	// If set and the header exists in Event.Headers, partition is calculated as:
	// hash(headerValue) % partitionCount
	// If not set or header doesn't exist, uses kafka.PartitionAny (default Kafka behavior)
	// Default is "dataId"
	PartitionKeyHeader string

	// PartitionCacheTTL specifies how long to cache partition counts before refreshing.
	// This handles scenarios where topic partitions are increased.
	// Default is 5 minutes. Set to 0 to disable caching (query every time).
	PartitionCacheTTL time.Duration

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
