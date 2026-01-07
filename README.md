# dbandpubsub

A Go library providing connectors for databases and message queues with built-in resilience patterns.

## Features

- **Kafka Connector** - Producer, Consumer, DLQ, retry logic, custom partitioning
- **MongoDB Client** - Connection pooling, replica set support, index management
- **Redis Lock Client** - Distributed locking (standalone, sentinel, cluster)
- **Zerolog Helper** - Structured logging configuration

## Installation

```bash
go get github.com/leyle/dbandpubsub
```

---

## Kafka Connector

### Quick Start

```go
import "github.com/leyle/dbandpubsub/kafkaconnector"

opt := &kafkaconnector.EventOption{
    Brokers:           []string{"localhost:9092"},
    GroupId:           "my-consumer-group",
    NeedAdmin:         true,
    PartitionNum:      12,
    ReplicationFactor: 3,
}

ec, err := kafkaconnector.NewEventConnector(opt)
if err != nil {
    log.Fatal(err)
}
defer ec.Stop(ctx)
```

### Producing Events

```go
event := &kafkaconnector.Event{
    Topic:   "my-topic",
    Key:     "event-key",
    Val:     []byte(`{"data": "value"}`),
    Headers: map[string]string{
        "dataId": "unique-id", // Used for custom partitioning
    },
}

result, err := ec.SendEvent(ctx, event)
```

### Consuming Events

```go
// Simple consumer
err := ec.ConsumeEvent(ctx, []string{"my-topic"}, func(ctx context.Context, msg *kafka.Message) error {
    // Process message
    return nil
})

// Concurrent consumer (one worker per partition)
err := ec.ConsumeEventConcurrently(ctx, []string{"my-topic"}, callback)
```

### Features

| Feature | Description |
|---------|-------------|
| **Custom Partitioning** | Route messages by `dataId` header for ordering |
| **Partition Cache** | Caches partition counts with configurable TTL |
| **Retry Logic** | Configurable retry count and delay |
| **Dead Letter Queue** | Failed messages sent to DLQ after retries |
| **Graceful Shutdown** | Proper consumer cleanup on stop signal |
| **Rebalance Handling** | Automatic worker cleanup on partition revocation |

### Configuration Options

```go
type EventOption struct {
    Brokers           []string      // Kafka broker addresses
    GroupId           string        // Consumer group ID
    
    // Admin client (for topic management)
    NeedAdmin         bool          // Enable admin client
    PartitionNum      int           // Partitions for new topics
    ReplicationFactor int           // Replication factor for new topics
    
    // Consumer options
    SessionTimeout    int           // Session timeout (ms), default: 6000
    OffsetReset       string        // "earliest" or "latest", default: "earliest"
    
    // Retry options
    RetryCount        int           // Retry attempts, default: 3
    RetryDelay        time.Duration // Delay between retries, default: 5s
    DLQTopic          string        // Dead letter queue topic
    
    // Custom partitioning
    PartitionKeyHeader string       // Header for partition key, default: "dataId"
    PartitionCacheTTL  time.Duration // Cache TTL, default: 5min
    
    // Concurrent consumer
    WorkerBufferSize  int           // Buffer per partition worker, default: 100
}
```

---

## MongoDB Client

Connection wrapper with pooling, replica set support, and index management.

### Quick Start

```go
import "github.com/leyle/dbandpubsub/mongodb"

opt := &mongodb.MgoOption{
    HostPorts:    []string{"localhost:27017"},
    Username:     "user",
    Password:     "password",
    Database:     "mydb",
    ReadTimeout:  60,  // seconds
    WriteTimeout: 120, // seconds
}

ds := mongodb.NewDataSource(opt)
defer ds.Close()
```

### Usage

```go
// Get collection
collection := ds.C("users")

// Use with contexts (with timeouts)
ctx := ds.ReadContext()  // 60s timeout
ctx := ds.WriteContext() // 120s timeout

// Create indexes
ds.InsureSingleIndexes("users", []string{"email", "createdAt"})
ds.InsureUniqueIndexes("users", []string{"email"})
ds.InsureCompoundIndex("users", []string{"status", "createdAt"})

// Access raw client
client := ds.Client()
```

### Features

| Feature | Description |
|---------|-------------|
| **Connection Pooling** | Min 100, Max 200 connections by default |
| **Replica Set** | Automatic support for 3+ member replica sets |
| **Singleton Pattern** | Thread-safe single instance per configuration |
| **Index Management** | Single, unique, and compound index creation |
| **Context Helpers** | Pre-configured read/write timeout contexts |

### Configuration Options

```go
type MgoOption struct {
    HostPorts    []string              // MongoDB host:port addresses
    Username     string                // Authentication username
    Password     string                // Authentication password
    Database     string                // Database name (also used for authSource)
    ConnOption   string                // Additional connection options
    ReadTimeout  int                   // Read timeout in seconds (default: 60)
    WriteTimeout int                   // Write timeout in seconds (default: 120)
    MoreOptions  *options.ClientOptions // Additional mongo driver options
}
```

---

## Redis Lock Client

Distributed locking with support for standalone, sentinel, and cluster modes.

```go
import "github.com/leyle/dbandpubsub/redislockclient"

// Acquire lock
lock, err := client.Lock(ctx, "resource-key", 30*time.Second)
if err != nil {
    // Lock failed
}
defer lock.Unlock(ctx)

// Do work with lock...
```

---

## Test Coverage

The kafkaconnector package has **55% test coverage** with tests for:
- Producer: SendEvent, ForwardEvent, partition distribution
- Consumer: Input validation, retry logic, DLQ handling
- Admin: CreateTopics, topic exists handling
- Connector: NewEventConnector, Stop, Reconnect

Run tests:
```bash
go test -v -coverprofile=coverage.out ./kafkaconnector/...
go tool cover -func=coverage.out
```

## License

MIT
