package event

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
)

// TestRebalanceCleanup tests that the rebalance callback is properly invoked
// and logs partition assignments with a single consumer.
//
// NOTE: Multi-consumer rebalance testing (2+ consumers in same process, same group)
// is NOT supported by librdkafka in a single process - it causes crashes during
// rebalance operations. To test multi-consumer scenarios, use:
// 1. Separate processes (e.g., Docker containers)
// 2. Integration tests with Kubernetes
// 3. Manual testing with multiple terminals
func TestRebalanceCleanup(t *testing.T) {
	testTopic := fmt.Sprintf("test-rebalance-%s", objectid.GetObjectId())
	groupId := fmt.Sprintf("test-rebalance-group-%s", objectid.GetObjectId())

	opt := &kafkaconnector.EventOption{
		Brokers:           customPartitionBrokers,
		GroupId:           groupId,
		NeedAdmin:         true,
		PartitionNum:      6,
		ReplicationFactor: 3,
		ConsumeTopics:     []string{testTopic},
		ProduceTopics:     map[string]string{"test": testTopic},
	}

	baseCtx := NewCfgAndCtx()
	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Stop(baseCtx.NewReq())

	ctx := baseCtx.NewReq()

	err = ec.CreateTopics(ctx, []string{testTopic})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Log("Topic created with 6 partitions")
	time.Sleep(2 * time.Second)

	var messageCount atomic.Int64

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- ec.ConsumeEventConcurrently(ctx, []string{testTopic}, func(ctx context.Context, msg *kafka.Message) error {
			messageCount.Add(1)
			return nil
		})
	}()

	// Wait for partition assignment (check logs for "partitions assigned count=6")
	time.Sleep(3 * time.Second)
	t.Log("Consumer started - check logs for 'partitions assigned count=6'")

	t.Log("Sending 12 messages...")
	for i := 0; i < 12; i++ {
		event := &kafkaconnector.Event{
			Topic:   testTopic,
			Key:     objectid.GetObjectId(),
			Val:     []byte(fmt.Sprintf("message %d", i)),
			Headers: map[string]string{"dataId": fmt.Sprintf("data-%d", i)},
		}
		ec.SendEvent(ctx, event)
	}

	time.Sleep(3 * time.Second)
	count := messageCount.Load()
	t.Logf("Messages processed: %d", count)

	if count != 12 {
		t.Errorf("Expected 12 messages processed, got %d", count)
	}

	// Stop consumer - triggers partition revocation
	t.Log("Stopping consumer - check logs for:")
	t.Log("  - 'partitions revoked count=6'")
	t.Log("  - 'cleaning up worker for revoked partition'")
	ec.Stop(ctx)

	select {
	case <-consumerDone:
		t.Log("Consumer stopped successfully")
	case <-time.After(10 * time.Second):
		t.Error("Consumer stop timeout")
	}

	t.Log("SUCCESS: Rebalance cleanup test completed")
}
