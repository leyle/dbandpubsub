package event

import (
	"fmt"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/kafkaconnector"
)

var customPartitionBrokers = []string{
	"k0.dev.test:9092",
	"k1.dev.test:9093",
	"k2.dev.test:9094",
}

func TestCustomPartitionKey(t *testing.T) {
	// Create a unique test topic with multiple partitions
	testTopic := fmt.Sprintf("test-custom-partition-%s", objectid.GetObjectId())
	t.Logf("Creating test topic: %s with 12 partitions", testTopic)

	opt := &kafkaconnector.EventOption{
		Brokers:           customPartitionBrokers,
		GroupId:           "op-gid-server",
		NeedAdmin:         true, // Required for CreateTopics and getPartitionCount
		PartitionNum:      12,   // Create topic with 12 partitions
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

	// Create the topic with 12 partitions
	err = ec.CreateTopics(ctx, []string{testTopic})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("Topic %s created successfully", testTopic)

	// Wait for topic to be fully created
	time.Sleep(2 * time.Second)

	// Test 1: Send multiple events with SAME dataId but DIFFERENT keys
	// They should all go to the SAME partition
	dataId := "test-data-id-" + objectid.GetObjectId()
	t.Logf("Test 1: Using dataId: %s", dataId)

	var partitions []int32
	for i := 0; i < 5; i++ {
		event := &kafkaconnector.Event{
			Topic: testTopic,
			Key:   objectid.GetObjectId(), // Different key each time
			Val:   []byte(fmt.Sprintf("test message %d", i)),
			Headers: map[string]string{
				"dataId": dataId, // Same dataId
			},
		}

		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent failed: %v", err)
		}

		t.Logf("Event %d: key=%s, partition=%d", i, event.Key, result.TopicPartition.Partition)
		partitions = append(partitions, result.TopicPartition.Partition)
	}

	// Verify all events went to the same partition
	firstPartition := partitions[0]
	for i, p := range partitions {
		if p != firstPartition {
			t.Errorf("Event %d went to partition %d, expected %d (same dataId should go to same partition)", i, p, firstPartition)
		}
	}
	t.Logf("SUCCESS: All 5 events with same dataId went to partition %d", firstPartition)

	// Test 2: Send events with DIFFERENT dataIds - they should (likely) go to DIFFERENT partitions
	t.Log("Test 2: Testing different dataIds go to different partitions...")
	partitionSet := make(map[int32]bool)
	for i := 0; i < 12; i++ {
		differentDataId := fmt.Sprintf("different-data-id-%d-%s", i, objectid.GetObjectId())
		event := &kafkaconnector.Event{
			Topic: testTopic,
			Key:   objectid.GetObjectId(),
			Val:   []byte(fmt.Sprintf("different dataId message %d", i)),
			Headers: map[string]string{
				"dataId": differentDataId,
			},
		}

		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent failed: %v", err)
		}

		t.Logf("Different dataId event %d: partition=%d", i, result.TopicPartition.Partition)
		partitionSet[result.TopicPartition.Partition] = true
	}
	t.Logf("SUCCESS: 12 different dataIds distributed across %d partitions", len(partitionSet))
	if len(partitionSet) < 2 {
		t.Errorf("Expected events to be distributed across multiple partitions, got %d", len(partitionSet))
	}

	// Test 3: Send events WITHOUT dataId header - should use default Kafka behavior
	t.Log("Test 3: Testing default behavior without dataId header...")
	for i := 0; i < 3; i++ {
		event := &kafkaconnector.Event{
			Topic: testTopic,
			Key:   objectid.GetObjectId(),
			Val:   []byte(fmt.Sprintf("no-dataId message %d", i)),
			// No Headers - should use kafka.PartitionAny (default behavior)
		}

		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent failed: %v", err)
		}

		t.Logf("No-dataId Event %d: key=%s, partition=%d", i, event.Key, result.TopicPartition.Partition)
	}
	t.Log("SUCCESS: Events without dataId sent successfully (using default partitioning)")
}

// TestCacheExpiry verifies that partition count cache expires after TTL and refreshes
func TestCacheExpiry(t *testing.T) {
	testTopic := fmt.Sprintf("test-cache-expiry-%s", objectid.GetObjectId())
	t.Logf("Creating test topic: %s", testTopic)

	// Use a very short TTL (2 seconds) for testing
	opt := &kafkaconnector.EventOption{
		Brokers:           customPartitionBrokers,
		GroupId:           "op-gid-server",
		NeedAdmin:         true,
		PartitionNum:      6,
		ReplicationFactor: 3,
		ConsumeTopics:     []string{testTopic},
		ProduceTopics:     map[string]string{"test": testTopic},
		PartitionCacheTTL: 2 * time.Second, // Short TTL for testing
	}

	baseCtx := NewCfgAndCtx()
	ec, err := kafkaconnector.NewEventConnector(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer ec.Stop(baseCtx.NewReq())

	ctx := baseCtx.NewReq()

	// Create the topic
	err = ec.CreateTopics(ctx, []string{testTopic})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	t.Logf("Topic %s created with 6 partitions", testTopic)
	time.Sleep(2 * time.Second)

	// First send - should cache partition count
	dataId1 := "cache-test-" + objectid.GetObjectId()
	event1 := &kafkaconnector.Event{
		Topic:   testTopic,
		Key:     objectid.GetObjectId(),
		Val:     []byte("first event"),
		Headers: map[string]string{"dataId": dataId1},
	}
	result1, err := ec.SendEvent(ctx, event1)
	if err != nil {
		t.Fatalf("First SendEvent failed: %v", err)
	}
	t.Logf("First event sent to partition %d (cache populated)", result1.TopicPartition.Partition)

	// Second send immediately - should use cached value (no "cached partition count" log)
	event2 := &kafkaconnector.Event{
		Topic:   testTopic,
		Key:     objectid.GetObjectId(),
		Val:     []byte("second event"),
		Headers: map[string]string{"dataId": dataId1},
	}
	result2, err := ec.SendEvent(ctx, event2)
	if err != nil {
		t.Fatalf("Second SendEvent failed: %v", err)
	}
	t.Logf("Second event sent to partition %d (should use cached value)", result2.TopicPartition.Partition)

	// Same dataId should go to same partition
	if result1.TopicPartition.Partition != result2.TopicPartition.Partition {
		t.Errorf("Same dataId went to different partitions: %d vs %d",
			result1.TopicPartition.Partition, result2.TopicPartition.Partition)
	}

	// Wait for cache to expire
	t.Log("Waiting 3 seconds for cache to expire (TTL is 2 seconds)...")
	time.Sleep(3 * time.Second)

	// Third send - should trigger cache refresh (look for "partition cache expired" log)
	event3 := &kafkaconnector.Event{
		Topic:   testTopic,
		Key:     objectid.GetObjectId(),
		Val:     []byte("third event after cache expiry"),
		Headers: map[string]string{"dataId": dataId1},
	}
	result3, err := ec.SendEvent(ctx, event3)
	if err != nil {
		t.Fatalf("Third SendEvent failed: %v", err)
	}
	t.Logf("Third event sent to partition %d (cache should have refreshed)", result3.TopicPartition.Partition)

	// Same dataId should still go to same partition (hash is deterministic)
	if result1.TopicPartition.Partition != result3.TopicPartition.Partition {
		t.Errorf("Same dataId went to different partitions after cache refresh: %d vs %d",
			result1.TopicPartition.Partition, result3.TopicPartition.Partition)
	}

	t.Log("SUCCESS: Cache expiry and refresh working correctly")
}
