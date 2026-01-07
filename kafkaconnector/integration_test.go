package kafkaconnector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

// Test Kafka Configuration
var testBrokers = []string{
	"k0.dev.test:9092",
	"k1.dev.test:9093",
	"k2.dev.test:9094",
}

const (
	testGroupId           = "op-gid-server-test"
	testPartitionNum      = 12
	testReplicationFactor = 3
	testDLQTopic          = "dead-letter-queue"
)

// testContext creates a context with zerolog logger for testing
func testContext() context.Context {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	return logger.WithContext(context.Background())
}

// generateTestTopic creates a unique topic name for testing
func generateTestTopic(prefix string) string {
	return fmt.Sprintf("test-%s-%d", prefix, time.Now().UnixNano())
}

// newTestEventConnector creates an EventConnector for testing
func newTestEventConnector(t *testing.T, topics []string) *EventConnector {
	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId + fmt.Sprintf("-%d", time.Now().UnixNano()),
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		DLQTopic:          testDLQTopic,
		ConsumeTopics:     topics,
		ProduceTopics:     map[string]string{"test": topics[0]},
		RetryCount:        2,
		RetryDelay:        1 * time.Second,
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("Failed to create EventConnector: %v", err)
	}
	return ec
}

// =============================================================================
// NewEventConnector Tests
// =============================================================================

func TestNewEventConnector_Success(t *testing.T) {
	topic := generateTestTopic("connector")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId,
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		ConsumeTopics:     []string{topic},
		ProduceTopics:     map[string]string{"test": topic},
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	if ec.producer == nil {
		t.Error("Producer should not be nil")
	}
	if ec.consumer == nil {
		t.Error("Consumer should not be nil")
	}
	if ec.adminClient == nil {
		t.Error("AdminClient should not be nil when NeedAdmin is true")
	}
}

func TestNewEventConnector_WithoutAdmin(t *testing.T) {
	topic := generateTestTopic("no-admin")

	opt := &EventOption{
		Brokers:       testBrokers,
		GroupId:       testGroupId,
		NeedAdmin:     false, // Admin client disabled
		ConsumeTopics: []string{topic},
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	if ec.adminClient != nil {
		t.Error("AdminClient should be nil when NeedAdmin is false")
	}
}

// =============================================================================
// CreateTopics Tests
// =============================================================================

func TestCreateTopics_Success(t *testing.T) {
	topic := generateTestTopic("create-topic")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()
	err := ec.CreateTopics(ctx, []string{topic})
	if err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
}

func TestCreateTopics_AlreadyExists(t *testing.T) {
	topic := generateTestTopic("already-exists")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first time
	err := ec.CreateTopics(ctx, []string{topic})
	if err != nil {
		t.Fatalf("First CreateTopics failed: %v", err)
	}

	// Create same topic again - should handle gracefully (log warning but no error)
	err = ec.CreateTopics(ctx, []string{topic})
	if err != nil {
		t.Fatalf("Second CreateTopics should handle existing topic: %v", err)
	}
}

func TestCreateTopics_NoAdminClient(t *testing.T) {
	topic := generateTestTopic("no-admin-create")

	opt := &EventOption{
		Brokers:       testBrokers,
		GroupId:       testGroupId,
		NeedAdmin:     false,
		ConsumeTopics: []string{topic},
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()
	err = ec.CreateTopics(ctx, []string{topic})
	if err == nil {
		t.Error("CreateTopics should fail when no admin client")
	}
}

// =============================================================================
// SendEvent Tests
// =============================================================================

func TestSendEvent_Success(t *testing.T) {
	topic := generateTestTopic("send-event")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second) // Wait for topic to be ready

	event := &Event{
		Topic: topic,
		Key:   "test-key-1",
		Val:   []byte("test-value-1"),
		Headers: map[string]string{
			"header1": "value1",
		},
	}

	result, err := ec.SendEvent(ctx, event)
	if err != nil {
		t.Fatalf("SendEvent failed: %v", err)
	}

	if result == nil {
		t.Fatal("SendEvent result should not be nil")
	}
	if *result.TopicPartition.Topic != topic {
		t.Errorf("Result topic = %q, want %q", *result.TopicPartition.Topic, topic)
	}
}

func TestSendEvent_MultipleMessages(t *testing.T) {
	topic := generateTestTopic("send-multi")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Send 10 messages
	for i := 0; i < 10; i++ {
		event := &Event{
			Topic: topic,
			Key:   fmt.Sprintf("multi-key-%d", i),
			Val:   []byte(fmt.Sprintf("multi-value-%d", i)),
		}
		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent %d failed: %v", i, err)
		}
		if result == nil {
			t.Fatalf("SendEvent %d result should not be nil", i)
		}
	}
}

func TestSendEvent_InvalidEvent_Nil(t *testing.T) {
	topic := generateTestTopic("nil-event")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	_, err := ec.SendEvent(ctx, nil)
	if err == nil {
		t.Error("SendEvent should fail with nil event")
	}
}

func TestSendEvent_InvalidEvent_EmptyTopic(t *testing.T) {
	topic := generateTestTopic("empty-topic")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	event := &Event{
		Topic: "", // Empty topic
		Key:   "key",
		Val:   []byte("value"),
	}

	_, err := ec.SendEvent(ctx, event)
	if err == nil {
		t.Error("SendEvent should fail with empty topic")
	}
}

func TestSendEvent_InvalidEvent_EmptyKey(t *testing.T) {
	topic := generateTestTopic("empty-key")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	event := &Event{
		Topic: topic,
		Key:   "", // Empty key
		Val:   []byte("value"),
	}

	_, err := ec.SendEvent(ctx, event)
	if err == nil {
		t.Error("SendEvent should fail with empty key")
	}
}

func TestSendEvent_InvalidEvent_NilValue(t *testing.T) {
	topic := generateTestTopic("nil-value")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	event := &Event{
		Topic: topic,
		Key:   "key",
		Val:   nil, // Nil value
	}

	_, err := ec.SendEvent(ctx, event)
	if err == nil {
		t.Error("SendEvent should fail with nil value")
	}
}

func TestSendEvent_WithHeaders(t *testing.T) {
	topic := generateTestTopic("headers")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	event := &Event{
		Topic: topic,
		Key:   "header-test-key",
		Val:   []byte("header-test-value"),
		Headers: map[string]string{
			"header1": "value1",
			"header2": "value2",
			"custom":  "custom-value",
		},
	}

	result, err := ec.SendEvent(ctx, event)
	if err != nil {
		t.Fatalf("SendEvent failed: %v", err)
	}

	if result == nil {
		t.Fatal("SendEvent result should not be nil")
	}

	// Verify event was sent successfully to the topic
	if *result.TopicPartition.Topic != topic {
		t.Errorf("Result topic = %q, want %q", *result.TopicPartition.Topic, topic)
	}
}

func TestSendEvent_WithPartitionKeyHeader(t *testing.T) {
	topic := generateTestTopic("partition-key")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Send multiple events with same dataId - should go to same partition
	dataId := "consistent-data-id"
	var partitions []int32

	for i := 0; i < 5; i++ {
		event := &Event{
			Topic: topic,
			Key:   fmt.Sprintf("key-%d", i),
			Val:   []byte(fmt.Sprintf("value-%d", i)),
			Headers: map[string]string{
				"dataId": dataId, // Same dataId for all
			},
		}

		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent %d failed: %v", i, err)
		}
		partitions = append(partitions, result.TopicPartition.Partition)
	}

	// All should go to same partition
	for i, p := range partitions {
		if p != partitions[0] {
			t.Errorf("Event %d went to partition %d, expected %d (same dataId)", i, p, partitions[0])
		}
	}
	t.Logf("All 5 events with same dataId went to partition %d", partitions[0])
}

func TestSendEvent_DifferentPartitionKeys(t *testing.T) {
	topic := generateTestTopic("diff-partition")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Send events with different dataIds - should distribute across partitions
	partitionSet := make(map[int32]bool)

	for i := 0; i < 12; i++ {
		event := &Event{
			Topic: topic,
			Key:   fmt.Sprintf("key-%d", i),
			Val:   []byte(fmt.Sprintf("value-%d", i)),
			Headers: map[string]string{
				"dataId": fmt.Sprintf("different-data-%d", i), // Different dataId for each
			},
		}

		result, err := ec.SendEvent(ctx, event)
		if err != nil {
			t.Fatalf("SendEvent %d failed: %v", i, err)
		}
		partitionSet[result.TopicPartition.Partition] = true
	}

	// Should be distributed across multiple partitions
	if len(partitionSet) < 2 {
		t.Errorf("Expected events to distribute across partitions, got only %d partition(s)", len(partitionSet))
	}
	t.Logf("12 different dataIds distributed across %d partitions", len(partitionSet))
}

// =============================================================================
// ConsumeEvent Tests (validation only - no actual consuming to avoid race)
// =============================================================================

func TestConsumeEvent_EmptyTopics(t *testing.T) {
	topic := generateTestTopic("empty-topics")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	callback := func(ctx context.Context, msg *kafka.Message) error {
		return nil
	}

	err := ec.ConsumeEvent(ctx, []string{}, callback)
	if err == nil {
		t.Error("ConsumeEvent should fail with empty topics")
	}
}

func TestConsumeEvent_NilTopics(t *testing.T) {
	topic := generateTestTopic("nil-topics")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	callback := func(ctx context.Context, msg *kafka.Message) error {
		return nil
	}

	err := ec.ConsumeEvent(ctx, nil, callback)
	if err == nil {
		t.Error("ConsumeEvent should fail with nil topics")
	}
}

func TestConsumeEvent_NilCallback(t *testing.T) {
	topic := generateTestTopic("nil-callback")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	err := ec.ConsumeEvent(ctx, []string{topic}, nil)
	if err == nil {
		t.Error("ConsumeEvent should fail with nil callback")
	}
}

// =============================================================================
// ConsumeEventConcurrently Tests (validation only)
// =============================================================================

func TestConsumeEventConcurrently_EmptyTopics(t *testing.T) {
	topic := generateTestTopic("concurrent-empty")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	callback := func(ctx context.Context, msg *kafka.Message) error {
		return nil
	}

	err := ec.ConsumeEventConcurrently(ctx, []string{}, callback)
	if err == nil {
		t.Error("ConsumeEventConcurrently should fail with empty topics")
	}
}

func TestConsumeEventConcurrently_NilCallback(t *testing.T) {
	topic := generateTestTopic("concurrent-nil")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	err := ec.ConsumeEventConcurrently(ctx, []string{topic}, nil)
	if err == nil {
		t.Error("ConsumeEventConcurrently should fail with nil callback")
	}
}

// =============================================================================
// executeCallbackWithRetry Tests (using exported helpers)
// =============================================================================

func TestExecuteCallbackWithRetry_SuccessFirstAttempt(t *testing.T) {
	topic := generateTestTopic("retry-first")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId + fmt.Sprintf("-%d", time.Now().UnixNano()),
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		ConsumeTopics:     []string{topic},
		RetryCount:        3,
		RetryDelay:        100 * time.Millisecond, // Short delay for test speed
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create a mock message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	var callCount atomic.Int32
	callback := func(ctx context.Context, msg *kafka.Message) error {
		callCount.Add(1)
		return nil // Success on first attempt
	}

	err = ec.executeCallbackWithRetry(ctx, msg, callback)
	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if callCount.Load() != 1 {
		t.Errorf("Expected 1 callback call, got %d", callCount.Load())
	}
}

func TestExecuteCallbackWithRetry_SuccessAfterRetries(t *testing.T) {
	topic := generateTestTopic("retry-success")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId + fmt.Sprintf("-%d", time.Now().UnixNano()),
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		ConsumeTopics:     []string{topic},
		RetryCount:        3,
		RetryDelay:        100 * time.Millisecond, // Short delay for test speed
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	var callCount atomic.Int32
	callback := func(ctx context.Context, msg *kafka.Message) error {
		count := callCount.Add(1)
		if count < 2 {
			return errors.New("simulated failure")
		}
		return nil // Success on second attempt
	}

	err = ec.executeCallbackWithRetry(ctx, msg, callback)
	if err != nil {
		t.Errorf("Expected success after retry, got error: %v", err)
	}

	if callCount.Load() != 2 {
		t.Errorf("Expected 2 callback calls, got %d", callCount.Load())
	}
}

func TestExecuteCallbackWithRetry_FailsAllRetries_DLQ(t *testing.T) {
	topic := generateTestTopic("retry-fail")
	dlqTopic := generateTestTopic("dlq")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId + fmt.Sprintf("-%d", time.Now().UnixNano()),
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		DLQTopic:          dlqTopic,
		ConsumeTopics:     []string{topic},
		RetryCount:        2,
		RetryDelay:        100 * time.Millisecond, // Short delay for test speed
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create DLQ topic
	if err := ec.CreateTopics(ctx, []string{dlqTopic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	var callCount atomic.Int32
	callback := func(ctx context.Context, msg *kafka.Message) error {
		callCount.Add(1)
		return errors.New("always fail")
	}

	err = ec.executeCallbackWithRetry(ctx, msg, callback)

	// Should return ErrMessageSentToDLQ
	if !errors.Is(err, ErrMessageSentToDLQ) {
		t.Errorf("Expected ErrMessageSentToDLQ, got: %v", err)
	}

	// Should have tried RetryCount times
	if callCount.Load() != 2 {
		t.Errorf("Expected %d callback calls, got %d", 2, callCount.Load())
	}
}

// =============================================================================
// Stop and Reconnect Tests
// =============================================================================

func TestStop_GracefulShutdown(t *testing.T) {
	topic := generateTestTopic("stop-test")
	ec := newTestEventConnector(t, []string{topic})

	ctx := testContext()

	// Should not panic
	ec.Stop(ctx)

	// Calling stop again should be safe
	ec.Stop(ctx)
}

func TestReconnect_Success(t *testing.T) {
	topic := generateTestTopic("reconnect")
	ec := newTestEventConnector(t, []string{topic})

	ctx := testContext()

	// Reconnect should return a new connector
	newEC, err := ec.Reconnect(ctx)
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}
	defer newEC.Stop(ctx)

	if newEC == nil {
		t.Error("Reconnect should return a new EventConnector")
	}

	// New connector should be functional
	if newEC.producer == nil {
		t.Error("New connector should have producer")
	}
	if newEC.consumer == nil {
		t.Error("New connector should have consumer")
	}
}

func TestReconnect_NewConnectorWorks(t *testing.T) {
	topic := generateTestTopic("reconnect-works")
	ec := newTestEventConnector(t, []string{topic})

	ctx := testContext()

	// Reconnect
	newEC, err := ec.Reconnect(ctx)
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}
	defer newEC.Stop(ctx)

	// Create topic with new connector
	err = newEC.CreateTopics(ctx, []string{topic})
	if err != nil {
		t.Fatalf("CreateTopics with new connector failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Send event with new connector
	event := &Event{
		Topic: topic,
		Key:   "reconnect-key",
		Val:   []byte("reconnect-value"),
	}
	result, err := newEC.SendEvent(ctx, event)
	if err != nil {
		t.Fatalf("SendEvent with new connector failed: %v", err)
	}
	if result == nil {
		t.Error("SendEvent result should not be nil")
	}
}

// =============================================================================
// ForwardEvent Tests
// =============================================================================

func TestForwardEvent_Success(t *testing.T) {
	topic := generateTestTopic("forward")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Create a kafka.Message to forward
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("forward-key"),
		Value: []byte("forward-value"),
	}

	err := ec.ForwardEvent(ctx, msg)
	if err != nil {
		t.Fatalf("ForwardEvent failed: %v", err)
	}
}

func TestForwardEvent_WithHeaders(t *testing.T) {
	topic := generateTestTopic("forward-headers")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic first
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Create a kafka.Message with headers
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("forward-header-key"),
		Value: []byte("forward-header-value"),
		Headers: []kafka.Header{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
	}

	err := ec.ForwardEvent(ctx, msg)
	if err != nil {
		t.Fatalf("ForwardEvent failed: %v", err)
	}
}

// =============================================================================
// getPartitionCount Tests (with real admin client)
// =============================================================================

func TestGetPartitionCount_Success(t *testing.T) {
	topic := generateTestTopic("partition-count")
	ec := newTestEventConnector(t, []string{topic})
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic with known partition count
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(2 * time.Second) // Give more time for topic metadata to propagate

	count, err := ec.getPartitionCount(ctx, topic)
	if err != nil {
		t.Fatalf("getPartitionCount failed: %v", err)
	}

	if count != testPartitionNum {
		t.Errorf("Partition count = %d, want %d", count, testPartitionNum)
	}
}

func TestGetPartitionCount_Caching(t *testing.T) {
	topic := generateTestTopic("cache-test")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId,
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		ConsumeTopics:     []string{topic},
		PartitionCacheTTL: 2 * time.Second, // Short TTL for testing
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create topic
	if err := ec.CreateTopics(ctx, []string{topic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(2 * time.Second)

	// First call - should query Kafka
	count1, err := ec.getPartitionCount(ctx, topic)
	if err != nil {
		t.Fatalf("First getPartitionCount failed: %v", err)
	}

	// Second call - should use cache
	count2, err := ec.getPartitionCount(ctx, topic)
	if err != nil {
		t.Fatalf("Second getPartitionCount failed: %v", err)
	}

	if count1 != count2 {
		t.Errorf("Cached count mismatch: %d vs %d", count1, count2)
	}

	// Wait for cache to expire
	time.Sleep(3 * time.Second)

	// Third call - should refresh cache
	count3, err := ec.getPartitionCount(ctx, topic)
	if err != nil {
		t.Fatalf("Third getPartitionCount failed: %v", err)
	}

	if count1 != count3 {
		t.Errorf("Partition count changed after cache refresh: %d vs %d", count1, count3)
	}
}

func TestGetPartitionCount_NoAdminClient(t *testing.T) {
	topic := generateTestTopic("no-admin-partition")

	opt := &EventOption{
		Brokers:       testBrokers,
		GroupId:       testGroupId,
		NeedAdmin:     false, // No admin client
		ConsumeTopics: []string{topic},
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	count, err := ec.getPartitionCount(ctx, topic)
	if err != nil {
		t.Fatalf("getPartitionCount should not error without admin, got: %v", err)
	}

	// Should return 0 when no admin client
	if count != 0 {
		t.Errorf("Expected 0 partition count without admin client, got %d", count)
	}
}

// =============================================================================
// sendToDLQ Tests
// =============================================================================

func TestSendToDLQ_Success(t *testing.T) {
	topic := generateTestTopic("dlq-send")
	dlqTopic := generateTestTopic("dlq-target")

	opt := &EventOption{
		Brokers:           testBrokers,
		GroupId:           testGroupId + fmt.Sprintf("-%d", time.Now().UnixNano()),
		NeedAdmin:         true,
		PartitionNum:      testPartitionNum,
		ReplicationFactor: testReplicationFactor,
		DLQTopic:          dlqTopic,
		ConsumeTopics:     []string{topic},
	}

	ec, err := NewEventConnector(opt)
	if err != nil {
		t.Fatalf("NewEventConnector failed: %v", err)
	}
	defer ec.Stop(testContext())

	ctx := testContext()

	// Create DLQ topic
	if err := ec.CreateTopics(ctx, []string{dlqTopic}); err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Create a message to send to DLQ
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Key:   []byte("dlq-key"),
		Value: []byte("dlq-value"),
		Headers: []kafka.Header{
			{Key: "original-header", Value: []byte("original-value")},
		},
	}

	err = ec.sendToDLQ(ctx, msg)
	if err != nil {
		t.Fatalf("sendToDLQ failed: %v", err)
	}
}
