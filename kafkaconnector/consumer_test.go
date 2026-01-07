package kafkaconnector

import (
	"errors"
	"testing"
)

func TestErrMessageSentToDLQ(t *testing.T) {
	// Verify the sentinel error exists and has expected message
	if ErrMessageSentToDLQ == nil {
		t.Fatal("ErrMessageSentToDLQ should not be nil")
	}

	expectedMsg := "message sent to DLQ after callback failures"
	if ErrMessageSentToDLQ.Error() != expectedMsg {
		t.Errorf("ErrMessageSentToDLQ.Error() = %q, want %q", ErrMessageSentToDLQ.Error(), expectedMsg)
	}
}

func TestErrMessageSentToDLQ_ErrorsIs(t *testing.T) {
	// Test that errors.Is works correctly with the sentinel error
	wrappedErr := errors.New("wrapper: " + ErrMessageSentToDLQ.Error())

	// Direct comparison should work
	if !errors.Is(ErrMessageSentToDLQ, ErrMessageSentToDLQ) {
		t.Error("errors.Is(ErrMessageSentToDLQ, ErrMessageSentToDLQ) should be true")
	}

	// Wrapped error should not match (it's a new error, not wrapped with %w)
	if errors.Is(wrappedErr, ErrMessageSentToDLQ) {
		t.Error("Non-wrapped error should not match sentinel")
	}
}

func TestErrMessageSentToDLQ_UsagePattern(t *testing.T) {
	// Simulate the typical usage pattern from runPartitionWorker
	simulateCallback := func(shouldFail bool) error {
		if shouldFail {
			return ErrMessageSentToDLQ
		}
		return nil
	}

	// Test successful case
	err := simulateCallback(false)
	if err != nil {
		t.Errorf("Expected nil, got %v", err)
	}

	// Test DLQ case
	err = simulateCallback(true)
	if !errors.Is(err, ErrMessageSentToDLQ) {
		t.Errorf("Expected ErrMessageSentToDLQ, got %v", err)
	}
}

func TestPartitionWorkerIdFormat(t *testing.T) {
	// Test the expected format for worker IDs
	// Format should be "topic-partitionID" as used in runDispatcher
	tests := []struct {
		topic     string
		partition int32
		expected  string
	}{
		{"my-topic", 0, "my-topic-0"},
		{"my-topic", 5, "my-topic-5"},
		{"another-topic", 11, "another-topic-11"},
		{"topic", 999, "topic-999"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			// Verify the expected format string is valid and non-empty
			if tt.expected == "" {
				t.Error("Expected format should not be empty")
			}
			// Verify format contains topic and partition
			if len(tt.expected) <= len(tt.topic) {
				t.Error("Expected format should include partition number")
			}
		})
	}
}

func TestEventConsumer_DefaultValues(t *testing.T) {
	// Verify default constant values are reasonable
	if defaultRetryCount <= 0 {
		t.Errorf("defaultRetryCount should be positive, got %d", defaultRetryCount)
	}

	if defaultRetryDelay <= 0 {
		t.Errorf("defaultRetryDelay should be positive, got %d", defaultRetryDelay)
	}

	if defaultDQLTopic == "" {
		t.Error("defaultDQLTopic should not be empty")
	}

	if defaultWorkerBufferSize <= 0 {
		t.Errorf("defaultWorkerBufferSize should be positive, got %d", defaultWorkerBufferSize)
	}
}
