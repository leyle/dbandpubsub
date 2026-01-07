package kafkaconnector

import (
	"testing"
	"time"
)

func TestEventOption_GetServers(t *testing.T) {
	tests := []struct {
		name     string
		brokers  []string
		expected string
	}{
		{
			name:     "single broker",
			brokers:  []string{"localhost:9092"},
			expected: "localhost:9092",
		},
		{
			name:     "multiple brokers",
			brokers:  []string{"broker1:9092", "broker2:9093", "broker3:9094"},
			expected: "broker1:9092,broker2:9093,broker3:9094",
		},
		{
			name:     "empty brokers",
			brokers:  []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &EventOption{Brokers: tt.brokers}
			result := opt.GetServers()
			if result != tt.expected {
				t.Errorf("GetServers() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestParseOption_Defaults(t *testing.T) {
	// Create an option with minimal values - parseOption should fill in defaults
	opt := &EventOption{
		Brokers: []string{"localhost:9092"},
		GroupId: "test-group",
	}

	parseOption(opt)

	// Verify defaults are set
	if opt.RetryCount != defaultRetryCount {
		t.Errorf("RetryCount = %d, want %d", opt.RetryCount, defaultRetryCount)
	}

	expectedRetryDelay := time.Duration(defaultRetryDelay) * time.Second
	if opt.RetryDelay != expectedRetryDelay {
		t.Errorf("RetryDelay = %v, want %v", opt.RetryDelay, expectedRetryDelay)
	}

	expectedTimeout := time.Duration(defaultAdminTimeout) * time.Second
	if opt.Timeout != expectedTimeout {
		t.Errorf("Timeout = %v, want %v", opt.Timeout, expectedTimeout)
	}

	if opt.SessionTimeout != defaultSessionTimeout {
		t.Errorf("SessionTimeout = %d, want %d", opt.SessionTimeout, defaultSessionTimeout)
	}

	if opt.OffsetReset != defaultOffsetRest {
		t.Errorf("OffsetReset = %q, want %q", opt.OffsetReset, defaultOffsetRest)
	}

	if opt.DLQTopic != defaultDQLTopic {
		t.Errorf("DLQTopic = %q, want %q", opt.DLQTopic, defaultDQLTopic)
	}

	if opt.PartitionKeyHeader != defaultPartitionKeyHeader {
		t.Errorf("PartitionKeyHeader = %q, want %q", opt.PartitionKeyHeader, defaultPartitionKeyHeader)
	}

	if opt.PartitionCacheTTL != defaultPartitionCacheTTL {
		t.Errorf("PartitionCacheTTL = %v, want %v", opt.PartitionCacheTTL, defaultPartitionCacheTTL)
	}

	// Verify err is cleared
	if opt.err != nil {
		t.Errorf("err should be nil after parseOption, got %v", opt.err)
	}
}

func TestParseOption_CustomValues(t *testing.T) {
	// Create an option with custom values - parseOption should preserve them
	customRetryDelay := 30 * time.Second
	customTimeout := 60 * time.Second

	opt := &EventOption{
		Brokers:            []string{"localhost:9092"},
		GroupId:            "test-group",
		RetryCount:         5,
		RetryDelay:         customRetryDelay,
		Timeout:            customTimeout,
		SessionTimeout:     10000,
		OffsetReset:        "latest",
		DLQTopic:           "my-dlq",
		PartitionKeyHeader: "myCustomKey",
		PartitionCacheTTL:  10 * time.Minute,
	}

	parseOption(opt)

	// Verify custom values are preserved
	if opt.RetryCount != 5 {
		t.Errorf("RetryCount = %d, want 5", opt.RetryCount)
	}

	if opt.RetryDelay != customRetryDelay {
		t.Errorf("RetryDelay = %v, want %v", opt.RetryDelay, customRetryDelay)
	}

	if opt.Timeout != customTimeout {
		t.Errorf("Timeout = %v, want %v", opt.Timeout, customTimeout)
	}

	if opt.SessionTimeout != 10000 {
		t.Errorf("SessionTimeout = %d, want 10000", opt.SessionTimeout)
	}

	if opt.OffsetReset != "latest" {
		t.Errorf("OffsetReset = %q, want %q", opt.OffsetReset, "latest")
	}

	if opt.DLQTopic != "my-dlq" {
		t.Errorf("DLQTopic = %q, want %q", opt.DLQTopic, "my-dlq")
	}

	if opt.PartitionKeyHeader != "myCustomKey" {
		t.Errorf("PartitionKeyHeader = %q, want %q", opt.PartitionKeyHeader, "myCustomKey")
	}

	if opt.PartitionCacheTTL != 10*time.Minute {
		t.Errorf("PartitionCacheTTL = %v, want %v", opt.PartitionCacheTTL, 10*time.Minute)
	}
}

func TestParseOption_BoundaryValues(t *testing.T) {
	// Test boundary conditions: values at or below thresholds should be replaced with defaults

	t.Run("RetryCount zero gets default", func(t *testing.T) {
		opt := &EventOption{RetryCount: 0}
		parseOption(opt)
		if opt.RetryCount != defaultRetryCount {
			t.Errorf("RetryCount = %d, want %d", opt.RetryCount, defaultRetryCount)
		}
	})

	t.Run("RetryCount negative gets default", func(t *testing.T) {
		opt := &EventOption{RetryCount: -1}
		parseOption(opt)
		if opt.RetryCount != defaultRetryCount {
			t.Errorf("RetryCount = %d, want %d", opt.RetryCount, defaultRetryCount)
		}
	})

	t.Run("RetryDelay 1 second gets default", func(t *testing.T) {
		opt := &EventOption{RetryDelay: 1 * time.Second}
		parseOption(opt)
		expectedRetryDelay := time.Duration(defaultRetryDelay) * time.Second
		if opt.RetryDelay != expectedRetryDelay {
			t.Errorf("RetryDelay = %v, want %v", opt.RetryDelay, expectedRetryDelay)
		}
	})

	t.Run("RetryDelay above threshold preserved", func(t *testing.T) {
		opt := &EventOption{RetryDelay: 2 * time.Second}
		parseOption(opt)
		if opt.RetryDelay != 2*time.Second {
			t.Errorf("RetryDelay = %v, want %v", opt.RetryDelay, 2*time.Second)
		}
	})

	t.Run("Timeout 1 second gets default", func(t *testing.T) {
		opt := &EventOption{Timeout: 1 * time.Second}
		parseOption(opt)
		expectedTimeout := time.Duration(defaultAdminTimeout) * time.Second
		if opt.Timeout != expectedTimeout {
			t.Errorf("Timeout = %v, want %v", opt.Timeout, expectedTimeout)
		}
	})

	t.Run("SessionTimeout zero gets default", func(t *testing.T) {
		opt := &EventOption{SessionTimeout: 0}
		parseOption(opt)
		if opt.SessionTimeout != defaultSessionTimeout {
			t.Errorf("SessionTimeout = %d, want %d", opt.SessionTimeout, defaultSessionTimeout)
		}
	})

	t.Run("SessionTimeout negative gets default", func(t *testing.T) {
		opt := &EventOption{SessionTimeout: -100}
		parseOption(opt)
		if opt.SessionTimeout != defaultSessionTimeout {
			t.Errorf("SessionTimeout = %d, want %d", opt.SessionTimeout, defaultSessionTimeout)
		}
	})
}

func TestEvent_Structure(t *testing.T) {
	// Test that Event struct fields work correctly
	event := &Event{
		Topic: "test-topic",
		Key:   "test-key",
		Val:   []byte("test-value"),
		Headers: map[string]string{
			"header1": "value1",
			"header2": "value2",
		},
	}

	if event.Topic != "test-topic" {
		t.Errorf("Topic = %q, want %q", event.Topic, "test-topic")
	}
	if event.Key != "test-key" {
		t.Errorf("Key = %q, want %q", event.Key, "test-key")
	}
	if string(event.Val) != "test-value" {
		t.Errorf("Val = %q, want %q", string(event.Val), "test-value")
	}
	if len(event.Headers) != 2 {
		t.Errorf("Headers length = %d, want 2", len(event.Headers))
	}
	if event.Headers["header1"] != "value1" {
		t.Errorf("Headers[header1] = %q, want %q", event.Headers["header1"], "value1")
	}
}
