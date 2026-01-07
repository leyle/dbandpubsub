package kafkaconnector

import (
	"testing"
	"time"
)

func TestPartitionCacheEntry_Structure(t *testing.T) {
	// Test that partition cache entry stores values correctly
	now := time.Now()
	entry := partitionCacheEntry{
		count:    12,
		cachedAt: now,
	}

	if entry.count != 12 {
		t.Errorf("count = %d, want 12", entry.count)
	}

	if !entry.cachedAt.Equal(now) {
		t.Errorf("cachedAt = %v, want %v", entry.cachedAt, now)
	}
}

func TestPartitionCacheEntry_TTLExpiry(t *testing.T) {
	// Test the TTL expiry logic that's used in getPartitionCount
	ttl := 5 * time.Minute

	tests := []struct {
		name      string
		cachedAt  time.Time
		shouldUse bool // whether cache should still be valid
	}{
		{
			name:      "just cached",
			cachedAt:  time.Now(),
			shouldUse: true,
		},
		{
			name:      "cached 1 minute ago",
			cachedAt:  time.Now().Add(-1 * time.Minute),
			shouldUse: true,
		},
		{
			name:      "cached 4 minutes ago",
			cachedAt:  time.Now().Add(-4 * time.Minute),
			shouldUse: true,
		},
		{
			name:      "cached 5 minutes ago",
			cachedAt:  time.Now().Add(-5 * time.Minute),
			shouldUse: false,
		},
		{
			name:      "cached 10 minutes ago",
			cachedAt:  time.Now().Add(-10 * time.Minute),
			shouldUse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := partitionCacheEntry{
				count:    6,
				cachedAt: tt.cachedAt,
			}

			// Simulate the cache validity check from getPartitionCount
			isValid := time.Since(entry.cachedAt) < ttl

			if isValid != tt.shouldUse {
				t.Errorf("Cache validity = %v, want %v", isValid, tt.shouldUse)
			}
		})
	}
}

func TestEventConnector_Structure(t *testing.T) {
	// Verify default constants are consistent
	if defaultPartitionCacheTTL <= 0 {
		t.Errorf("defaultPartitionCacheTTL should be positive, got %v", defaultPartitionCacheTTL)
	}

	if defaultPartitionKeyHeader == "" {
		t.Error("defaultPartitionKeyHeader should not be empty")
	}

	if defaultRequestMsgSize <= 0 {
		t.Errorf("defaultRequestMsgSize should be positive, got %d", defaultRequestMsgSize)
	}
}

func TestDefaultConstants(t *testing.T) {
	// Verify all default constants have sensible values
	tests := []struct {
		name     string
		value    interface{}
		validate func(interface{}) bool
	}{
		{
			name:  "defaultSessionTimeout",
			value: defaultSessionTimeout,
			validate: func(v interface{}) bool {
				return v.(int) > 0
			},
		},
		{
			name:  "defaultAdminTimeout",
			value: defaultAdminTimeout,
			validate: func(v interface{}) bool {
				return v.(int) > 0
			},
		},
		{
			name:  "defaultOffsetRest",
			value: defaultOffsetRest,
			validate: func(v interface{}) bool {
				s := v.(string)
				return s == "earliest" || s == "latest"
			},
		},
		{
			name:  "defaultDQLTopic",
			value: defaultDQLTopic,
			validate: func(v interface{}) bool {
				return v.(string) != ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.validate(tt.value) {
				t.Errorf("Invalid default value for %s: %v", tt.name, tt.value)
			}
		})
	}
}

func TestPartitionCacheTTL_ZeroMeansDefault(t *testing.T) {
	// Test that PartitionCacheTTL=0 gets default in parseOption
	opt := &EventOption{
		Brokers:           []string{"localhost:9092"},
		PartitionCacheTTL: 0,
	}

	parseOption(opt)

	if opt.PartitionCacheTTL != defaultPartitionCacheTTL {
		t.Errorf("PartitionCacheTTL = %v, want %v (default)", opt.PartitionCacheTTL, defaultPartitionCacheTTL)
	}
}

func TestPartitionCacheTTL_NegativePreserved(t *testing.T) {
	// Test that negative PartitionCacheTTL is preserved (used to disable caching)
	opt := &EventOption{
		Brokers:           []string{"localhost:9092"},
		PartitionCacheTTL: -1 * time.Second,
	}

	parseOption(opt)

	// Negative values should be preserved (they disable caching)
	if opt.PartitionCacheTTL != -1*time.Second {
		t.Errorf("PartitionCacheTTL = %v, want -1s (preserved negative)", opt.PartitionCacheTTL)
	}
}
