package kafkaconnector

import (
	"testing"
)

func TestCalculatePartition_Consistency(t *testing.T) {
	// Same key should always map to the same partition
	key := "test-data-id-12345"
	partitionCount := 12

	firstResult := calculatePartition(key, partitionCount)

	// Run 100 times to ensure consistency
	for i := 0; i < 100; i++ {
		result := calculatePartition(key, partitionCount)
		if result != firstResult {
			t.Errorf("Inconsistent partition: got %d, expected %d on iteration %d", result, firstResult, i)
		}
	}
}

func TestCalculatePartition_Distribution(t *testing.T) {
	// Different keys should distribute across partitions
	partitionCount := 12
	partitionHits := make(map[int32]int)

	// Generate 1000 different keys
	for i := 0; i < 1000; i++ {
		key := "unique-key-" + string(rune(i)) + "-" + string(rune(i*17))
		partition := calculatePartition(key, partitionCount)

		// Partition should be within valid range
		if partition < 0 || partition >= int32(partitionCount) {
			t.Errorf("Partition %d out of range [0, %d)", partition, partitionCount)
		}

		partitionHits[partition]++
	}

	// We should have at least some distribution (hit more than 1 partition)
	if len(partitionHits) < 2 {
		t.Errorf("Poor distribution: only %d partitions hit out of %d", len(partitionHits), partitionCount)
	}

	t.Logf("Distribution across %d partitions: %v", len(partitionHits), partitionHits)
}

func TestCalculatePartition_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		partitionCount int
		expectValid    bool
	}{
		{
			name:           "empty key single partition",
			key:            "",
			partitionCount: 1,
			expectValid:    true,
		},
		{
			name:           "empty key multiple partitions",
			key:            "",
			partitionCount: 10,
			expectValid:    true,
		},
		{
			name:           "long key",
			key:            "this-is-a-very-long-key-that-might-have-some-impact-on-hashing-" + string(make([]byte, 1000)),
			partitionCount: 100,
			expectValid:    true,
		},
		{
			name:           "unicode key",
			key:            "测试数据-κόσμε-🎉",
			partitionCount: 6,
			expectValid:    true,
		},
		{
			name:           "single partition always returns 0",
			key:            "any-key",
			partitionCount: 1,
			expectValid:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculatePartition(tt.key, tt.partitionCount)

			if tt.expectValid {
				if result < 0 || result >= int32(tt.partitionCount) {
					t.Errorf("Partition %d out of range [0, %d)", result, tt.partitionCount)
				}
			}
		})
	}
}

func TestCalculatePartition_SinglePartition(t *testing.T) {
	// With only 1 partition, all keys should map to partition 0
	keys := []string{"key1", "key2", "key3", "different", "another-key", ""}

	for _, key := range keys {
		result := calculatePartition(key, 1)
		if result != 0 {
			t.Errorf("Single partition: key %q got partition %d, expected 0", key, result)
		}
	}
}

func TestCalculatePartition_DifferentPartitionCounts(t *testing.T) {
	// Same key with different partition counts should give different results (usually)
	key := "consistent-test-key"

	results := make(map[int]int32)
	partitionCounts := []int{2, 3, 5, 7, 10, 12, 20, 100}

	for _, count := range partitionCounts {
		result := calculatePartition(key, count)
		results[count] = result

		// Result should always be in valid range
		if result < 0 || result >= int32(count) {
			t.Errorf("Partition count %d: result %d out of range", count, result)
		}
	}

	t.Logf("Same key with different partition counts: %v", results)
}
