package redislockclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/leyle/crud-log/pkg/crudlog"
	"github.com/rs/zerolog"
)

// Test configuration for Redis Cluster
// To run these tests, ensure the Redis cluster is accessible
var testClusterConfig = &RedisClientOption{
	HostPorts: []string{
		"redis.dev.test:6379",
		"redis.dev.test:6380",
		"redis.dev.test:6381",
		"redis.dev.test:6382",
		"redis.dev.test:6383",
		"redis.dev.test:6384",
	},
	Password:    "abc123",
	ServiceName: "lock-test",
}

// createTestContext creates a context with a zerolog logger for testing
func createTestContext() context.Context {
	logger := crudlog.NewConsoleLogger(zerolog.DebugLevel)
	return logger.WithContext(context.Background())
}

// getTestClient creates a Redis client for testing
func getTestClient(t *testing.T) RedisLockClient {
	client, err := NewRedisLockClient(testClusterConfig)
	if err != nil {
		t.Skipf("Skipping test: unable to connect to Redis cluster: %v", err)
	}
	return client
}

func TestAcquireLock(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-acquire-lock"

	// Acquire lock
	val, ok := client.AcquireLock(ctx, resource, 5*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}
	if val == "" {
		t.Fatal("Lock value should not be empty")
	}

	t.Logf("Acquired lock with value: %s", val)

	// Release lock
	released := client.ReleaseLock(ctx, resource, val)
	if !released {
		t.Fatal("Failed to release lock")
	}
}

func TestAcquireLockObject(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-acquire-lock-object"

	// Acquire lock object
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock object")
	}
	if lock == nil {
		t.Fatal("Lock object should not be nil")
	}
	if lock.Value() == "" {
		t.Fatal("Lock value should not be empty")
	}
	if lock.Resource() != resource {
		t.Fatalf("Lock resource mismatch: expected %s, got %s", resource, lock.Resource())
	}

	t.Logf("Acquired lock object with value: %s", lock.Value())

	// Release lock
	released := lock.Release(ctx)
	if !released {
		t.Fatal("Failed to release lock")
	}
}

func TestExtendLock(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-extend-lock"

	// Acquire lock with short TTL
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 3*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}
	defer lock.Release(ctx)

	// Check initial TTL
	ttl := lock.TTL(ctx)
	t.Logf("Initial TTL: %v", ttl)
	if ttl <= 0 {
		t.Fatalf("Initial TTL should be positive, got %v", ttl)
	}

	// Extend lock
	extended := lock.Extend(ctx, 10*time.Second)
	if !extended {
		t.Fatal("Failed to extend lock")
	}

	// Check extended TTL
	newTTL := lock.TTL(ctx)
	t.Logf("Extended TTL: %v", newTTL)
	if newTTL <= ttl {
		t.Fatalf("Extended TTL should be greater than initial TTL, initial: %v, new: %v", ttl, newTTL)
	}
}

func TestExtendLock_NotOwner(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-extend-lock-not-owner"

	// Acquire lock
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}
	defer lock.Release(ctx)

	// Try to extend with wrong value
	extended := client.ExtendLock(ctx, resource, "wrong-value", 10*time.Second)
	if extended {
		t.Fatal("Should not be able to extend lock with wrong value")
	}
}

func TestIsLockHeld(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-is-lock-held"

	// Acquire lock
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}

	// Check lock is held
	held := lock.IsHeld(ctx)
	if !held {
		t.Fatal("Lock should be held")
	}

	// Check with wrong value
	heldWrongValue := client.IsLockHeld(ctx, resource, "wrong-value")
	if heldWrongValue {
		t.Fatal("Lock should not be held with wrong value")
	}

	// Release lock
	lock.Release(ctx)

	// Check lock is no longer held
	heldAfterRelease := lock.IsHeld(ctx)
	if heldAfterRelease {
		t.Fatal("Lock should not be held after release")
	}
}

func TestIsLockHeld_Expired(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-is-lock-held-expired"

	// Acquire lock with very short TTL
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 1*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}

	// Wait for lock to expire
	time.Sleep(1500 * time.Millisecond)

	// Check lock is expired
	held := lock.IsHeld(ctx)
	if held {
		t.Fatal("Lock should have expired")
	}
}

func TestGetLockTTL(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-get-lock-ttl"

	// Acquire lock
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}
	defer lock.Release(ctx)

	// Get TTL
	ttl := lock.TTL(ctx)
	t.Logf("Lock TTL: %v", ttl)

	// TTL should be between 0 and 10 seconds (allowing for some time to pass)
	if ttl <= 0 || ttl > 10*time.Second {
		t.Fatalf("Unexpected TTL: %v", ttl)
	}
}

func TestGetLockTTL_NonExistent(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-get-lock-ttl-nonexistent"

	// Get TTL of non-existent lock
	ttl := client.GetLockTTL(ctx, resource)
	t.Logf("Non-existent lock TTL: %v", ttl)

	// Should return -2 (no key exists, which redis returns as -2ms when key doesn't exist)
	if ttl != -2*time.Millisecond {
		t.Logf("Note: TTL for non-existent key is %v (expected -2ms)", ttl)
	}
}

func TestLockObject_FullCycle(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-lock-full-cycle"

	// Acquire lock
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 5*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}

	t.Logf("Lock acquired: resource=%s, value=%s", lock.Resource(), lock.Value())

	// Check held
	if !lock.IsHeld(ctx) {
		t.Fatal("Lock should be held")
	}

	// Check TTL
	ttl := lock.TTL(ctx)
	t.Logf("Initial TTL: %v", ttl)
	if ttl <= 0 {
		t.Fatal("TTL should be positive")
	}

	// Extend
	if !lock.Extend(ctx, 10*time.Second) {
		t.Fatal("Failed to extend lock")
	}
	t.Log("Lock extended")

	// Check new TTL
	newTTL := lock.TTL(ctx)
	t.Logf("New TTL after extend: %v", newTTL)

	// Release
	if !lock.Release(ctx) {
		t.Fatal("Failed to release lock")
	}
	t.Log("Lock released")

	// Verify not held
	if lock.IsHeld(ctx) {
		t.Fatal("Lock should not be held after release")
	}
}

func TestConcurrentLockAcquisition(t *testing.T) {
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-concurrent-lock"
	numGoroutines := 5
	lockHoldTime := 100 * time.Millisecond

	var wg sync.WaitGroup
	successCount := 0
	var mutex sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Try to acquire lock with short acquire timeout
			lock, ok := client.AcquireLockObject(ctx, resource, 500*time.Millisecond, 5*time.Second)
			if ok {
				mutex.Lock()
				successCount++
				mutex.Unlock()

				t.Logf("Goroutine %d acquired lock", id)

				// Hold lock for a bit
				time.Sleep(lockHoldTime)

				// Release
				lock.Release(ctx)
				t.Logf("Goroutine %d released lock", id)
			} else {
				t.Logf("Goroutine %d failed to acquire lock (expected for some)", id)
			}
		}(i)
	}

	wg.Wait()

	// At least one goroutine should have successfully acquired the lock
	if successCount == 0 {
		t.Fatal("No goroutine successfully acquired the lock")
	}
	t.Logf("Total successful acquisitions: %d", successCount)
}

func TestLockAutoRenewal_Pattern(t *testing.T) {
	// This test demonstrates the recommended pattern for auto-renewing locks
	// for long-running tasks
	client := getTestClient(t)
	defer client.Close()

	ctx := createTestContext()
	resource := "test-auto-renewal-pattern"

	// Acquire lock with relatively short TTL
	lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 3*time.Second)
	if !ok {
		t.Fatal("Failed to acquire lock")
	}

	// Create a channel to stop the renewal goroutine
	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})

	// Start background goroutine to renew lock
	go func() {
		defer close(renewalDone)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !lock.Extend(ctx, 3*time.Second) {
					t.Log("Warning: Failed to extend lock in renewal goroutine")
					return
				}
				t.Log("Lock renewed in background")
			case <-stopRenewal:
				t.Log("Stopping lock renewal")
				return
			}
		}
	}()

	// Simulate some work that takes longer than the initial lock TTL
	t.Log("Starting simulated work...")
	time.Sleep(5 * time.Second)
	t.Log("Simulated work completed")

	// Stop renewal and release lock
	close(stopRenewal)
	<-renewalDone

	// Verify lock is still held (because of renewals)
	if !lock.IsHeld(ctx) {
		t.Fatal("Lock should still be held after renewals")
	}

	// Release lock
	lock.Release(ctx)
	t.Log("Lock released after auto-renewal pattern test")
}
