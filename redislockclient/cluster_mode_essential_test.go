package redislockclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
)

func newEssentialClusterClient(t *testing.T) *ClusterRedisClient {
	t.Helper()

	client, err := NewClusterRedisClient(clusterCfg)
	if err != nil {
		t.Skipf("Skipping cluster essential tests: %v", err)
	}
	return client
}

func TestClusterMode_AcquireRelease_Basic(t *testing.T) {
	client := newEssentialClusterClient(t)
	defer client.Close()

	ctx := wrapZeroLogContext()
	resource := "cluster-basic-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	val, ok := client.AcquireLock(ctx, resource, 3*time.Second, 5*time.Second)
	if !ok || val == "" {
		t.Fatal("failed to acquire cluster lock")
	}
	t.Log("lock val", val)

	if !client.ReleaseLock(ctx, resource, val) {
		t.Fatal("failed to release cluster lock")
	}
	t.Log("release lock done")

	if client.IsLockHeld(ctx, resource, val) {
		t.Fatal("lock should not be held after release")
	}
	t.Log("lock is no longer held")
}

func TestClusterMode_ReleaseWrongValue_DoesNotDeleteOwnerLock(t *testing.T) {
	client := newEssentialClusterClient(t)
	defer client.Close()

	ctx := wrapZeroLogContext()
	resource := "cluster-wrong-release-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	val, ok := client.AcquireLock(ctx, resource, 3*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("failed to acquire cluster lock")
	}
	t.Log("owner lock val", val)
	defer client.ReleaseLock(ctx, resource, val)

	// By design, this returns true, but it must not delete the owner lock.
	wrongVal := "not-owner-" + objectid.GetObjectId()
	t.Log("release with wrong val", wrongVal)
	if !client.ReleaseLock(ctx, resource, wrongVal) {
		t.Fatal("release with wrong value should still return success by design")
	}
	t.Log("wrong-value release returned success as designed")

	if !client.IsLockHeld(ctx, resource, val) {
		t.Fatal("owner lock should still be present after wrong-value release")
	}
	t.Log("owner lock remains held")
}

func TestClusterMode_AcquireLock_RespectsCanceledContext(t *testing.T) {
	client := newEssentialClusterClient(t)
	defer client.Close()

	ctx := wrapZeroLogContext()
	resource := "cluster-cancel-acquire-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	heldVal, ok := client.AcquireLock(ctx, resource, 3*time.Second, 10*time.Second)
	if !ok {
		t.Fatal("failed to pre-acquire cluster lock")
	}
	t.Log("pre-acquired lock val", heldVal)
	defer client.ReleaseLock(ctx, resource, heldVal)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	start := time.Now()
	val, got := client.AcquireLock(canceledCtx, resource, 5*time.Second, 5*time.Second)
	elapsed := time.Since(start)
	t.Log("canceled acquire result", "ok:", got, "val:", val, "elapsed:", elapsed)

	if got || val != "" {
		t.Fatal("acquire should fail with canceled context")
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("canceled acquire should return quickly, took %v", elapsed)
	}
}

func TestClusterMode_ExtendLock_RespectsCanceledContext(t *testing.T) {
	client := newEssentialClusterClient(t)
	defer client.Close()

	ctx := wrapZeroLogContext()
	resource := "cluster-cancel-extend-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	lock, ok := client.AcquireLockObject(ctx, resource, 3*time.Second, 5*time.Second)
	if !ok {
		t.Fatal("failed to acquire cluster lock object")
	}
	t.Log("lock val", lock.Value())
	defer lock.Release(ctx)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	if lock.Extend(canceledCtx, 5*time.Second) {
		t.Fatal("extend should fail with canceled context")
	}
	t.Log("extend with canceled context failed as expected")
}

func TestClusterMode_GetLockTTL_NonExistentKey(t *testing.T) {
	client := newEssentialClusterClient(t)
	defer client.Close()

	ctx := wrapZeroLogContext()
	resource := "cluster-no-ttl-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	ttl := client.GetLockTTL(ctx, resource)
	t.Log("ttl result", ttl)
	if ttl != -2 {
		t.Fatalf("expected -2 for non-existent key, got %v", ttl)
	}
}

func TestClusterMode_HighContention_ExclusiveCriticalSection(t *testing.T) {
	ctx := wrapZeroLogContext()
	resource := "cluster-high-contention-" + objectid.GetObjectId()
	t.Log("lock key:", resource)

	const workers = 8
	const roundsPerWorker = 8

	var wg sync.WaitGroup
	var inCritical int32
	var maxInCritical int32
	var successfulAcquires int32
	var violations int32
	var activeWorkers int32

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var client *ClusterRedisClient
			var err error
			for attempt := 0; attempt < 3; attempt++ {
				client, err = NewClusterRedisClient(clusterCfg)
				if err == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if err != nil {
				// Avoid flaking the whole contention test on transient dial/init errors.
				t.Log("worker", workerID, "failed to init cluster client after retries")
				return
			}
			defer client.Close()
			atomic.AddInt32(&activeWorkers, 1)
			t.Log("worker", workerID, "started")

			for j := 0; j < roundsPerWorker; j++ {
				val, ok := client.AcquireLock(ctx, resource, 2*time.Second, 500*time.Millisecond)
				if !ok {
					continue
				}
				t.Log("worker", workerID, "round", j, "acquired", val)

				atomic.AddInt32(&successfulAcquires, 1)

				n := atomic.AddInt32(&inCritical, 1)
				for {
					m := atomic.LoadInt32(&maxInCritical)
					if n <= m || atomic.CompareAndSwapInt32(&maxInCritical, m, n) {
						break
					}
				}
				if n > 1 {
					atomic.AddInt32(&violations, 1)
				}

				time.Sleep(30 * time.Millisecond)

				atomic.AddInt32(&inCritical, -1)
				_ = client.ReleaseLock(ctx, resource, val)
				t.Log("worker", workerID, "round", j, "released", val)
			}
		}(i)
	}

	wg.Wait()

	if successfulAcquires == 0 {
		t.Fatal("expected at least one successful lock acquisition under contention")
	}
	if activeWorkers == 0 {
		t.Fatal("all workers failed to initialize cluster clients")
	}
	if violations != 0 {
		t.Fatalf("critical section exclusivity violated %d times (max concurrent=%d)", violations, maxInCritical)
	}
	if maxInCritical > 1 {
		t.Fatalf("expected max concurrent critical sections to be 1, got %d", maxInCritical)
	}
	t.Log("contention summary", "activeWorkers:", activeWorkers, "successfulAcquires:", successfulAcquires, "maxConcurrent:", maxInCritical, "violations:", violations)
}
