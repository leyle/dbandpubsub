package redislockclient

import (
	"errors"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
)

func TestNewRedisSingletonClient(t *testing.T) {
	// Skip this test when only Redis cluster is available
	// Singleton mode requires a standalone Redis instance
	t.Skip("Skipping singleton test: environment has Redis cluster only")

	// singleton mode - uses single host
	// Note: This test requires a standalone Redis instance, not a cluster
	var cfg = &RedisClientOption{
		HostPorts:   []string{"redis.dev.test:6379"},
		Password:    "abc123",
		DbNO:        defaultDbNO,
		ServiceName: "SINGLETON-TEST",
	}

	client, err := NewRedisLockClient(cfg)
	if err != nil {
		t.Skipf("Skipping singleton test: %v", err)
	}

	ctx := wrapZeroLogContext()

	resource := objectid.GetObjectId()
	t.Log("lock key:", resource)

	lockVal, ok := client.AcquireLock(ctx, resource, -1, -1)
	if !ok {
		t.Fatal(errors.New("failed to get lock"))
	}
	t.Log("lock val", lockVal)

	// test that another lock attempt fails while the first lock is held
	// this lock should wait 5 secs and failed, due to first lock is last for 10 secs
	lockVal2, ok := client.AcquireLock(ctx, resource, 5*time.Second, -1)
	if ok {
		t.Log("lockVal2", lockVal2)
		t.Fatal(errors.New("expected to not acquire lock but did"))
	}
	if !ok {
		t.Log("ok, lock failed as expected")
	}

	// release first lock
	_ = client.ReleaseLock(ctx, resource, lockVal)
}

func TestNewRedisClusterClient(t *testing.T) {
	// cluster mode
	var cfg = &RedisClientOption{
		HostPorts:   clusterHostPorts,
		Password:    "abc123",
		ServiceName: "CLUSTER-TEST",
	}

	client, err := NewRedisLockClient(cfg)
	if err != nil {
		t.Skipf("Skipping cluster test: %v", err)
	}

	ctx := wrapZeroLogContext()

	resource := objectid.GetObjectId()
	t.Log("lock key:", resource)

	lockVal, ok := client.AcquireLock(ctx, resource, -1, -1)
	if !ok {
		t.Fatal(errors.New("failed to get lock"))
	}
	t.Log("lock val", lockVal)

	// test that another lock attempt fails while the first lock is held
	// this lock should wait 5 secs and failed, due to first lock is last for 10 secs
	lockVal2, ok := client.AcquireLock(ctx, resource, 5*time.Second, -1)
	if ok {
		t.Log("lockVal2", lockVal2)
		t.Fatal(errors.New("expected to not acquire lock but did"))
	}
	if !ok {
		t.Log("ok, lock failed as expected")
	}

	// release first lock
	_ = client.ReleaseLock(ctx, resource, lockVal)
}

func TestNewRedisSentinelRedis(t *testing.T) {
	// sentinel mode
	var cfg = &RedisClientOption{
		HostPorts:   sentinelHostPorts,
		Password:    "",
		ServiceName: "SENTINEL-TEST",
		MasterName:  "mymaster",
	}

	client, err := NewRedisLockClient(cfg)
	if err != nil {
		t.Skipf("Skipping sentinel test: %v", err)
	}

	ctx := wrapZeroLogContext()

	resource := objectid.GetObjectId()
	t.Log("lock key:", resource)

	lockVal, ok := client.AcquireLock(ctx, resource, -1, -1)
	if !ok {
		t.Fatal(errors.New("failed to get lock"))
	}
	t.Log("lock val", lockVal)

	// test that another lock attempt fails while the first lock is held
	// this lock should wait 5 secs and failed, due to first lock is last for 10 secs
	lockVal2, ok := client.AcquireLock(ctx, resource, 5*time.Second, -1)
	if ok {
		t.Log("lockVal2", lockVal2)
		t.Fatal(errors.New("expected to not acquire lock but did"))
	}
	if !ok {
		t.Log("ok, lock failed as expected")
	}

	// release first lock
	_ = client.ReleaseLock(ctx, resource, lockVal)
}
