package redislockclient

import (
	"errors"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"testing"
	"time"
)

func TestNewRedisSingletonClient(t *testing.T) {
	// singleton mode
	var cfg = &RedisClientOption{
		HostPorts:   []string{"redis.x1c.pymom.com:6379"},
		Password:    "abc123",
		DbNO:        defaultDbNO,
		ServiceName: "SINGLETON-TEST",
	}

	client, err := NewRedisLockClient(cfg)
	if err != nil {
		t.Error(err)
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
		t.Error(err)
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
		t.Error(err)
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
