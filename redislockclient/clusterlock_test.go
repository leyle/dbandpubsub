package redislockclient

import (
	"errors"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/redis/go-redis/v9"
)

var clusterHostPorts = []string{
	"redis.dev.test:6379",
	"redis.dev.test:6380",
	"redis.dev.test:6381",
	"redis.dev.test:6382",
	"redis.dev.test:6383",
	"redis.dev.test:6384",
}

var clusterCfg = &RedisClientOption{
	HostPorts:   clusterHostPorts,
	Password:    "abc123",
	ServiceName: "TEST-CLUSTER",
}

func TestNewRedisClientCluster(t *testing.T) {
	opt := &redis.ClusterOptions{
		Addrs:    clusterCfg.HostPorts,
		Password: clusterCfg.Password,
	}

	rdb := redis.NewClusterClient(opt)

	ctx := wrapZeroLogContext()
	key := objectid.GetObjectId()
	val := objectid.GetObjectId()
	err := rdb.Set(ctx, key, val, 0).Err()
	if err != nil {
		t.Skipf("Skipping cluster test: %v", err)
	}
	t.Log("set", key, val, "done")

	result, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Skipf("Skipping cluster test: %v", err)
	}

	t.Log("get", key, "result", result)
}

func TestRedisClientCluster_AcquireLock(t *testing.T) {
	client, err := NewClusterRedisClient(clusterCfg)
	if err != nil {
		t.Skipf("Skipping cluster lock test: %v", err)
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
	if !ok {
		t.Log("ok, lock failed as expected")
	}

	if ok {
		t.Log("lockVal2", lockVal2)
		t.Fatal(errors.New("expected to not acquire lock but did"))
	}

	// release first lock
	_ = client.ReleaseLock(ctx, resource, lockVal)
}
