package redislockclient

import (
	"errors"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

var hosts = []string{
	"redis.x1c.pymom.com:6379",
	"redis.x1c.pymom.com:6380",
	"redis.x1c.pymom.com:6381",
	"redis.x1c.pymom.com:6382",
	"redis.x1c.pymom.com:6383",
	"redis.x1c.pymom.com:6384",
}

var clusterCfg = &RedisClientOption{
	HostPorts:   hosts,
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
		t.Error(err)
	}
	t.Log("set", key, val, "done")

	result, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Error(err)
	}

	t.Log("get", key, "result", result)
}

func TestRedisClientCluster_AcquireLock(t *testing.T) {
	client, err := NewClusterRedisClient(clusterCfg)
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
