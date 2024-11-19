package redislockclient

import (
	"errors"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"testing"
	"time"
)

var sentinelHostPorts = []string{
	"redis.x1c.pymom.com:26379",
	"redis.x1c.pymom.com:26380",
	"redis.x1c.pymom.com:26381",
}

func newRedisClientSentinel() *SentinelRedisClient {
	var cfg = &RedisClientOption{
		HostPorts:   sentinelHostPorts,
		ServiceName: "TEST",
		MasterName:  "mymaster",
	}

	client, err := NewSentinelRedisClient(cfg)
	if err != nil {
		panic(err)
	}
	return client
}

func TestNewSentinelRedisClient(t *testing.T) {
	client := newRedisClientSentinel()
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
