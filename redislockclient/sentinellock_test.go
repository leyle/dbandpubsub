package redislockclient

import (
	"errors"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
)

var sentinelHostPorts = []string{
	"redis.dev.test:26379",
	"redis.dev.test:26380",
	"redis.dev.test:26381",
	"redis.dev.test:26382",
	"redis.dev.test:26383",
	"redis.dev.test:26384",
}

func newRedisClientSentinel(t *testing.T) *SentinelRedisClient {
	var cfg = &RedisClientOption{
		HostPorts:   sentinelHostPorts,
		ServiceName: "TEST",
		MasterName:  "mymaster",
	}

	client, err := NewSentinelRedisClient(cfg)
	if err != nil {
		t.Skipf("Skipping sentinel test: %v (expected if no sentinel available)", err)
	}
	return client
}

func TestNewSentinelRedisClient(t *testing.T) {
	client := newRedisClientSentinel(t)
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
