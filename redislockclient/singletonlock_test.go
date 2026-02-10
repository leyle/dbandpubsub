package redislockclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/logclient"
	"github.com/rs/zerolog"
)

func newRedisClientSingleTon(t *testing.T) *SingletonRedisClient {
	// Note: Singleton mode only works with a single Redis instance, not a cluster.
	// If your environment only has a Redis cluster, this test will be skipped.
	var cfg = &RedisClientOption{
		HostPorts:   []string{"redis.dev.test:6379"},
		Password:    "abc123",
		DbNO:        defaultDbNO,
		ServiceName: "TEST",
	}
	client, err := NewSingletonRedisClient(cfg)
	if err != nil {
		t.Skipf("Skipping singleton test: %v (expected if only cluster is available)", err)
	}
	return client
}

func TestRedisClientSingleton_AcquireLock(t *testing.T) {
	client := newRedisClientSingleTon(t)

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

func wrapZeroLogContext() context.Context {
	logger := logclient.NewConsoleLogger(zerolog.DebugLevel)
	ctx := logger.WithContext(context.Background())

	return ctx
}
