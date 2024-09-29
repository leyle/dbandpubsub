package redislockclient

import (
	"context"
	"errors"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/leyle/dbandpubsub/logclient"
	"github.com/rs/zerolog"
	"testing"
	"time"
)

func newRedisClientSingleTon() *SingletonRedisClient {
	var cfg = &RedisClientOption{
		HostPorts:   []string{"redis.x1c.pymom.com:6379"},
		Password:    "abc123",
		DbNO:        defaultDbNO,
		ServiceName: "TEST",
	}
	client, err := NewSingletonRedisClient(cfg)
	if err != nil {
		panic(err)
	}
	return client
}

func TestRedisClientSingleton_AcquireLock(t *testing.T) {
	client := newRedisClientSingleTon()

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

func wrapZeroLogContext() context.Context {
	logger := logclient.NewConsoleLogger(zerolog.DebugLevel)
	ctx := logger.WithContext(context.Background())

	return ctx
}
