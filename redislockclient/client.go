package redislockclient

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisLockClient interface {
	redis.Cmdable
	Close() error
	AcquireLock(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (string, bool)
	ReleaseLock(ctx context.Context, resource, val string) bool
	GenerateRedisKey(moduleName, userKey string) string
}

func NewRedisLockClient(cfg *RedisClientOption) (RedisLockClient, error) {
	hostPorts := cfg.HostPorts
	if len(hostPorts) == 1 {
		return NewSingletonRedisClient(cfg)
	}

	if cfg.MasterName != "" {
		return NewSentinelRedisClient(cfg)
	}

	return NewClusterRedisClient(cfg)
}
