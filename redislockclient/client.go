package redislockclient

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLockClient defines the interface for distributed locking with Redis.
// It supports singleton, sentinel, and cluster modes.
type RedisLockClient interface {
	redis.Cmdable
	Close() error

	// AcquireLock attempts to acquire a distributed lock on the specified resource.
	// acquireTimeout: maximum time to wait for the lock
	// lockTimeout: TTL of the lock once acquired
	// Returns the lock value (for ownership verification) and success status.
	AcquireLock(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (string, bool)

	// ReleaseLock releases a previously acquired lock.
	// The val parameter must match the value returned by AcquireLock.
	ReleaseLock(ctx context.Context, resource, val string) bool

	// GenerateRedisKey generates a namespaced Redis key for the given module and user key.
	GenerateRedisKey(moduleName, userKey string) string

	// AcquireLockObject attempts to acquire a lock and returns a Lock object for easier management.
	// This is the recommended way to acquire locks as it provides a more ergonomic API.
	AcquireLockObject(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (*Lock, bool)

	// ExtendLock extends the TTL of an existing lock if the caller still owns it.
	// Returns true if the lock was successfully extended.
	ExtendLock(ctx context.Context, resource, val string, extendDuration time.Duration) bool

	// IsLockHeld checks if the lock is currently held by the caller with the specified value.
	// Returns true if the lock exists and the value matches.
	IsLockHeld(ctx context.Context, resource, val string) bool

	// GetLockTTL returns the remaining TTL of a lock.
	// Returns -2 if the key doesn't exist, -1 if no TTL is set.
	GetLockTTL(ctx context.Context, resource string) time.Duration
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
