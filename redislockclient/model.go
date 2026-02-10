package redislockclient

import (
	"context"
	"time"
)

const (
	moduleName  = "LOCK"
	defaultDbNO = 0
)

const (
	defaultAcquireTimeout time.Duration = 10 * time.Second
	defaultLockKeyTimout  time.Duration = 10 * time.Second
	defaultRetryDuration  time.Duration = 10 * time.Millisecond
)

type RedisClientOption struct {
	HostPorts   []string
	Password    string
	DbNO        int    // redis database number, only work for singleton
	ServiceName string // application service name
	MasterName  string // redis sentinel name, only work for sentinel mode
}

// Lock represents an acquired distributed lock.
// It encapsulates the lock state and provides methods for lock management.
type Lock struct {
	client   RedisLockClient
	resource string
	val      string
}

// NewLock creates a new Lock instance. This is typically called internally
// by AcquireLockObject after successfully acquiring a lock.
func NewLock(client RedisLockClient, resource, val string) *Lock {
	return &Lock{
		client:   client,
		resource: resource,
		val:      val,
	}
}

// Extend extends the lock TTL by the specified duration.
// Returns true if the lock was successfully extended, false if the lock
// is no longer owned by this caller (expired or taken by another).
func (l *Lock) Extend(ctx context.Context, duration time.Duration) bool {
	return l.client.ExtendLock(ctx, l.resource, l.val, duration)
}

// Release releases the lock.
// Returns true if the lock was successfully released.
func (l *Lock) Release(ctx context.Context) bool {
	return l.client.ReleaseLock(ctx, l.resource, l.val)
}

// IsHeld checks if the lock is still held by this caller.
// Returns true if the lock exists and is owned by this caller.
func (l *Lock) IsHeld(ctx context.Context) bool {
	return l.client.IsLockHeld(ctx, l.resource, l.val)
}

// TTL returns the remaining time-to-live of the lock.
// Returns -2 if the lock doesn't exist, -1 if no TTL is set.
func (l *Lock) TTL(ctx context.Context) time.Duration {
	return l.client.GetLockTTL(ctx, l.resource)
}

// Value returns the unique lock value that proves ownership.
// Useful for debugging and logging.
func (l *Lock) Value() string {
	return l.val
}

// Resource returns the resource name that is locked.
func (l *Lock) Resource() string {
	return l.resource
}
