# redislockclient

A Redis-based distributed lock wrapper for Go, with support for:
- singleton Redis
- Redis Sentinel
- Redis Cluster

This README focuses on **cluster mode**, which is the primary production mode.

## Import

```go
import "github.com/leyle/dbandpubsub/redislockclient"
```

## Lock lifecycle (cluster mode)

1. Create a cluster client.
2. Acquire a lock for a resource.
3. Run critical section logic.
4. (Optional) Extend lock TTL for long-running work.
5. Release lock.
6. Close client.

## Quick start (cluster mode)

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/leyle/dbandpubsub/redislockclient"
)

func main() {
	cfg := &redislockclient.RedisClientOption{
		HostPorts: []string{
			"redis.dev.test:6379",
			"redis.dev.test:6380",
			"redis.dev.test:6381",
			"redis.dev.test:6382",
			"redis.dev.test:6383",
			"redis.dev.test:6384",
		},
		Password:    "abc123",
		ServiceName: "my-service",
	}

	// For cluster-focused usage, prefer the explicit constructor.
	client, err := redislockclient.NewClusterRedisClient(cfg)
	if err != nil {
		log.Fatalf("create cluster client failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	resource := "order:12345"

	lock, ok := client.AcquireLockObject(ctx, resource, 3*time.Second, 10*time.Second)
	if !ok {
		log.Println("lock busy, skip")
		return
	}
	defer lock.Release(ctx)

	// critical section
	log.Printf("lock acquired resource=%s val=%s", lock.Resource(), lock.Value())
	time.Sleep(500 * time.Millisecond)
}
```

## API notes

### `AcquireLock` / `AcquireLockObject`

- `acquireTimeout`: max wait time while retrying lock acquisition.
- `lockTimeout`: TTL for the lock key.
- If timeout values are `<= 0`, defaults are used.

Use `AcquireLockObject` for cleaner lifecycle handling:

```go
lock, ok := client.AcquireLockObject(ctx, resource, 5*time.Second, 15*time.Second)
if !ok {
	return
}
defer lock.Release(ctx)
```

### `ExtendLock` / `lock.Extend`

Use this for long-running tasks so the lock does not expire mid-task.

```go
if !lock.Extend(ctx, 10*time.Second) {
	// lock expired or ownership lost
	return
}
```

### `ReleaseLock` / `lock.Release`

- Release is **atomic** (compare-and-delete via Lua), so one client cannot delete another owner's lock by race.
- By design, it returns `true` when lock is already expired or owned by another client (idempotent cleanup behavior).
- Returns `false` on Redis execution errors.

### `IsLockHeld` / `lock.IsHeld`

Checks if the lock key exists and value matches your lock value.

### `GetLockTTL` / `lock.TTL`

Returns remaining TTL as `time.Duration`.
Special values:
- `-2`: key does not exist
- `-1`: key exists but no TTL

## Context behavior

Lock operations respect the provided context:
- canceled/deadline-exceeded contexts can cause early return
- `AcquireLock` may return before `acquireTimeout` if `ctx` is canceled

If you want acquisition to rely only on `acquireTimeout`, use a non-canceled context for that call.

## Constructor selection

You can also use the generic constructor:

```go
client, err := redislockclient.NewRedisLockClient(cfg)
```

Selection rules:
- `len(HostPorts) == 1` => singleton
- `MasterName != ""` => sentinel
- otherwise => cluster

For cluster-first deployments, `NewClusterRedisClient` avoids accidental mode mismatch.

## Recommended production pattern

1. Use `AcquireLockObject`.
2. Defer `lock.Release(ctx)`.
3. For tasks near TTL duration, run periodic renewal using `lock.Extend`.
4. Check `lock.IsHeld(ctx)` at critical checkpoints in very long workflows.

## Troubleshooting

### `ReleaseLock` returns `false`

Likely Redis execution error. Common cause in locked-down environments:
- Redis ACL does not allow `EVAL`

`ReleaseLock` uses an atomic Lua compare-and-delete for safety.  
Ensure your Redis user has permission for scripting commands used by this library.

### Lock acquisition fails too often

Check:
1. `acquireTimeout` is too short for current contention.
2. `lockTimeout` is too short for task duration (lock expires before task ends).
3. Resource key is too coarse (too many workflows contending for one key).

Tuning tips:
1. Increase `acquireTimeout` for bursty contention.
2. Increase `lockTimeout` or add periodic `lock.Extend(...)`.
3. Use finer-grained resource keys.

### Acquire returns early before `acquireTimeout`

`AcquireLock` respects context cancellation/deadlines.  
If `ctx` is canceled, acquisition exits early by design.

Use a context with sufficient lifetime for lock acquisition if needed.

### Unexpected mode selected with `NewRedisLockClient`

Mode selection is:
1. `len(HostPorts) == 1` => singleton
2. `MasterName != ""` => sentinel
3. otherwise => cluster

For cluster-only deployments, call `NewClusterRedisClient(cfg)` directly.

### TTL special values look strange

`GetLockTTL` / `lock.TTL` special values:
1. `-2` => key does not exist
2. `-1` => key exists but no TTL

Depending on formatting, these may appear as negative nanoseconds when printed as `time.Duration`.
