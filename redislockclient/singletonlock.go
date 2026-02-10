package redislockclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Lua script for atomic lock extension: check if value matches, if so extend TTL
const extendLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`

// Lua script for atomic unlock: only delete when lock value matches.
const releaseLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`

type SingletonRedisClient struct {
	cfg *RedisClientOption
	*redis.Client
}

func NewSingletonRedisClient(cfg *RedisClientOption) (*SingletonRedisClient, error) {
	// check cfg, todo
	rcs := &SingletonRedisClient{
		cfg: cfg,
	}

	opt := &redis.Options{
		Addr:     cfg.HostPorts[0],
		Password: cfg.Password,
		DB:       cfg.DbNO,
	}

	client := redis.NewClient(opt)

	// test redis connection
	key := rcs.GenerateRedisKey(moduleName, "test-redis")
	val := "test-redis-connection"

	ctx := context.Background()
	err := client.Set(ctx, key, val, 0).Err()
	if err != nil {
		return nil, err
	}

	dbVal, err := client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if dbVal != val {
		return nil, fmt.Errorf("val[%s] != dbVal[%s], fatal error occurred", val, dbVal)
	}

	err = client.Del(ctx, key).Err()
	if err != nil {
		return nil, err
	}

	rcs.Client = client
	return rcs, nil
}

func (r *SingletonRedisClient) AcquireLock(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (string, bool) {
	logger := zerolog.Ctx(ctx)

	if acquireTimeout <= 0 {
		acquireTimeout = defaultAcquireTimeout
	}
	if lockTimeout <= 0 {
		lockTimeout = defaultLockKeyTimout
	}

	val := objectid.GetObjectId()
	endTime := time.Now().Add(acquireTimeout)

	redisKey := r.GenerateRedisKey(moduleName, resource)

	for time.Now().UnixMilli() < endTime.UnixMilli() {
		select {
		case <-ctx.Done():
			logger.Warn().Err(ctx.Err()).Str("resource", resource).Msg("acquire lock canceled by context")
			return "", false
		default:
		}

		ok, err := r.SetNX(ctx, redisKey, val, lockTimeout).Result()
		if err != nil {
			logger.Error().Err(err).Str("resource", resource).Msg("try to set redis lock(SetNX) failed")
			return "", false
		}
		if ok {
			logger.Info().Str("resource", resource).Msgf("set redis lock succeeded, lock val is:[%s]", val)
			return val, true
		} else {
			// retry with 10 millisecond
			select {
			case <-ctx.Done():
				logger.Warn().Err(ctx.Err()).Str("resource", resource).Msg("acquire lock canceled while waiting retry")
				return "", false
			case <-time.After(defaultRetryDuration):
			}
			continue
		}
	}

	logger.Error().Str("resource", resource).Msgf("with [%s] time period, try to get lock failed", acquireTimeout.String())
	return "", false
}

// AcquireLockObject acquires a lock and returns a Lock object for easier management.
func (r *SingletonRedisClient) AcquireLockObject(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (*Lock, bool) {
	val, ok := r.AcquireLock(ctx, resource, acquireTimeout, lockTimeout)
	if !ok {
		return nil, false
	}
	return NewLock(r, resource, val), true
}

// ExtendLock extends the TTL of an existing lock if the caller still owns it.
// Uses a Lua script for atomic check-and-extend operation.
func (r *SingletonRedisClient) ExtendLock(ctx context.Context, resource, val string, extendDuration time.Duration) bool {
	logger := zerolog.Ctx(ctx)
	redisKey := r.GenerateRedisKey(moduleName, resource)

	result, err := r.Eval(ctx, extendLockScript, []string{redisKey}, val, extendDuration.Milliseconds()).Int()
	if err != nil {
		logger.Error().Err(err).Str("resource", resource).Msg("extend lock failed")
		return false
	}

	if result == 1 {
		logger.Debug().Str("resource", resource).Dur("extendBy", extendDuration).Msg("lock extended successfully")
		return true
	}

	logger.Warn().Str("resource", resource).Msg("extend lock failed - lock not owned or expired")
	return false
}

// IsLockHeld checks if the lock is currently held by the caller with the specified value.
func (r *SingletonRedisClient) IsLockHeld(ctx context.Context, resource, val string) bool {
	logger := zerolog.Ctx(ctx)
	redisKey := r.GenerateRedisKey(moduleName, resource)

	v, err := r.Get(ctx, redisKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			logger.Debug().Str("resource", resource).Msg("lock key does not exist")
			return false
		}
		logger.Error().Err(err).Str("resource", resource).Msg("check lock held failed")
		return false
	}

	return v == val
}

// GetLockTTL returns the remaining TTL of a lock.
// Returns -2 if the key doesn't exist, -1 if no TTL is set.
func (r *SingletonRedisClient) GetLockTTL(ctx context.Context, resource string) time.Duration {
	logger := zerolog.Ctx(ctx)
	redisKey := r.GenerateRedisKey(moduleName, resource)

	ttl, err := r.PTTL(ctx, redisKey).Result()
	if err != nil {
		logger.Error().Err(err).Str("resource", resource).Msg("get lock TTL failed")
		return -1
	}

	return ttl
}

func (r *SingletonRedisClient) ReleaseLock(ctx context.Context, resource, val string) bool {
	logger := zerolog.Ctx(ctx)

	redisKey := r.GenerateRedisKey(moduleName, resource)

	result, err := r.Eval(ctx, releaseLockScript, []string{redisKey}, val).Int()
	if err != nil {
		logger.Error().Err(err).Str("resource", resource).Str("val", val).Msg("release redis lock failed (atomic eval)")
		return false
	}

	if result == 1 {
		logger.Debug().Str("resource", resource).Str("val", val).Msg("delete lock key, release lock succeed")
		return true
	}

	logger.Warn().Str("resource", resource).Str("val", val).Msg("release skipped: lock already expired or owned by another client")
	return true
}

func (r *SingletonRedisClient) Close() error {
	return r.Client.Close()
}

func (r *SingletonRedisClient) GenerateRedisKey(moduleName, userKey string) string {
	// SERVICE:MODULE:USER_KEY
	// module name shouldn't have ":"
	moduleName = strings.ReplaceAll(moduleName, ":", "")
	moduleName = strings.ToUpper(moduleName)
	userKey = strings.ToUpper(userKey)
	return fmt.Sprintf("%s:%s:%s", r.cfg.ServiceName, moduleName, userKey)
}
