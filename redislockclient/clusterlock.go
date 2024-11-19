package redislockclient

import (
	"context"
	"errors"
	"fmt"
	"github.com/leyle/crud-objectid/pkg/objectid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"strings"
	"time"
)

type ClusterRedisClient struct {
	cfg *RedisClientOption
	*redis.ClusterClient
}

func NewClusterRedisClient(cfg *RedisClientOption) (*ClusterRedisClient, error) {
	rcc := &ClusterRedisClient{
		cfg: cfg,
	}

	opt := &redis.ClusterOptions{
		Addrs:    cfg.HostPorts,
		Password: cfg.Password,
	}

	client := redis.NewClusterClient(opt)

	// test redis connection
	key := rcc.GenerateRedisKey(moduleName, "test-redis")
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
	rcc.ClusterClient = client
	return rcc, nil
}

func (r *ClusterRedisClient) AcquireLock(ctx context.Context, resource string, acquireTimeout, lockTimeout time.Duration) (string, bool) {
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
	logger.Debug().Str("lockResource", redisKey).Send()

	for time.Now().UnixMilli() < endTime.UnixMilli() {
		ok, err := r.SetNX(context.Background(), redisKey, val, lockTimeout).Result()
		if err != nil {
			logger.Error().Err(err).Str("resource", resource).Msg("try to set redis lock(SetNX) failed")
			return "", false
		}
		if ok {
			logger.Info().Str("resource", resource).Msgf("set redis lock succeeded, lock val is:[%s]", val)
			return val, true
		} else {
			// retry with 10 millisecond
			time.Sleep(defaultRetryDuration)
			continue
		}
	}

	logger.Error().Str("resource", resource).Msgf("with [%s] time period, try to get lock failed", acquireTimeout.String())
	return "", false
}

func (r *ClusterRedisClient) ReleaseLock(ctx context.Context, resource, val string) bool {
	logger := zerolog.Ctx(ctx)

	redisKey := r.GenerateRedisKey(moduleName, resource)

	v, err := r.Get(context.Background(), redisKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		logger.Error().Err(err).Str("resource", resource).Str("val", val).Msg("release redis lock failed")
		return false
	}

	if errors.Is(err, redis.Nil) {
		logger.Debug().Str("resource", resource).Str("val", val).Msg("lock key has expired, release lock succeed")
		return true
	}

	if v == val {
		r.Del(context.Background(), redisKey)
		logger.Debug().Str("resource", resource).Str("val", val).Msg("delete lock key, release lock succeed")
		return true
	} else {
		logger.Warn().Str("resource", resource).Str("val", val).Msg("when try to release lock, but the lock has locked by others, we think this situation is ok ")
		return true
	}
}

func (r *ClusterRedisClient) Close() error {
	return r.ClusterClient.Close()
}

func (r *ClusterRedisClient) GenerateRedisKey(moduleName, userKey string) string {
	// SERVICE:MODULE:USER_KEY
	// module name shouldn't have ":"
	moduleName = strings.ReplaceAll(moduleName, ":", "")
	moduleName = strings.ToUpper(moduleName)
	userKey = strings.ToUpper(userKey)
	return fmt.Sprintf("%s:%s:%s", r.cfg.ServiceName, moduleName, userKey)
}
