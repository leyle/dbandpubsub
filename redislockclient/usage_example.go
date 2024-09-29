package redislockclient

import (
	"context"
)

// usage of this library

// 1. create a config struct

type SampleConfig struct {
	HostPorts   []string `yaml:"hostPorts"`
	Password    string   `yaml:"password"`
	ServiceName string   `yaml:"serviceName"`
}

// 2. load yaml configuration file to struct
// viper <-

// 3. create redis client instance
// call NewRedisLockClient()
func getRedisClient(cfg *SampleConfig) (RedisLockClient, error) {
	redisCfg := &RedisClientOption{
		HostPorts:   cfg.HostPorts,
		Password:    cfg.Password,
		DbNO:        0,
		ServiceName: cfg.ServiceName,
	}

	client, err := NewRedisLockClient(redisCfg)
	if err != nil {
		// process the error, todo
		panic(err)
	}
	return client, nil
}

// 4. lock/release critical value
func lockReleaseUsage(cfg *SampleConfig) error {
	client, err := getRedisClient(cfg)
	if err != nil {
		panic(err)
	}
	lockVal, ok := client.AcquireLock(context.Background(), "resource", -1, -1)
	if !ok {
		// todo
	}
	defer client.ReleaseLock(context.Background(), "resource", lockVal)
	// do things

	// redis client get/set/del ...
	_, _ = client.Set(context.Background(), "key", "val", 5).Result()
	_ = client.Del(context.Background(), "key").Err()

	// todo

	return nil
}
