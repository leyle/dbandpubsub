package redislockclient

import "time"

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
	DbNO        int
	ServiceName string
}
