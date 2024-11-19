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
	DbNO        int    // redis database number, only work for singleton
	ServiceName string // application service name
	MasterName  string // redis sentinel name, only work for sentinel mode
}
