package mongodb

import (
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

// connection uri ref: https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-formats
// replica set deployment ref:
// https://www.mongodb.com/docs/manual/administration/replica-set-deployment/
// https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/

// standalone
// mongodb://mongodb0.example.com:27017
// mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017/?authSource=admin

// replica set
// mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017,mongodb2.example.com:27017/?replicaSet=myRepl
// mongodb://myDBReader:D1fficultP%40ssw0rd@mongodb0.example.com:27017,mongodb1.example.com:27017,mongodb2.example.com:27017/?authSource=admin&replicaSet=myRepl

// shared cluster
// basically, we don't use shared cluster mode.
// mongodb://mongos0.example.com:27017,mongos1.example.com:27017,mongos2.example.com:27017
// mongodb://myDBReader:D1fficultP%40ssw0rd@mongos0.example.com:27017,mongos1.example.com:27017,mongos2.example.com:27017/?authSource=admin

// replica set keyfile configuration ref: https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-with-keyfile-access-control/

// transaction ref: https://www.mongodb.com/docs/manual/core/transactions/

const replicaSetName = "devRepl"

const (
	defaultReadTimeout  = 60  // seconds
	defaultWriteTimeout = 120 // seconds
)

var (
	defaultMinPoolSize uint64 = 100
	defaultMaxPoolSize uint64 = 200
)

var defaultClientPoolSizeOption = &options.ClientOptions{
	MaxPoolSize: &defaultMaxPoolSize,
	MinPoolSize: &defaultMinPoolSize,
}

type MgoOption struct {
	HostPorts    []string
	Username     string
	Password     string
	Database     string
	ConnOption   string // e.g. maxPoolSize=20&w=majority
	ReadTimeout  int    // seconds
	WriteTimeout int    // seconds
	MoreOptions  *options.ClientOptions
}

func (op *MgoOption) uri() string {
	conn := "mongodb://"

	if op.Username != "" && op.Password != "" {
		conn = fmt.Sprintf("%s%s:%s@", conn, op.Username, op.Password)
	}
	hostPorts := strings.Join(op.HostPorts, ",")
	if len(op.HostPorts) == 1 {
		conn = fmt.Sprintf("%s%s/?authSource=%s", conn, hostPorts, op.Database)
	} else {
		if len(op.HostPorts) < 3 {
			panic("mongodb runs on replica set mode, it should contains at least 3 members")
		}
		conn = fmt.Sprintf("%s%s/?authSource=%s&replicaSet=%s", conn, hostPorts, op.Database, replicaSetName)
	}

	if op.ConnOption != "" {
		conn = fmt.Sprintf("%s&%s", conn, op.ConnOption)
	}

	return conn
}

func (op *MgoOption) String() string {
	uri := op.uri()
	s := strings.Replace(uri, op.Password, "******", 1)
	return s
}
