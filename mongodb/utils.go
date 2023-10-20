package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/fs"
	"os"
	"syscall"
)

// we need to pass configurations to this library, then we can write a function to get mongo client

// sample mongodb configuration

/* yaml file configuration

mongodb:
  replica: true
  replicaSet: "devRepl"
  hostPorts:
    - 127.0.0.1:27017
    - 127.0.0.1:27018
    - 127.0.0.1:27019
  username: "dbuser"
  password: "dbpasswd"
  database: "dev"
  connOption: "maxPoolSize=20&w=majority"
  writeTimeout: 120 # seconds
  readTimeout: 60 # seconds
  tls:
    enabled: false
    pem: "/path/to/pem/file"

*/

type MongodbConf struct {
	Replica      bool     `yaml:"replica"`
	ReplicaSet   string   `yaml:"replicaSet"`
	HostPorts    []string `yaml:"hostPorts"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	Database     string   `yaml:"database"`
	ConnOption   string   `yaml:"connOption"`
	WriteTimeout int      `yaml:"writeTimeout"`
	ReadTimeout  int      `yaml:"readTimeout"`
	TLS          struct {
		Enabled bool   `yaml:"enabled"`
		PEM     string `yaml:"pem"`
	} `yaml:"tls"`
}

var (
	MgoMinPoolSize uint64 = 20
	MgoMaxPoolSize uint64 = 200
)

func GetMongodbClient(cfg *MongodbConf) *DataSource {
	op := &MgoOption{
		HostPorts:    cfg.HostPorts,
		Username:     cfg.Username,
		Password:     cfg.Password,
		Database:     cfg.Database,
		ConnOption:   cfg.ConnOption,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	moreOptions := &options.ClientOptions{
		MinPoolSize: &MgoMinPoolSize,
		MaxPoolSize: &MgoMaxPoolSize,
	}

	// override the default replicaset name
	if cfg.Replica && cfg.ReplicaSet != "" {
		moreOptions.ReplicaSet = &cfg.ReplicaSet
	}

	if cfg.TLS.Enabled {
		// check file is valid
		err := checkPathExist(cfg.TLS.PEM, 4)
		if err != nil {
			fmt.Println("invalid mongodb tls pem file: ", cfg.TLS.PEM, err.Error())
			os.Exit(1)
		}
		tlsCfg, err := setMongodbTLSCfg(cfg.TLS.PEM)
		if err != nil {
			fmt.Println("load pem file failed: ", cfg.TLS.PEM, err.Error())
			os.Exit(1)
		}

		moreOptions.SetTLSConfig(tlsCfg)
	}

	op.MoreOptions = moreOptions

	ds := NewDataSource(op)

	return ds
}

func CreateNewDatabase() {

}

func setMongodbTLSCfg(pem string) (*tls.Config, error) {
	// Load the PEM file
	pemBytes, err := os.ReadFile(pem)
	if err != nil {
		return nil, err
	}

	// Create a CertPool to hold the PEM file's contents
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(pemBytes); !ok {
		return nil, errors.New("load pem file failed")
	}

	// Create a new TLS Config instance with the CertPool
	tlsConfig := &tls.Config{
		RootCAs: roots,
	}

	return tlsConfig, nil
}

func checkPathExist(path string, permission int) error {
	// minPermission:
	// 4 -> only check if it can read
	// 4 + 2 = 6 -> check if it can read and write

	// first check if exist
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// if os.IsNotExist(err) {
			// todo
		} else {
		}
		return err
	}

	// then check if it can read or read/write
	var bit uint32 = syscall.O_RDWR
	if permission < 6 {
		bit = syscall.O_RDONLY
	}

	err := syscall.Access(path, bit)
	if err != nil {
		return err
	}
	return nil
}
