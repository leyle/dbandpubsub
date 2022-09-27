package mongodb

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

// index ref: https://www.mongodb.com/docs/manual/indexes/

const (
	sortOrderAscending  = 1
	sortOrderDescending = -1
)

var lock = &sync.RWMutex{}
var singleDs *DataSource

type DataSource struct {
	op     *MgoOption
	client *mongo.Client
	logger *zerolog.Logger
}

func NewDataSource(op *MgoOption) *DataSource {
	if singleDs != nil {
		return singleDs
	}
	lock.Lock()
	defer lock.Unlock()

	if singleDs != nil {
		return singleDs
	}

	// it's safe to use in concurrent goroutines
	// check options
	if op.ReadTimeout < defaultReadTimeout {
		op.ReadTimeout = defaultReadTimeout
	}
	if op.WriteTimeout < defaultWriteTimeout {
		op.WriteTimeout = defaultWriteTimeout
	}

	fmt.Println("NewDataSource, options are: ", op)
	fmt.Println("read timeout is: ", op.ReadTimeout)
	fmt.Println("write timeout is: ", op.WriteTimeout)
	fmt.Println("default minPoolSize:", *defaultClientPoolSizeOption.MinPoolSize, "default maxPoolSize:", *defaultClientPoolSizeOption.MaxPoolSize)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(op.ReadTimeout)*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(op.uri()), defaultClientPoolSizeOption, op.MoreOptions)
	if err != nil {
		panic(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}

	ds := &DataSource{
		op:     op,
		client: client,
	}
	return ds
}

func (ds *DataSource) C(collection string) *mongo.Collection {
	return ds.client.Database(ds.op.Database).Collection(collection)
}

func (ds *DataSource) Client() *mongo.Client {
	return ds.client
}

func (ds *DataSource) Close() {
	ds.client.Disconnect(context.TODO())
}

func (ds *DataSource) ReadContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Duration(ds.op.ReadTimeout)*time.Second)
	return ctx
}

func (ds *DataSource) WriteContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Duration(ds.op.WriteTimeout)*time.Second)
	return ctx
}

func (ds *DataSource) InsureSingleIndexes(collection string, keys []string) error {
	var idxKeys []mongo.IndexModel
	for _, key := range keys {
		idxKey := mongo.IndexModel{
			Keys: bson.M{
				key: sortOrderDescending,
			},
		}
		idxKeys = append(idxKeys, idxKey)
	}

	names, err := ds.C(collection).Indexes().CreateMany(ds.WriteContext(), idxKeys)
	if err != nil {
		log.Error().Err(err).Msg("create mongodb single key index failed")
		return err
	}

	log.Debug().Strs("names", names).Send()
	return nil
}

func (ds *DataSource) InsureUniqueIndexes(collection string, keys []string) error {
	var idxKeys []mongo.IndexModel
	var optTrue = true
	uniqueOpt := &options.IndexOptions{
		Unique: &optTrue,
	}
	for _, key := range keys {
		idxKey := mongo.IndexModel{
			Keys: bson.M{
				key: sortOrderDescending,
			},
			Options: uniqueOpt,
		}
		idxKeys = append(idxKeys, idxKey)
	}

	names, err := ds.C(collection).Indexes().CreateMany(ds.WriteContext(), idxKeys)
	if err != nil {
		log.Error().Err(err).Msg("create mongodb unique index failed")
		return err
	}

	log.Debug().Strs("names", names).Send()
	return nil
}

func (ds *DataSource) InsureCompoundIndex(collection string, keys []string) error {
	var compoundKey bson.D
	for _, key := range keys {
		e := bson.E{
			Key:   key,
			Value: sortOrderAscending,
		}
		compoundKey = append(compoundKey, e)
	}

	idxKey := mongo.IndexModel{
		Keys: compoundKey,
	}

	name, err := ds.C(collection).Indexes().CreateOne(ds.WriteContext(), idxKey)
	if err != nil {
		log.Error().Err(err).Msg("create mongodb compound index failed")
		return err
	}

	log.Debug().Str("names", name).Send()
	return nil
}
