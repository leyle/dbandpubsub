package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type DataSource struct {
	op     *MgoOption
	client *mongo.Client
}

func NewDataSource(op *MgoOption) *DataSource {
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
