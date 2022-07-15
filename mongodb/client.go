package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type MgoClient struct {
	op     *MgoOption
	client *mongo.Client
}

func NewMgoClient(op *MgoOption) *MgoClient {
	// it's safe to use in concurrent goroutines
	// check options
	if op.ReadTimeout < defaultReadTimeout {
		op.ReadTimeout = defaultReadTimeout
	}
	if op.WriteTimeout < defaultWriteTimeout {
		op.WriteTimeout = defaultWriteTimeout
	}

	fmt.Println("NewMgoClient, options are: ", op)
	fmt.Println("read timeout is: ", op.ReadTimeout)
	fmt.Println("write timeout is: ", op.WriteTimeout)
	fmt.Println("minPoolSize:", *defaultClientPoolSizeOption.MinPoolSize, "maxPoolSize:", *defaultClientPoolSizeOption.MaxPoolSize)

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

	ds := &MgoClient{
		op:     op,
		client: client,
	}
	return ds
}

func (ds *MgoClient) C(collection string) *mongo.Collection {
	return ds.client.Database(ds.op.Database).Collection(collection)
}

func (ds *MgoClient) Close() {
	ds.client.Disconnect(context.TODO())
}

func (ds *MgoClient) ReadContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Duration(ds.op.ReadTimeout)*time.Second)
	return ctx
}

func (ds *MgoClient) WriteContext() context.Context {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Duration(ds.op.WriteTimeout)*time.Second)
	return ctx
}
