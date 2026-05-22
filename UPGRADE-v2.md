# Upgrading to dbandpubsub v2

v2 of this library bumps the underlying MongoDB driver from
`go.mongodb.org/mongo-driver` v1 to `go.mongodb.org/mongo-driver/v2`. Because
the driver's public types appear in this library's own signatures (e.g.
`*mongo.Client`, `*mongo.GridFSBucket`, `*options.ClientOptions`), the bump is
a breaking change and the module path is now `github.com/leyle/dbandpubsub/v2`.

This document walks through the upgrade using the
[general-event-server](https://github.com/Emali-Limited/general-event-server)
service as a worked example. The total diff there was 12 files / ~40 lines —
most of it is import paths.

## What changed in v2

Library-level changes are minimal — the public API of `mongodb`,
`kafkaconnector`, and `redislockclient` is essentially unchanged. The breaking
changes you will hit are all from the upstream driver:

| Area | v1 | v2 |
|---|---|---|
| Module path | `github.com/leyle/dbandpubsub` | `github.com/leyle/dbandpubsub/v2` |
| Driver path | `go.mongodb.org/mongo-driver/...` | `go.mongodb.org/mongo-driver/v2/...` |
| ObjectID | `bson/primitive.NewObjectID()` | `bson.NewObjectID()` (the `primitive` package was merged into `bson`) |
| Connect | `mongo.Connect(ctx, opts...)` | `mongo.Connect(opts...)` (no context) |
| Index options | `&options.IndexOptions{Unique: &t}` | `options.Index().SetUnique(true)` |
| Transaction callback | `func(sctx mongo.SessionContext) (interface{}, error)` | `func(ctx context.Context) (any, error)` |
| GridFS package | `go.mongodb.org/mongo-driver/mongo/gridfs` | merged into `mongo` package |
| `gridfs.NewBucket(db, opts)` | `db.GridFSBucket(opts)` (method on `*mongo.Database`) |
| `*gridfs.Bucket` | `*mongo.GridFSBucket` |
| `gridfs.ErrFileNotFound` | `mongo.ErrFileNotFound` |
| Bucket `Open*` / `Delete` methods | now take `context.Context` as the first argument |
| `bson.D` literals | unkeyed literals (`{"updated", -1}`) trigger a `go vet` warning; use `{Key: "updated", Value: -1}` |

## Migration recipe

### 1. Update `go.mod`

```diff
- github.com/leyle/dbandpubsub v0.0.0-20260115031754-020f392f8380
- go.mongodb.org/mongo-driver v1.17.6
+ github.com/leyle/dbandpubsub/v2 v2.0.0
+ go.mongodb.org/mongo-driver/v2 v2.6.0
```

If you are upgrading before a v2 tag has been published, point at the local
checkout with a `replace` directive:

```
replace github.com/leyle/dbandpubsub/v2 => ../../leyle/dbandpubsub
```

Then run `go mod tidy`. It will drop `golang/snappy` and `montanaflynn/stats`
from indirect deps — the v2 driver no longer pulls them in.

### 2. Rename imports

These six rewrites cover almost everything:

| Old | New |
|---|---|
| `github.com/leyle/dbandpubsub/kafkaconnector` | `github.com/leyle/dbandpubsub/v2/kafkaconnector` |
| `github.com/leyle/dbandpubsub/mongodb` | `github.com/leyle/dbandpubsub/v2/mongodb` |
| `github.com/leyle/dbandpubsub/redislockclient` | `github.com/leyle/dbandpubsub/v2/redislockclient` |
| `go.mongodb.org/mongo-driver/bson` | `go.mongodb.org/mongo-driver/v2/bson` |
| `go.mongodb.org/mongo-driver/mongo` | `go.mongodb.org/mongo-driver/v2/mongo` |
| `go.mongodb.org/mongo-driver/mongo/options` | `go.mongodb.org/mongo-driver/v2/mongo/options` |

If you import `bson/primitive` or `mongo/gridfs` directly, those packages no
longer exist — see steps 3 and 5 below.

In general-event-server this covered 8 of the 12 changed files: `cmd/event-server/{main,mqutils,dbutils}.go`,
`configandcontext/context.go`, `pkg/eventapp/{handler,dbutils}.go`,
`pkg/apikeyapp/{apikey,handler}.go`. None of them needed any further code
changes — the symbols actually used (`bson.M`, `bson.D`, `mongo.ErrNoDocuments`,
`options.Find().SetSort(...)`, `&options.ClientOptions{...}`, `Collection.Find/InsertOne/UpdateByID/DeleteOne`)
all keep working.

### 3. Replace `primitive.NewObjectID()` and friends

The `bson/primitive` package was folded into `bson`. Common rewrites:

```diff
- import "go.mongodb.org/mongo-driver/bson/primitive"
+ import "go.mongodb.org/mongo-driver/v2/bson"

- id := primitive.NewObjectID()
+ id := bson.NewObjectID()
```

`primitive.ObjectID` becomes `bson.ObjectID`; `primitive.E` / `primitive.D` /
`primitive.M` become `bson.E` / `bson.D` / `bson.M`. general-event-server didn't
use this package directly so no rewrites were needed.

### 4. `mongo.Connect` no longer takes a context

```diff
- client, err := mongo.Connect(ctx, opts)
+ client, err := mongo.Connect(opts)
```

If you were using the same context for `Connect` and a follow-up `Ping`, keep
the context around for `Ping` — only the `Connect` call dropped its context
argument.

### 5. GridFS migration (`pkg/fileapp` in general-event-server)

This is the only place general-event-server needed real code changes. The
`mongo/gridfs` package was deleted; bucket construction moved to
`*mongo.Database`, and every bucket method now takes a context.

```diff
 import (
-    "go.mongodb.org/mongo-driver/mongo/gridfs"
-    "go.mongodb.org/mongo-driver/mongo/options"
+    "go.mongodb.org/mongo-driver/v2/mongo"
+    "go.mongodb.org/mongo-driver/v2/mongo/options"
 )

-func getGridFSBucket(ctx *configandcontext.APIContext, prefix string) (*gridfs.Bucket, error) {
+func getGridFSBucket(ctx *configandcontext.APIContext, prefix string) (*mongo.GridFSBucket, error) {
     db := ctx.Ds.Client().Database(ctx.Cfg.Mongodb.Database)
-    opts := options.GridFSBucket()
-    opts.SetName(prefix)
-    bucket, err := gridfs.NewBucket(db, opts)
-    return bucket, err
+    bucket := db.GridFSBucket(options.GridFSBucket().SetName(prefix))
+    return bucket, nil
 }
```

`*mongo.Database.GridFSBucket` does not return an error in v2 — you can drop
the second return value entirely if you prefer.

Each upload/download/delete call now takes a context. In general-event-server
we threaded the existing `Ds.ReadContext()` / `Ds.WriteContext()` helpers:

```diff
- stream, err := bucket.OpenUploadStreamWithID(id, name, uploadOpts)
+ stream, err := bucket.OpenUploadStreamWithID(ctx.Ds.WriteContext(), id, name, uploadOpts)

- stream, err := bucket.OpenDownloadStream(id)
+ stream, err := bucket.OpenDownloadStream(ctx.Ds.ReadContext(), id)

- stream, err := bucket.OpenDownloadStreamByName(filename)
+ stream, err := bucket.OpenDownloadStreamByName(ctx.Ds.ReadContext(), filename)

- err = bucket.Delete(fileID)
+ err = bucket.Delete(ctx.Ds.WriteContext(), fileID)
```

The `gridfs.ErrFileNotFound` sentinel moved to the `mongo` package:

```diff
- if err == gridfs.ErrFileNotFound {
+ if errors.Is(err, mongo.ErrFileNotFound) {
```

`stream.GetFile().Metadata` is still `bson.Raw`, so `bson.Unmarshal(md, &metadata)`
keeps working unchanged.

> ⚠️ Watch out for write timeouts. If your service uploads large files via
> GridFS, the new ctx parameter means uploads now run under whatever deadline
> you pass in. In general-event-server we use `Ds.WriteContext()`, whose
> timeout comes from `cfg.WriteTimeout` — verify this is generous enough for
> your largest expected upload, or thread through a longer-lived context for
> the upload path specifically.

### 6. Transaction callback signature

If you call `Session.WithTransaction`, update the callback signature:

```diff
- callback := func(sctx mongo.SessionContext) (interface{}, error) {
+ callback := func(ctx context.Context) (any, error) {
     ...
 }
```

In v2 the session is attached to the context that the driver passes in, so
inside the callback you use `ctx` for any operations that should participate
in the transaction. general-event-server doesn't use transactions; this is
listed for completeness.

### 7. Keep your `bson.D` literals keyed

`go vet` in modern Go versions warns on unkeyed `bson.E` literals. While not
a v2 requirement, the lint surfaced once during the migration:

```diff
- sort := bson.D{
-     {"updated", ginsetup.SortDesc},
- }
+ sort := bson.D{
+     {Key: "updated", Value: ginsetup.SortDesc},
+ }
```

## Verifying the upgrade

```bash
go mod tidy
go build ./...
go vet ./...
```

For services that exercise GridFS, MongoDB transactions, or unusual BSON
encoding, run integration tests against a real MongoDB instance — the v2
driver tightens some encoding behaviors and the build alone won't catch
runtime regressions there.

## Rough effort estimate

For a service that looks roughly like general-event-server (≈ a dozen files
touching this library and the driver), expect:

- ~10–15 minutes for steps 1–4 (mostly mechanical, IDE-driven rename)
- ~30 minutes per GridFS-using package (step 5)
- Plus integration-test time

If your service doesn't touch GridFS, transactions, or `primitive.*` types,
the upgrade is essentially a find-and-replace plus `go mod tidy`.
