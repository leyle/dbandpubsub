# mongodb 安装配置

## replica set 配置

在配置 replica set 模式之前，需要知道的几个问题

- 节点数使用 >=3 的奇数个节点，比如 3 个、5 个等，因为使用的写策略是 majority
- 当节点数不够 majority 时，默认情况下会出现写入数据失败的情况（不影响读数据），所以
  - 如果搭建了 3 个节点，挂掉 1 个，不影响写入，挂掉 2 个，写入会失败
  - 如果搭建了 4/5 个节点，最低 majority 数是 3，所以可以挂掉 1/2 个节点
  - 如果搭建了 6/7，最低 majority 是 4 个，可以挂掉 2/3 个节点
- 各个节点间，使用同一个 keyfile 进行认证，这个 keyfile 也就是个包含了几百个 base64 字符集的字符串的文件



基于 mongodb 5.0 编写的操作记录

```shell
# ref https://www.mongodb.com/docs/manual/administration/replica-set-deployment/
# ref https://www.mongodb.com/docs/manual/core/replica-set-architectures/
```

主要步骤为

1. 生成一个足够长的  keyfile，存储随机的 base64 字符集字符串 ，作为各个节点间能够互相联通的依据。
2. 如果使用了 docker-compose，上述 keyfile 需要注意想办法修改为 400 的权限。
3. 在启动参数中指定 `--replSet $NAME`值，各个 node 需要都使用相同的 name 和 keyfile
4. 启动各个节点
5. 进入某一个节点，通过 local 进入链接，在 mongodb 5.0 中，已使用 `mongosh` 来作为默认的 client shell 使用
6. 创建 replica set 的配置文件（此步骤之前，可能会需要先进行 admin 权限验证）
7. 验证 replica set 配置是否正确

下面通过 shell 命令及部分文件来详细说明如何配置有一个 primary node，两个 secondary nodes 的文档。

**⚠️注：此处以 mongodb 的 docker image 及 docker-compose 文件作为说明，其他使用方式，逻辑一致，细节可能需要调整**

```shell
# 生成一个长度在 6-1024 个字符，足够随机，base64 字符集的 key file
# 可以用 openssl 直接生成，根据官网文档
openssl rand -base64 756 > replica.key

# 将此文件分发给所有的节点使用，所有的节点的 --keyFile 参数的值都是这个相同的 replica.key

# 调整 mongodb 的 docker-compose 文件，能够加载此 replica.key
# 需要特别注意的是，mongodb 读取此文件时，要求此文件的拥有者与 mongodb 运行进程的 userid 一致，同时 rwx 是 400
# 下面附带一个 docker compose 文件例子

# 通过下述的 start.sh 即可启动 mongodb server
# 依次在各个机器上启动各自的 mongodb server，此处以 3 个节点为例
# 三个节点的地址分别为
# mgo0.fabric.emali.dev:27017
# mgo1.fabric.emali.dev:27017
# mgo2.fabric.emali.dev:27017

# 当三个 mongodb server 都启动完毕后，进入其中任何一个 container 里面
# 执行 replica set 的相关配置

# 首先启动 mongodb shell
mongosh

# 切换到 admin database，并进行验证（admin 账户在启动时，通过环境变量提供的）
use admin
db.auth('rootuser', 'rootpasswd')

# 创建 rs 配置
# _id 是 replica set 的 name
# host 是此 set 中各个节点的信息，根据实际情况调整填写
rs.initiate(
    {
        _id : "devRepl",
        members: [
          { _id : 0, host : "mgo0.fabric.emali.dev:27017" },
          { _id : 1, host : "mgo1.fabric.emali.dev:27017" },
          { _id : 2, host : "mgo2.fabric.emali.dev:27017" }
        ]
  }
)

# 确认是否创建成功
# 会返回一些基本信息，包含了 members、写入策略、replicaSetId 等信息
rs.conf()

# 检查是否有 primary node
# 查看返回的信息
rs.status()

# 到此位置，基本上就把 replica set 搭建完毕

# 下面可以退出 admin 账户，使用之前启动时附带的 db-init.js 里面的账户来创建一些测试数据看看

# 比如
use dev
db.auth("dbuser", "dbpasswd")
db.test.insertOne({"name": "initialTest", "version": 1})
db.test.find({"name": "initialTest"})

# 检查是否数据同步到了 secondary nodes 上
# 默认情况下，可以看到数据库及表，但是无法查询
# 如果要查询，需要做一下配置，比如
db.getMongo().setReadPref('secondaryPreferred')

# 下面附上主要的配置文件
```



**start.sh 例子**

```shell
#!/bin/bash

export TAG=5.0
export NETWORK=mongodb-dev
export TIMEZONE=Asia/Shanghai
export ROOTUSER=rootuser
export ROOTPASSWD=rootpasswd
export PORT=27017
export CONTAINER=mongodb-$PORT

export REPLICA_SET_NAME=devRepl

export DBUSER=dbuser
export DBPASSWD=dbpasswd
export DBDEV=dev

DEBUG=$1
if [ -z $DEBUG ]; then
    docker-compose -f mongodb.yaml up -d
else
    docker-compose -f mongodb.yaml up
fi
```



**mongodb.yaml docker compose yaml 例子**

这个例子中，需要注意两点

- command 命令及其参数样子
- entrypoint 中，通过一个中转文件，把最终的 replica.key 文件的权限修改了

```yaml
version: '3.6'

networks:
  mongodb-nt:
    name: $NETWORK

services:
  mongo:
    image: mongo:$TAG
    command: "mongod --replSet $REPLICA_SET_NAME --keyFile /data/replica.key"
    restart: always
    container_name: $CONTAINER
    environment:
      - TZ=$TIMEZONE
      - MONGO_INITDB_ROOT_USERNAME=$ROOTUSER
      - MONGO_INITDB_ROOT_PASSWORD=$ROOTPASSWD
      - MONGO_INITDB_DATABASE=$DBDEV
    volumes:
      - $PWD/data:/data/db
      - ./db-init.js:/docker-entrypoint-initdb.d/mongo-init.js:ro
      - $PWD/keyfile/dev.key:/data/replica.key.tmp
    entrypoint:
        - bash
        - -c
        - |
            cp /data/replica.key.tmp /data/replica.key
            chmod 400 /data/replica.key
            chown 999:999 /data/replica.key
            exec docker-entrypoint.sh $$@
    networks:
      - mongodb-nt
    ports:
      - $PORT:27017
```

**db-init.js 例子**

```js
db.createUser(
  {
    user: "dbuser",
    pwd: "dbpasswd",
    roles: [ { role: "readWrite", db: "dev" }],
    passwordDigestor: "server"
  }
);

db.createUser(
  {
    user: "readonly",
    pwd: "readpasswd",
    roles: [ { role: "read", db: "dev" }],
    passwordDigestor: "server"
  }
);
```



---

## 一些参数说明

### oplog

```shell
# oplog
# oplog 用来记录数据库发生的变化的日志
# ref https://www.mongodb.com/docs/manual/core/replica-set-oplog/
# 默认值是 5% 的硬盘空闲空间，最低值是 990MB

```



---

## auth 安全性配置与用户创建

1. 首先以普通无认证的方式启动mongodb，然后创建一个用户，这个用户的roie是 `userAdminAnyDatabase`  或 `userAdmin`，下面是一个创建例子。

   ```shell
   use admin
   db.createUser(
     {
       user: "myUserAdmin",
       pwd: "abc123",
       roles: [ { role: "userAdminAnyDatabase", db: "admin" }, "readWriteAnyDatabase" ]
     }
   )	
   ```

   然后关闭mongodb，修改配置文件，改为认证启用，再启动mongodb

   

2. 用帐号，密码的方式重新进入shell

   ```shell
   mongo --port 27017 -u "myUserAdmin" -p "abc123" --authenticationDatabase "admin"	
   ```

   当然也可以先进入shell，在进行认证

   ```shell
   mongo --port 27017
   use admin
   db.auth("myUserAdmin", "abc123" )		
   ```

3. 按需创建用户，赋给相应的数据库权限等操作

   ```shell
   # 先切换到想要创建用户的数据库，然后再用 createUser 来创建用户
   # 用户的 role 主要包含 read 或者 readWrite 权限
   use test
   db.createUser(
     {
       user: "myTester",
       pwd: "xyz123",
       roles: [ { role: "readWrite", db: "test" }],
       passwordDigestor: "server"
     }
   )
   
   ```

