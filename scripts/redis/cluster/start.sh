#!/bin/bash

. ./env.sh

docker-compose -f docker-3nodes.yaml up -d 

docker run --rm -it --network host redis:$VERSION redis-cli --cluster create $IP:6379 $IP:6380 $IP:6381 $IP:6382 $IP:6383 $IP:6384 --cluster-replicas 1 --cluster-yes -a $REDIS_PASSWORD
