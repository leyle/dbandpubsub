#!/bin/bash
#
. ./env.sh

# docker run --rm -it --network host redis:$VERSION redis-cli -c -h $IP -p 6379

docker exec -it redis-cli redis-cli -c -h $IP -p 6379 -a $REDIS_PASSWORD
