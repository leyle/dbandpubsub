#!/bin/bash

source ./env.sh

docker-compose -f redis.yaml up -d
