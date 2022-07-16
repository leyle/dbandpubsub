#!/bin/bash

. ./env.sh

docker-compose -f mongodb.yaml down -v

sudo rm -rf ./data
