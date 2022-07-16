#!/bin/bash

. ./env.sh

KEY_FILE=$PWD/keyfile/dev.key
if [ ! -f $KEY_FILE ]; then
    echo "No $KEY_FILE"
    exit 1
fi

DEBUG=$1
if [ -z $DEBUG ]; then
    docker-compose -f mongodb.yaml up -d
else
    docker-compose -f mongodb.yaml up
fi
