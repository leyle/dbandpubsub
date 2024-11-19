#!/bin/bash
#

. ./env.sh

case "$1" in
  up)
    echo "Starting Redis cluster with master, slave, and sentinels..."
    docker-compose -f nodes.yaml up 
    ;;
  down)
    echo "Stopping Redis cluster..."
    docker-compose -f nodes.yaml down -v
    ;;
  *)
    echo "Usage: $0 {up|down}"
    exit 1
    ;;
esac
