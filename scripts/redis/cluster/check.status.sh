#!/bin/bash
#
. ./env.sh

# Function to check cluster status

# Function to check if Redis cluster is working
check_cluster() {
    echo "Checking Redis cluster..."
    docker exec redis-cli redis-cli -h $IP -p $PORT1 cluster info | grep "cluster_state:ok"
    if [ $? -eq 0 ]; then
        echo "Redis cluster is working correctly."
        return 0
    else
        echo "Redis cluster is not working correctly."
        return 1
    fi
}

check_cluster_status() {
    echo "Checking cluster status..."
    
    echo "Cluster Info:"
    docker exec redis-cli redis-cli -h $IP -p $PORT1 cluster info
    
    echo -e "\nCluster Nodes:"
    docker exec redis-cli redis-cli -h $IP -p $PORT1 cluster nodes
    
    echo -e "\nCluster Health:"
    docker exec redis-cli redis-cli --cluster check $IP:$PORT1
}

check_cluster

if [ $? -eq 0 ]; then
    echo "Redis cluster setup complete and working correctly."

    # Check cluster status
    check_cluster_status
fi


