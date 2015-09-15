#!/usr/bin/env bash
eval "$(docker-machine env dev)"

# docker run --rm -i -t -p 9200:9200 -p 9300:9300 -h dev -v "$PWD/config":/usr/share/elasticsearch/config elasticsearch

docker run --rm -i -t\
    -h dev\
    -p 9200:9200\
    -p 9300:9300\
    elasticsearch\
    elasticsearch\
    -Des.cluster.name=elasticsearch\
    -Des.index.number_of_shards=1\
    -Des.index.number_of_replicas=0\
    -Des.network.bind_host=dev\
    -Des.network.publish_host=dev\
    -Des.cluster.routing.allocation.disk.threshold_enabled=false\
    -Des.action.disable_delete_all_indices=true\
    -Des.multicast.enabled=false\
    -Des.transport.publish_host=dev\
    -Des.discovery.zen.ping.unicast.hosts=dev\
    -Des.discovery.zen.minimum_master_nodes=1
