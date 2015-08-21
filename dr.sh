#!/usr/bin/env bash
eval "$(docker-machine env dev)"
docker run --rm -i -t -p 9200:9200 -p 9300:9300 -h dev -v "$PWD/config":/usr/share/elasticsearch/config elasticsearch
