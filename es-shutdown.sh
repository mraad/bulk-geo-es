#!/usr/bin/env bash
curl -XPOST $(docker-machine ip dev):9200/_cluster/nodes/_shutdown
