#!/bin/bash

apt-get install -y docker.io
docker run -d -p 4001:4001 quay.io/coreos/etcd
