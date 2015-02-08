#!/bin/bash

apt-get update
apt-get install -y python-pip python-dev libffi-dev libssl-dev
pip install python-etcd
