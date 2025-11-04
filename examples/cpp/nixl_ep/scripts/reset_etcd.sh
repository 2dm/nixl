#!/bin/bash

set +e
pkill -9 -f etcd
pkill -9 -f python
rm -rf default.etcd
HOST_IP=$(hostname -I | awk '{print $1}')
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://${HOST_IP}:2379 &
