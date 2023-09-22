#!/bin/bash

POOL_NAME=$1

sudo ceph tell mon.\* injectargs '--mon-allow-pool-delete=true'
sudo ceph osd pool rm $POOL_NAME $POOL_NAME --yes-i-really-really-mean-it