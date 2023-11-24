#!/bin/bash

source scripts/common.sh

#### 1. RMW-intensive workloads 

ExpName="_exp1_rmw"
readRatios=(1) 

## RocksDB, RocksDB+KDS, BlobDB, BlobDB+KDS, vLogDB, vLogDB+KDS
runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
func
