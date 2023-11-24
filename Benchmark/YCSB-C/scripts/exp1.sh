#!/bin/bash

source scripts/common.sh

#### 1. RMW-intensive workloads 

readRatios=(1) 

## RocksDB, RocksDB+KDS, BlobDB, BlobDB+KDS, vLogDB, vLogDB+KDS
ExpName="_exp1_rmw"
runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
func
