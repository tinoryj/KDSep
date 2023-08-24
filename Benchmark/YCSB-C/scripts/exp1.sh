#!/bin/bash

source scripts/common.sh
works=8
rounds=1
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
bucketNumber=2048
kdcacheSize=512
fcl=10
flength=100
req="100M"
req="15M"
op="100M"

#### 1. RMW-intensive workloads 

bonus="ec"
readRatios=(1) 

## RocksDB, BlobDB, vLogDB
ExpName="_exp2"
runModeSet=('raw' 'bkv' 'kv')
runModeSet=('bkvkd')
func
exit

## add KDSep
cacheSize=3584
runModeSet=('kd' 'bkvkd' 'kvkd')
func
