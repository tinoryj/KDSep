#!/bin/bash

source scripts/common.sh
works=8
rounds=1
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
fcl=10
flength=100
req="1000M"
op="1000M"

bonus="ec"
readRatios=(1) 

#### 2. UP2X workloads 

ExpName="_exp2_up2x"

## RocksDB, BlobDB, vLogDB
runModeSet=('raw' 'bkv' 'kv')
req="1000M"
bonus4="up2x"
bonus5=""
func

## Add KDSep 
cacheSize=3584
runModeSet=('kd' 'bkvkd' 'kvkd')
func
