#!/bin/bash

source scripts/common.sh
works=8
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
fcl=10
flength=100
req="105M"
op="100M"
readRatios=(1)

#### 5. Skewness 

bonus="ec"
zipf=(0.8 1.0 1.1 1.2 0.9) 

## RocksDB, BlobDB, vLogDB
ExpName="_exp5_skew"
runModeSet=('kv')

for ((i=0; i<${#zipf[@]}; i++)); do
    bonus4="zipf${sts[$i]}"
    func
done

## add KDSep
cacheSize=3584
runModeSet=('kvkd')

for ((i=0; i<${#zipf[@]}; i++)); do
    bonus4="zipf${sts[$i]}"
    func
done
