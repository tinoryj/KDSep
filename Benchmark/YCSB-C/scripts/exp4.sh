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

#### 4. Read-to-RMW ratio 

bonus="ec"
readRatios=(1 3 5 7 9) 

## vLogDB
ExpName="_exp4_ratio"
runModeSet=('kv')
func

## add KDSep
cacheSize=3584
runModeSet=('kvkd')
func
