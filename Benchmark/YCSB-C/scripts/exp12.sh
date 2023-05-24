#!/bin/bash

source scripts/common.sh

works=8
batchSize=2
cacheSize=3584
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
fcl=10
flength=100
req="105M"
op="100M"
readRatios=(1)

#### 11. Write buffer 

ExpName="_exp12_kdc"

bonus="ec"
kdcs=(0 64 128 256 1024)
for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    runModeSet=('kvkd') 
    func
done
