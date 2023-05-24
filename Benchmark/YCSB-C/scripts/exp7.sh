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

#### 7. Vary k 

#### 7.1 Disable split and merge 
# (Set split threshold = 110% of capacity) 

bonus="ec"
ks=(10 11 12 13 14 15)

ExpName="_exp7_kdcache"
runModeSet=('kvkd')
splitThres=1.1

for ((ri=0; ri<${#ks[@]}; ri++)); do
    initBit="initBit${ks[$ri]}"
    func
done

#### 7.2 Enable split and merge 
splitThres=0.8

for ((ri=0; ri<${#ks[@]}; ri++)); do
    initBit="initBit${ks[$ri]}"
    func
done

