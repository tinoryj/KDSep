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

#### 11. Write buffer 

ExpName="_exp11_writebuf"
cacheSize=4096

bss=(1 2 4 8 16)
bonus="ec"
for ((bssi=0; bssi<${#bss[@]}; bssi++)); do
    batchSize=${bss[$bssi]}
    cacheSize=4096
    runModeSet=('kv')
    cacheSize=3584
    runModeSet=('kvkd')
    func
done
