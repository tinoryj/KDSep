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

#### 8. Vary split threshold 

bonus="ec"
splitThresholds=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)

ExpName="_exp8_splitthres"
runModeSet=('kvkd')

for ((ri=0; ri<${#splitThresholds[@]}; ri++)); do
    splitThres="${splitThresholds[$ri]}"
    func
done
