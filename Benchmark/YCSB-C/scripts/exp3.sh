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
req="105M"
op="100M"
readRatios=(5) # Arbitrary 

#### 3. YCSB

### Workloads A-D, F
workloads=(a b c d f)
op="20M"

runModeSet=('raw' 'bkv' 'kv')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
    func
done

runModeSet=('kd' 'bkvkd' 'kvkd')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
    func
done

### Workload E 

op="1M"
cacheSize=4096
runModeSet=('raw' 'bkv' 'kv' 'kd' 'bkvkd' 'kvkd')
bonus="workloade"
func
