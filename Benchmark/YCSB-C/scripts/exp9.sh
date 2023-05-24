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

#### 9. Overhead of crash consistency components 

ExpName="_exp9_crash"
runModeSet=('kvkd')

#### 9.1 Without crash consistency

bonus=""

## Workload 1
func
## Workloads 2-4
for ((i=2; i<=4; i++)); do
    bonus4="workload${i}"
    func
done
bonus4=""

#### 9.2 With crash consistency

bonus="ec"

## Workload 1
func
## Workloads 2-4
for ((i=2; i<=4; i++)); do
    bonus4="workload${i}"
    func
done
