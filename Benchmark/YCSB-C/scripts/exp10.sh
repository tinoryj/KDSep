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

#### 10. Recovery performance 

ExpName="_exp9_crash"
runModeSet=('kvkd')

bonus="ec"

## Workload 1
for ((i=0; i<10; i++)); do
    bonus4="crash1800"   # Estimate total runtime in seconds
    func   # Run and randomly crash

    bonus4="recovery"
    func   # Recovery
done

## Workloads 2-4
runtimesecs=(300 600 9600) # Estimate total runtime in seconds
for ((i=0; i<10; i++)); do
    for ((j=0; j<=2; j++)); do
	bonus4="workload$(( ${j} + 2 ))"
	bonus5="crash${runtimesecs[$j]}"
	func   # Run and randomly crash

	bonus5="recovery"
	func   # Recovery
    done
done
