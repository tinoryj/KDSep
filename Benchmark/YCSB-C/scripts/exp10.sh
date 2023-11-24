#!/bin/bash

source scripts/common.sh

#### 10. Recovery performance 

ExpName="_exp10_recovery"
runModeSet=('kvkd')

## Workload 1
for ((i=0; i<10; i++)); do
    bonus4="crash3200"          # Estimate total runtime in seconds
    func   # Run and randomly crash

    bonus4="recovery"
    func   # Recovery
done

## Workloads 2-4
runtimesecs=(900 3300 5500)     # Estimate total runtime in seconds
for ((i=0; i<10; i++)); do
    for ((j=0; j<=2; j++)); do
	bonus4="workload$(( ${j} + 2 ))"
	bonus5="crash${runtimesecs[$j]}"
	func   # Run and randomly crash

	bonus5="recovery"
	func   # Recovery
    done
done
