#!/bin/bash

source scripts/common.sh

#### 9. Overhead of crash consistency components 

ExpName="_exp9_crash_thpt"
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
