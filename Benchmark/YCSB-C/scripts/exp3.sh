#!/bin/bash

source scripts/common.sh

#### 3. YCSB

ExpName="_exp3_ycsb"

### Workloads A-D, F
workloads=(a b c d f armw brmw frmw)

runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
    func
done

### Workload E 

op="1M"
runModeSet=('raw' 'bkv' 'kv' 'kd' 'bkvkd' 'kvkd')
bonus="workloade"
func
