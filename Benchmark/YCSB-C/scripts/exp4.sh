#!/bin/bash

source scripts/common.sh

#### 4. Read-to-RMW ratio 

readRatios=(1 3 5 7 9) 

## vLogDB
ExpName="_exp4_ratio"
runModeSet=('kv' 'kvkd')
func
