#!/bin/bash

source scripts/common.sh

#### 5. Skewness 

## vLogDB, vLogDB+KDS
ExpName="_exp5_skew"
runModeSet=('kv' 'kvkd')
zipfs=(0.8 0.9 1.0 1.1 1.2) 

for zipf in ${zipfs[@]}; do
    func
done
