#!/bin/bash

source scripts/common.sh

#### 8. Vary split threshold 

splitThresholds=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)
ExpName="_exp8_splitthres"
runModeSet=('kvkd')

for splitThres in ${splitThresholds[@]}; do
    func
done
