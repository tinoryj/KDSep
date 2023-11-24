#!/bin/bash

source scripts/common.sh

#### 12. Maximum KD cache 

ExpName="_exp12_kdc"

kdcs=(0 32 64 128 256 512 1024)
runModeSet=('kvkd') 
for kdcacheSize in ${kdcs[@]}; do
    func
done
