#!/bin/bash

source scripts/common.sh

#### 11. Write buffer 

ExpName="_exp11_writebuf"

bss=(1 2 4 8 16)
runModeSet=('kv' 'kvkd')
for batchSize in ${bss[@]}; do 
    func
done
