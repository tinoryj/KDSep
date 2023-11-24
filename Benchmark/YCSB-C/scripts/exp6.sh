#!/bin/bash

source scripts/common.sh

#### 6. value size and delta size 

runModeSet=('kv' 'kvkd')

#### 6.1 value size 

ExpName="_exp6_vsize"
fcs=(1 2 5 10 20 40 80)

for fcl in "${fcs[@]}"; do 
    req="$(( 100 * 1024 * 1024 * 1024 / (24 + $fcl * 100) / 1000000 + 1 ))M" 
    func
done

#### 6.2 delta size

ExpName="_exp6_dsize"
fcs=(320 160 80 40 20 10)
fls=(25 50 100 200 400 800)
bucketNumbers=("4096" "8192" "16384" "32768" "65536" "131072")
req="14M"

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flength=${fls[$ri]}
    bucketNumber=${bucketNumbers[$ri]}
    func
done
