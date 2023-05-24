#!/bin/bash

source scripts/common.sh
works=8
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
fcl=10
flength=100
req="105M"
op="100M"
readRatios=(1)

#### 6. value size and delta size 

#### 6.1 value size 

bonus="ec"
zipf=(0.8 1.0 1.1 1.2 0.9) 
fcs=(10 20 40 80)
rreqs=("105M" "54M" "27M" "14M")

ExpName="_exp6_vsize"
runModeSet=('kv')

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    req=${rreqs[$ri]}
    func
done

## add KDSep
cacheSize=3584
runModeSet=('kvkd')

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    req=${rreqs[$ri]}
    func
done

#### 6.2 delta size

ExpName="_exp6_vsize"
fcs=(80 40 20 10)
fls=(100 200 400 800)
req="14M"

cacheSize=4096
runModeSet=('kv')

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flength=${fls[$ri]}
    func
done

cacheSize=1024  # 512MiB block cache is enough
runModeSet=('kvkd')
bns=(32768 65536 131072 262144)

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flength=${fls[$ri]}
    bucketNumber=${bns[$ri]}
    func
done
