#!/bin/bash

source scripts/common.sh

works=8
rounds=1
bfs=(10)
batchSize=2
sstSizes=(16)
cacheSize=4096
splitThres=0.8
gcWriteBackSize=100000
bucketNumber=32768
kdcacheSize=512
flength=100
req="1500M"
req="605M"
req="105M"

sst="16"
memtable="64"
l1sz="256"


#### 0. Motivation

ExpName="_p34_motivation"
ExpName="_post2_motivation"
bonus=""
readRatios=(1 3 5 7 9)
op="100M"
runModeSet=('raw' 'bkv' 'kv')
readRatios=(1)
#func

############################# Experiments start
ExpName="_q1_compaction"
ExpName="_new_exp1"
ExpName="_post3_exp_ycsb_zipf0.9"
ExpName="_post5_addexp4_5"
ExpName="_post6_cpu"
readRatios=(0)
runModeSet=('raw')

#sst="16"
#memtable=1024 # "$(( $sst ))"
#l1sz="$(( $memtable * 4 ))"
#
#sstszs=(32 64 128 256 512 1024)
#sstszs=(16 1024)
#
#for ((ri=0; ri<${#sstszs[@]}; ri++)); do
#    func
#done

req="105M"
op="100M"
#req="1M"
#op="1M"
readRatios=(1 3 5 7 9)
readRatios=(1)
runModeSet=('raw' 'bkv' 'kv')
#runModeSet=('kv')

cacheSize=4096
#func

bonus="ec"
#bonus5="workerT4"
runModeSet=('kd' 'kvkd' 'bkvkd')
#runModeSet=('kd' 'bkvkd')
cacheSize=3584
#func

runModeSet=('raw' 'bkv' 'kvkd')
cacheSize=4096
#func
runModeSet=('kd' 'bkvkd')
cacheSize=3584
#func

ExpName="Exp_post7_zipf"
sts=(0.8 1.0 1.1 1.2 0.9)
#sts=(0.7 0.8 1.0 1.1 1.2)
readRatios=(1)
bucketNumber=32768
for ((i=0; i<${#sts[@]}; i++)); do
    runModeSet=('bkv' 'raw')
    runModeSet=('kv')
    cacheSize=4096
    bonus5="zipf${sts[$i]}"
    func
    runModeSet=('bkvkd' 'kd')
    runModeSet=('kvkd')
    cacheSize=3584
    func
done
exit

#for ((i=0; i<5; i++)); do
#    for ((ri=0; ri<${#sstszs[@]}; ri++)); do
#        sst="${sstszs[$ri]}"
#        memtable=1024 # "$(( $sst ))"
#        l1sz="$(( $memtable * 4 ))"
#        func
#        exit
#    done
#    rm -rf /mnt/sn640/KDSepanonymous/loaded* 
#    for ((ri=$(( ${#sstszs[@]} - 1 )); ri>=0; ri--)); do
#        sst="${sstszs[$ri]}"
#        memtable=1024 # "$(( $sst ))"
#        l1sz="$(( $memtable * 4 ))"
#        func
#    done
#    rm -rf /mnt/sn640/KDSepanonymous/loaded* 
#done
#exit

#### 1. YCSB 

bonus="rmw"
#ExpName="_p35_exp1_ycsb"
workloads=(c d f)
readRatios=(5) # A, B, C, F
op="20M"

cacheSize=4096
runModeSet=('bkv' 'kv' 'raw')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

workloads=(a b c d f)
cacheSize=3584
runModeSet=('bkvkd' 'kvkd' 'kd')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

op="1M"
cacheSize=4096
runModeSet=('bkv' 'kv' 'raw')
runModeSet=('bkv' 'raw')
#bonus="workloade"
#bonus4="nodirectreads"
#mem="16g"
#func
bonus4=""
mem=""

#### 1.1 Test: 16G memory, not cutting KD cache

###########################################################

#### 2. Performance
bonus="ep"
initBit="initBit10"
wbread="wbread0"
readRatios=(1) 
op="100M"
ExpName="_p50_exp2"
cacheSize=4096
runModeSet=('bkv' 'raw' 'kv')
runModeSet=('kv')
#func

cacheSize=3712
runModeSet=('kvkd' 'bkvkd' 'kd')
runModeSet=('bkvkd')
wbread="wbread1"
#func
wbread="wbread0"

#### 3. UP2X

op="300M"
ExpName="_p50_exp3_up2x"
cacheSize=6144
runModeSet=('kv' 'raw' 'bkv')
req="1000M"
fcl=1
bonus4="up2x"
bonus5="ec"
#func

cacheSize=5632
runModeSet=('kvkd' 'kd' 'bkvkd')
func
op="3000M"
#func

bonus4=""
#bonus5=""
exit

############### 5. Read ratio

##### Running
ExpName="_p50_exp5_ratio"
fcl=10
req="105M"
readRatios=(3 5 7 9) 
runModeSet=('bkv' 'raw' 'kv') 
runModeSet=('kv') 
cacheSize=4096
#func

readRatios=(3 5 7 9) 
runModeSet=('kvkd') 
cacheSize=3584
wbread="wbread1"
#func
wbread="wbread0"
#func
wbread="wbread0"

runModeSet=('bkvkd') 
cacheSize=3584
#func

#### 4. value size

ExpName="_p50_exp4_vsize"
req="100M"
readRatios=(1) 
fcs=(10 20 40 80 160 320 640)
rreqs=("100M" "50M" "25M" "13M" "6M" "4M" "2M")

fcs=(10 20 40 80)
rreqs=("105M" "54M" "27M" "14M")

#fcs=(2 5)
#rreqs=("537M" "215M")

runModeSet=('bkv' 'raw' 'kv')
runModeSet=('kv')
cacheSize=4096
cacheSize=2048

#for ((ri=0; ri<${#rreqs[@]}; ri++)); do
for ((ri=1; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    req=${rreqs[$ri]}
#    func
done

cacheSize=4096
runModeSet=('bkvkd' 'kvkd' 'kd') 
runModeSet=('kvkd') 
cacheSize=3584

#for ((ri=0; ri<${#rreqs[@]}; ri++)); do
for ((ri=1; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    req=${rreqs[$ri]}
    wbread="wbread1"
#    func
    wbread="wbread0"
#    func
done

########### Delta size!

ExpName="_p50_exp4_dsize"
runModeSet=('bkv' 'raw' 'kv')
runModeSet=('kv')
cacheSize=2048

fcs=(80 40 20 10)
fls=(100 200 400 800)
req="14M"

#for ((ri=0; ri<${#fcs[@]}; ri++)); do
for ((ri=2; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flength=${fls[$ri]}
#    func
done

bns=(32768 65536 98304 131072)
runModeSet=('bkvkd' 'kvkd' 'kd') 
runModeSet=('kvkd') 
cacheSize=1024

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flength=${fls[$ri]}
    bucketNumber=${bns[$ri]}
    wbread="wbread1"
#    func
    wbread="wbread0"
#    func
done
bucketNumber=32768

############### A. Test 

#ExpName="_p42_test"

fcl=10
fls=(100)
flength=100
req="105M"


op="300M"
op="100M"
readRatios=(1) 
runModeSet=('kvkd' 'bkvkd') 
cacheSize=3584
#func

runModeSet=('kv' 'bkv') 
cacheSize=4096
#func



##### Test

#ExpName="Exp_p36_test_part4"
kdcacheSize=512
readRatios=(1) 
cacheSize=3584
runModeSet=('bkvkd') 
#func


ExpName="Exp_p50_exp7_kdc"

kdcs=(0 64 128 256 512 1024)

for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    runModeSet=('kvkd' 'bkvkd' 'kd') 
    runModeSet=('kvkd') 
#    func
#    wbread="wbread1"
#    func
    wbread="wbread0"
#    func
done

kdcs=(0 64 128 256 1024)
for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    runModeSet=('kvkd') 
#    func
done
kdcacheSize=512

ExpName="Exp_p50_exp6_wbuf"
bss=(64K 128K 256K 512K 1 2 4 8 16)
bss=(16K 32K 64K 128K 256K 512K 1 2 4 8 16)
#bss=(1 16)
fcl=10
flength=100
req="105M"
op="100M"

for ((bssi=0; bssi<${#bss[@]}; bssi++)); do
    readRatios=(1)
    batchSize=${bss[$bssi]}
#    wbread="wbread200"
#    func
    wbread="wbread0"
#func
    cacheSize=4096
    runModeSet=('kv')
#    func
    cacheSize=3584
    runModeSet=('kvkd')
    wbread="wbread1"
#    func
    wbread="wbread0"
    func

    bonus4="overwrite"
#1    func
    bonus4=""
    readRatios=(1)
done
exit
batchSize=2

ExpName="Exp_p50_exp9_bucsize"
bucnums=(16384 65536 131072)
bucnums=(65536 131072)
readRatios=(1)
runModeSet=('kvkd')

for ((buci=0; buci<${#bucnums[@]}; buci++)); do
    bucketNumber=${bucnums[$buci]}
#    func
done
bucketNumber=32768

ExpName="Exp_p50_exp9_bucsize"
bucsizes=(512)
bucnums=(16384)
readRatios=(1)
cacheSize=3584
runModeSet=('kvkd')
batchSize=2

for ((buci=0; buci<${#bucsizes[@]}; buci++)); do
    bonus4="bucketSize$(( ${bucsizes[$buci]} * 1024 ))"
    bucketNumber=${bucnums[$buci]}
    for ((j=4; j<5; j++)); do
        bonus5="unsort$(( $j * 40 ))"
#        func
    done
    bonus5=""
done
bonus4=""
bucketNumber=32768

ExpName="Exp_p50_exp10_wbread"
wbreads=(1 200 400 800 1000)
cacheSize=4096
runModeSet=('kv')
#func

cacheSize=3584
runModeSet=('kvkd')

readRatios=(1)
for ((wbi=0; wbi<${#wbreads[@]}; wbi++)); do
    wbread="wbread${wbreads[$wbi]}"
#    func
done
readRatios=(5)
for ((wbi=0; wbi<${#wbreads[@]}; wbi++)); do
    wbread="wbread${wbreads[$wbi]}"
#    func
done
wbread="wbread0"

ExpName="Exp_p50_exp11_splitthres"
sts=(0.1 0.9)
runModeSet=('kvkd')
readRatios=(1)

for ((i=0; i<${#sts[@]}; i++)); do
    splitThres=${sts[$i]}
    func
done
splitThres=0.8

ExpName="Exp_p50_exp12_pbbuf"
sts=(1024 2048 3072)
runModeSet=('kvkd')
readRatios=(1 5)

for ((i=0; i<${#sts[@]}; i++)); do
    bonus4="flushSize${sts[$i]}"
#    func
done
bonus4=""

ExpName="Exp_p50_exp13_ud"
readRatios=(0)
for ((i=0; i<${#sts[@]}; i++)); do
    runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
#    func
done
bonus4=""

ExpName="Exp_p50_exp14_zipf"
sts=(1.0 1.1 1.2)
sts=(1.0 1.1 1.2)
#sts=(0.7 0.8 1.0 1.1 1.2)
readRatios=(1)
bucketNumber=32768
for ((i=0; i<${#sts[@]}; i++)); do
    runModeSet=('bkv' 'raw')
    runModeSet=('kv')
    cacheSize=4096
    bonus4="zipf${sts[$i]}"
#    func
    runModeSet=('bkvkd' 'kd')
    runModeSet=('kvkd')
    cacheSize=3584
#    wbread="wbread200"
#    func
#    wbread="wbread200"
#    func
    wbread="wbread0"
    bonus4=""
done

ExpName="Exp_p50_exp15_initBit"
sts=(7 8 9 11 12 13 14) 
for ((i=0; i<${#sts[@]}; i++)); do
    initBit="initBit${sts[$i]}"
#    func
done
