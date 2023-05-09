#!/bin/bash

func() {
    for bs in "${blocksizes[@]}"; do
        for ((j=0; j<${#flengths[@]}; j++)); do
            for ((si=0; si<${#sstSizes[@]}; si++)); do
                sst=${sstSizes[$si]}
                memtable=$(( ${sst} * 4 ))
                l1sz=$(( ${sst} * 16 ))

                for runMode in "${runModeSet[@]}"; do
                    threadNumber=8

                    fl=${flengths[$j]}
                    req=${reqs[$j]}

                    for ((roundi=1; roundi<=${rounds}; roundi++)); do
                        for op in "${ops[@]}"; do
                            opnum=`echo $op | sed 's/M/000000/g' | sed 's/K/000/g'`
                            for index in "${indexSet[@]}"; do
                                for ((k=0; k<${#cacheSizes[@]}; k++)); do
                                    cacheSize=${cacheSizes[$k]}
#                                    bucketNumber=$(echo "( $opnum * (10 - $index) / 10 * (38 + $fl) ) / 262144 / 0.5"|bc)
                                    bucketNumber=$(echo "( $opnum * (10 - $index) / 10 * (38 + $fl) ) / 256 / 1024 / 0.5"|bc)
                                    if [[ $index -gt 10 ]]; then
                                        bucketNumber=$(echo "( $opnum * (100 - $index) / 100 * (38 + $fl) ) / 256 / 1024 / 0.5"|bc)
                                    fi
                                    if [[ $bucketNumber -gt $maxBucketNumber ]]; then
                                        bucketNumber=$maxBucketNumber
                                    fi
                                    ratio="0.$index"
                                    if [[ $index -eq 10 ]]; then
                                        ratio="1"
                                    fi

                                    if [[ "$ratio" == "1" && $runMode =~ "kd" ]]; then
                                        continue
                                    fi

                                    if [[ "$runMode" == "raw" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "bkvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "kvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchSize${batchSize} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat #paretokey
# gcThres0.6 splitThres0.3
                                    elif [[ "$runMode" == "kd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${wbread} ${initBit} $checkrepeat #paretokey
                                    fi
                                done
                            done
                        done
                    done
#		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
#		    cache$cacheSize \
#		    readRatio$ratio Exp$ExpName bs${bs} clean 
                done
            done
        done
    done
}

works=8
gcs=2
rounds=1
bfs=(10)
batchSize=2
blocksizes=(65536)
sstSizes=(16)
cacheSizes=(4096)
splitThres=0.8
gcWriteBackSize=100000
maxBucketNumber=32768
kdcacheSize=512

if [[ $(diff ycsbc ycsbc_release | wc -l) -ne 0 ]]; then
    echo "Not release version!"
    exit
fi

flengths=(100)
reqs=("105M")

#### 0. Motivation

ExpName="_p34_motivation"
bonus=""
indexSet=(1 3 5 7 9)
ops=("50M")
runModeSet=('raw')

checkrepeat=""
indexSet=(1 3 5 7 9)
runModeSet=('bkv' 'kv')
#func

############################# Experiments start
ExpName="_p50_exp1_ycsb"

#### 1. YCSB 

bonus="rmw"
#ExpName="_p35_exp1_ycsb"
workloads=(c d f)
indexSet=(5) # A, B, C, F
ops=("20M")

cacheSizes=(4096)
runModeSet=('bkv' 'kv' 'raw')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

workloads=(a b c d f)
cacheSizes=(3584)
runModeSet=('bkvkd' 'kvkd' 'kd')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

ops=("1M")
cacheSizes=(4096)
runModeSet=('bkv' 'kv' 'raw')
runModeSet=('bkv')
bonus="workloade"
bonus4="nodirectreads"
mem="16g"
#func
bonus4=""
mem=""

#### 1.1 Test: 16G memory, not cutting KD cache

###########################################################

#### 2. Performance
bonus="ep"
initBit="initBit10"
wbread="wbread0"
indexSet=(1) 
ops=("100M")
ExpName="_p50_exp2"
cacheSizes=(4096)
runModeSet=('bkv' 'raw' 'kv')
bonus5=""
#bonus5="blobgcforce0.8"
runModeSet=('kv')
#func

cacheSizes=(3584)
runModeSet=('kvkd' 'bkvkd' 'kd')
runModeSet=('kvkd')
#wbread="wbread200"
#func
bonus5=""
wbread="wbread0"

#### 3. UP2X

ExpName="_p50_exp3_up2x"
cacheSizes=(4096)
runModeSet=('kv' 'raw' 'bkv')
runModeSet=('bkv')
reqs=("1000M")
ops=("100M")
ops=("300M")
fcl=1
bonus4="up2x"
bonus5=""
wbread="wbread1"
#func

cacheSizes=(3584)
runModeSet=('kvkd' 'kd' 'bkvkd')
runModeSet=('kvkd')
#func
bonus4=""
bonus5=""
ops=("100M")

#### 4. value size

ExpName="_p50_exp4_vsize"
reqs=("100M")
indexSet=(1) 
fcs=(10 20 40 80 160 320 640)
rreqs=("100M" "50M" "25M" "13M" "6M" "4M" "2M")

fcs=(10 20 40 80)
rreqs=("105M" "54M" "27M" "14M")

runModeSet=('bkv' 'raw' 'kv')
cacheSizes=(4096)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    func
done

runModeSet=('bkvkd' 'kvkd' 'kd') 
cacheSizes=(3584)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    func
done

########### Delta size!

#ExpName="_p47_exp4_delta"
runModeSet=('bkv' 'raw' 'kv')
runModeSet=('kv')
cacheSizes=(2048)

fcs=(40 20 10)
fls=(200 400 800)
fcs=(40 20)
fls=(200 400)
reqs=("14M")

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
#    func
done

fcs=(40 10)
fls=(200 800)
bns=(65536 131072)
runModeSet=('bkvkd' 'kvkd' 'kd') 
runModeSet=('kvkd') 
#cacheSizes=(3584)
cacheSizes=(1024)

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
    maxBucketNumber=${bns[$ri]}
#    func
done
maxBucketNumber=32768

############### A. Test 

#ExpName="_p42_test"

fcl=10
fls=(100)
flengths=(100)
reqs=("105M")


ops=("300M")
indexSet=(1) 
runModeSet=('kvkd' 'bkvkd') 
cacheSizes=(3584)
#func

runModeSet=('kv' 'bkv') 
cacheSizes=(4096)
#func


############### 5. Read ratio
ops=("100M")

##### Running
#ExpName="_p42_test"
indexSet=(1 3 5 7 9) 
runModeSet=('bkv' 'raw' 'kv') 
runModeSet=('kv') 
cacheSizes=(4096)
#func

indexSet=(3 5 7) 
runModeSet=('kvkd') 
cacheSizes=(3584)
wbread="wbread200"
#func
wbread="wbread0"

runModeSet=('bkvkd') 
cacheSizes=(3584)
#func

##### Test

kdcs=(512)
#ExpName="Exp_p36_test_part4"

kdcacheSize=${kdcs[$kdcsi]}
indexSet=(1) 
cacheSizes=(3584)
runModeSet=('bkvkd') 
#func


ExpName="Exp_p50_exp7_kdc"

for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    indexSet=(1 3) 
    cacheSizes=(3584)
    runModeSet=('kvkd' 'bkvkd' 'kd') 
#    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 10 * 6))
#    func
done

kdcs=(0 64 128 256 1024)
for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    indexSet=(1) 
    cacheSizes=(3584)
    runModeSet=('kvkd') 
#    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 10 * 6))
#    func
done
#gcWriteBackSize=600
kdcacheSize=512

ExpName="Exp_p50_exp6_wbuf"
bss=(64K 128K 256K 512K 1 2 4 8 16)
bss=(64K 256K 1 16)
fcl=10
flengths=(100)
reqs=("105M")
ops=("100M")

for ((bssi=0; bssi<${#bss[@]}; bssi++)); do
    indexSet=(1)
    cacheSizes=(3584)
    runModeSet=('kvkd')
    batchSize=${bss[$bssi]}
#    wbread="wbread200"
#    func
    wbread="wbread0"
#func
    cacheSizes=(4096)
    runModeSet=('kv')
    func
    indexSet=(0)
#    cacheSizes=(4096)
#    runModeSet=('kv')
#    func

    bonus4="overwrite"
#1    func
    bonus4=""
    indexSet=(1)
done
batchSize=2
exit

ExpName="Exp_p50_exp9_bucsize"
bucnums=(16384 65536 131072)
indexSet=(1)
cacheSizes=(3584)
runModeSet=('kvkd')

for ((buci=0; buci<${#bucnums[@]}; buci++)); do
    maxBucketNumber=${bucnums[$buci]}
    splitThres=0.8
#    func
    if [[ ${buci} -ge 1 ]]; then
        splitThres=0.5
#        func
    fi
done
maxBucketNumber=32768
splitThres=0.8

ExpName="Exp_p50_exp9_bucsize"
bucsizes=(512)
bucnums=(16384)
indexSet=(1)
cacheSizes=(3584)
runModeSet=('kvkd')
batchSize=2

for ((buci=0; buci<${#bucsizes[@]}; buci++)); do
    bonus4="bucketSize$(( ${bucsizes[$buci]} * 1024 ))"
    maxBucketNumber=${bucnums[$buci]}
    for ((j=4; j<5; j++)); do
        bonus5="unsort$(( $j * 40 ))"
#        func
    done
    bonus5=""
done
bonus4=""
maxBucketNumber=32768

ExpName="Exp_p50_exp10_wbread"
wbreads=(1 200 400 800 1000)
cacheSizes=(4096)
runModeSet=('kv')
#func

cacheSizes=(3584)
runModeSet=('kvkd')

indexSet=(1)
for ((wbi=0; wbi<${#wbreads[@]}; wbi++)); do
    wbread="wbread${wbreads[$wbi]}"
#    func
done
indexSet=(5)
for ((wbi=0; wbi<${#wbreads[@]}; wbi++)); do
    wbread="wbread${wbreads[$wbi]}"
#    func
done
wbread="wbread0"

ExpName="Exp_p50_exp11_splitthres"
sts=(0.5 0.6 0.7 0.9)
runModeSet=('kvkd')
indexSet=(1 5)

for ((i=0; i<${#sts[@]}; i++)); do
    splitThres=${sts[$i]}
#    func
done
splitThres=0.8

ExpName="Exp_p50_exp12_pbbuf"
sts=(1024 2048 3072)
runModeSet=('kvkd')
indexSet=(1 5)

for ((i=0; i<${#sts[@]}; i++)); do
    bonus4="flushSize${sts[$i]}"
#    func
done
bonus4=""

ExpName="Exp_p50_exp13_ud"
indexSet=(0)
for ((i=0; i<${#sts[@]}; i++)); do
    runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
#    func
done
bonus4=""

ExpName="Exp_p50_exp14_zipf"
sts=(1.0 1.1 1.2)
sts=(1.0 1.1 1.2)
#sts=(0.7 0.8 1.0 1.1 1.2)
indexSet=(1)
maxBucketNumber=32768
for ((i=0; i<${#sts[@]}; i++)); do
    runModeSet=('bkv' 'raw')
    runModeSet=('kv')
    cacheSizes=(4096)
    bonus4="zipf${sts[$i]}"
    func
    runModeSet=('bkvkd' 'kd')
    runModeSet=('kvkd')
    cacheSizes=(3584)
#    wbread="wbread200"
    func
#    wbread="wbread200"
#    func
    wbread="wbread0"
    bonus4=""
done
