#!/bin/bash

func() {
    for bs in "${blocksizes[@]}"; do
        for ((j=0; j<${#flengths[@]}; j++)); do
            for ((si=0; si<${#sstSizes[@]}; si++)); do
                sst=${sstSizes[$si]}
                memtable=$(( ${sst} * 4 ))
                l1sz=$(( ${sst} * 16 ))

                for runMode in "${runModeSet[@]}"; do
                    threadNumber=16

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
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "bkvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "kvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchSize${batchSize} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
# gcThres0.6 splitThres0.3
                                    elif [[ "$runMode" == "kd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} batchSize${batchSize} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
gcWriteBackSize=600
maxBucketNumber=32768
kdcacheSize=512

if [[ $(diff ycsbc ycsbc_release | wc -l) -ne 0 ]]; then
    echo "Not release version!"
    exit
fi

flengths=(100)
reqs=("100M")

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
#### 1. YCSB 

bonus="rmw"
ExpName="_p35_exp1_ycsb"
workloads=(a b c d f)
indexSet=(5) # A, B, C, F
ops=("20M")

cacheSizes=(4096)
runModeSet=('bkv' 'kv' 'raw')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

cacheSizes=(3584)
runModeSet=('bkvkd' 'kvkd' 'kd')
for w in "${workloads[@]}"; do
    bonus="workload${w}"
#    func
done

ops=("1M")
cacheSizes=(4096)
runModeSet=('bkv' 'kv' 'raw')
bonus="workloade"
#func

#### 1.1 Test: 16G memory, not cutting KD cache

###########################################################

#### 2. Performance
checkrepeat="checkrepeat"
bonus="ep"
indexSet=(1) 
ops=("100M")
ExpName="_p36_exp2_r10"
cacheSizes=(4096)
runModeSet=('bkv' 'raw' 'kv')
#func

cacheSizes=(3584)
runModeSet=('bkvkd' 'kd' 'kvkd')
#func

#### 4. value size

ExpName="_p37_exp4_test"
checkrepeat=""
indexSet=(1) 
fcs=(10 20 40 80 160 320 640)
rreqs=("100M" "50M" "25M" "13M" "6M" "4M" "2M")

fcs=(20 40 80)
rreqs=("50M" "25M" "13M")

maxBucketNumber=32768
flengths=(100)
cacheSizes=(4096)
bonus2="blobgcforce0.8"
bonus2=""
bonus5="wbread0"
runModeSet=('bkv' 'raw' 'kv')
runModeSet=('bkv')

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})

#    func
    break
done

runModeSet=('bkvkd' 'kvkd' 'kd') 
runModeSet=('bkvkd') 
cacheSizes=(3584)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 10 * 6))
#    func
done

########### Delta size!

ExpName="_p37_exp4_test_delta"
runModeSet=('bkv' 'raw' 'kv')
cacheSizes=(4096)

fcs=(20)
fls=(400)
reqs=("14M")

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
#    func
done

runModeSet=('bkvkd' 'kvkd' 'kd') 
runModeSet=('kvkd') 
cacheSizes=(3584)
maxBucketNumber=131072

for ((ri=0; ri<${#fcs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
#    func
done

# Test
maxBucketNumber=262144
for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
#    func
done

############### A. Test 

ExpName="_p42_test"
bonus5="timeout"

fcl=10
fls=(100)
flengths=(100)
reqs=("105M")


ops=("300M")
indexSet=(1) 
runModeSet=('kvkd' 'bkvkd') 
cacheSizes=(3584)
maxBucketNumber=65536
#func

runModeSet=('kv' 'bkv') 
cacheSizes=(4096)
#func


############### 5. Read ratio
ops=("100M")

##### Running
indexSet=(1 3 5 7 9) 
indexSet=(9) 
runModeSet=('raw' 'kv') 
cacheSizes=(4096)
#func

indexSet=(7 9) 
runModeSet=('kd') 
cacheSizes=(3584)
maxBucketNumber=32768
#func

indexSet=(9) 
runModeSet=('bkvkd') 
cacheSizes=(3584)
maxBucketNumber=32768
#func

##### Test

kdcs=(512)
ExpName="Exp_p36_test_part4"

kdcacheSize=${kdcs[$kdcsi]}
indexSet=(1) 
cacheSizes=(3584)
runModeSet=('bkvkd') 
bonus5="wbread0"
gcWriteBackSize="600"
#func
bonus5=""
#func


ExpName="Exp_p41_exp6_new_kdc"

for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    indexSet=(1 3) 
    cacheSizes=(3584)
    runModeSet=('kvkd' 'bkvkd' 'kd') 
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 10 * 6))
    bonus5=""
#    func
    bonus5="wbread0"
#    func
done

bonus5="wbread0"
kdcs=(0 32 64 128 256 1024)
for ((kdcsi=0; kdcsi<${#kdcs[@]}; kdcsi++)); do
    kdcacheSize=${kdcs[$kdcsi]}
    indexSet=(1 5) 
    cacheSizes=(3584)
    runModeSet=('kvkd') 
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 10 * 6))
#    func
done

ExpName="Exp_p44_exp7_wbuf"
bss=(1 4 8 16)
fcl=10
flengths=(100)
gcWriteBackSize=600
reqs=("105M")
ops=("100M")

bonus3="initBit10"
for ((bssi=0; bssi<${#bss[@]}; bssi++)); do
    kdcacheSize=512
    indexSet=(1)
    cacheSizes=(3584)
    runModeSet=('bkvkd' 'kvkd')
    batchSize=${bss[$bssi]}
    if [[ $bssi -ge 1 ]]; then
#        func
    fi
    cacheSizes=(4096)
    runModeSet=('bkv' 'kv')
#    func
done

ExpName="Exp_p45_exp8_bucsize"
bucsizes=(128 512 1024)
bucnums=(65536 16384 8192)
indexSet=(1)
cacheSizes=(3584)
runModeSet=('bkvkd' 'kvkd')

for ((buci=0; buci<${#bucsizes[@]}; buci++)); do
    bonus4=${bucsizes[$buci]}
    maxBucketNumber=${bucnums[$buci]}
    func
done
