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
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "bkvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( 512 ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" && "$cutKDCache" == "true" ]]; then
                                            blockCacheSize=$(( ${cacheSize} - 1 ))
                                            kdcacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "kvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( 512 ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" && "$cutKDCache" == "true" ]]; then
                                            blockCacheSize=$(( ${cacheSize} - 1 ))
                                            kdcacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchSize${batchSize} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
# gcThres0.6 splitThres0.3
                                    elif [[ "$runMode" == "kd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( 512 ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" && "$cutKDCache" == "true" ]]; then
                                            blockCacheSize=$(( ${cacheSize} - 1 ))
                                            kdcacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
batchSize=100000
blocksizes=(65536)
sstSizes=(16)
cacheSizes=(4096)
splitThres=0.3
gcWriteBackSize=500
maxBucketNumber=50000

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

checkrepeat="checkrepeat"
indexSet=(1 3 5 7 9)
runModeSet=('bkv' 'kv')
#func

#checkrepeat=""
#indexSet=(5)
#runModeSet=('bkv')
#checkrepeat="checkrepeat"

bonus="rmw"
#func

#### 1. YCSB 

bonus="rmw"
ExpName="_p35_exp1_ycsb"
indexSet=(5 95 10 5) # A, B, C, F
ops=("20M")
runModeSet=('bkv' 'kv' 'raw')
checkrepeat=""
#func

ops=("50M")
runModeSet=('bkv' 'kv' 'raw')
#func

cutKDCache="true"
ops=("20M")
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

cutKDCache="false"
ops=("20M")
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

indexSet=(5) # D 
bonus="workloadd"
cutKDCache="true"
runModeSet=('bkv' 'raw' 'kv' 'bkvkd' 'kvkd' 'kd')
#func

bonus="workloade"
checkrepeat="checkrepeat"
runModeSet=('bkv' 'raw' 'kv')
ops=("1M")
#func

#checkrepeat="scanThreads8"
#func

#checkrepeat="scanThreads32"
#func

cacheSizes=(4096)
ops=("20M")

#### 2. Performance
checkrepeat="checkrepeat"
bonus=""
indexSet=(1) 
ops=("50M")
ExpName="_p36_exp2_r10"
runModeSet=('bkv' 'raw' 'kv' 'bkvkd' 'kvkd' 'kd')
#func

### Test part
ExpName="_p36_test"
maxBucketNumber=32768
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

maxBucketNumber=16384
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

checkrepeat=""
maxBucketNumber=32768
runModeSet=('bkvkd')
bonus2="flushSize0"
bonus3="initBit8"
#func
bonus2="flushSize0"
bonus3="initBit20"
#func
bonus2="note_batch20k"
bonus3="initBit8"
#func
bonus2="note_batch20k"
bonus3="initBit8"
bonus4="flushSize0"
#func
#exit
runModeSet=('kvkd' 'kd' 'bkvkd')
indexSet=(1) 
func
checkrepeat="checkrepeat"
exit

#### 4. value size

ExpName="_p37_exp4_vsize"
indexSet=(5) 
fcs=(10 20 40 80 160 320 640)
rreqs=("100M" "50M" "25M" "13M" "6M" "4M" "2M")
fcs=(10 20 40 80)
rreqs=("100M" "50M" "25M" "13M")
runModeSet=('bkv' 'raw' 'kv' 'bkvkd' 'kvkd' 'kd') 
maxBucketNumber=32768
flengths=(100)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
    echo $gcWriteBackSize
    func
done

indexSet=(1) 
for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
    echo $gcWriteBackSize
    func
done
