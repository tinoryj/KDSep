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
#                                    if [[ $bucketNumber -gt 16384 ]]; then
#                                        bucketNumber=16384
#                                    fi
                                    ratio="0.$index"
                                    if [[ $index -eq 10 ]]; then
                                        ratio="1"
                                    fi
                                    echo $runMode $ratio

                                    if [[ "$ratio" == "1" && $runMode =~ "kd" ]]; then
                                        continue
                                    fi

                                    if [[ "$runMode" == "raw" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} $checkrepeat #paretokey
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
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus} $checkrepeat #paretokey
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
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchSize${batchSize} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} #paretokey
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
                                        scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus} #paretokey
                                    fi
                                done
                            done
                        done
                    done
#		scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
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

if [[ $(diff ycsbc ycsbc_release) -ne 0 ]]; then
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
func
