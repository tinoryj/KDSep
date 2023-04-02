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
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
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
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
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
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
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
indexSet=(1 3 5 7 9 10)

indexSet=(1 5 10)
cacheSizes=(2048 2048 2048 4096 4096 4096 4096 4096 4096 4096 1024 1024 1024 1024)

indexSet=(5 1)
blocksizes=(65536)
reqs=("25M")
sstSizes=(16)
cacheSizes=(4096)
indexSet=(0 5 1 10)
# memSizes the same


### Base test!!!
works=8
gcs=2
flengths=(100 400 100 400)
flengths=(400)
flengths=(100 400)
runModeSet=('kvkd' 'kd' 'bkvkd' 'kv' 'raw' 'bkv')
runModeSet=('kd' 'bkvkd')
runModeSet=('kvkd' 'bkvkd' 'kd')
runModeSet=('kv' 'bkv' 'raw' 'kvkd' 'bkvkd' 'kd')
cacheSizes=(2048)
indexSet=(1 3 5 7 9 10)
ops=("10M")
ops=("20M")
reqs=("40M" "10M" "100M" "25M")
reqs=("100M" "25M")

bonus="rmw"

flengths=(100)
ExpName="motivation"
indexSet=(1 3 5 7 9)
reqs=("100M")
ops=("30M")
cacheSizes=(4096)
runModeSet=('raw')

#ExpName="debug"
#bonus=""
#runModeSet=('kv')
#indexSet=(5)
#reqs=("1M")
#ops=("10K")
#func
#exit

bonus=""
ExpName="_p27_test_partial_merge"
indexSet=(1)
works=8
#indexSet=(5)
flengths=(400 100)
reqs=("25M" "100M")
ops=("10M")
#ops=("100M")
#runModeSet=('kvkd' 'kv' 'bkvkd' 'kd' 'raw' 'bkv')
runModeSet=('kvkd')
cacheSizes=(2048)
cacheSizes=(4096)
splitThres=0.3
gcWriteBackSize=500

if [[ $(diff ycsbc ycsbc_release) -ne 0 ]]; then
    echo "Not release version!"
    exit
fi

#func
#
#works=16
#func

#indexSet=(1 3 5 7 9)
#runModeSet=('kv' 'bkvkd' 'bkv' 'kd' 'raw')
#runModeSet=('kv' 'bkv' 'raw')
#runModeSet=('kv' 'raw' 'bkv')
#runModeSet=('bkvkd' 'kd' 'kvkd')
#func

flengths=(100 400)
reqs=("100M" "25M")
flengths=(100)
reqs=("100M")
cacheSizes=(4096)
ops=("40M")
indexSet=(1 3 5 7 9)
runModeSet=('kv' 'raw' 'bkv' 'bkvkd' 'kd' 'kvkd')
runModeSet=('raw')
bonus=""
#runModeSet=('bkvkd' 'kd' 'kvkd')
#runModeSet=('bkvkd')
#func
#
#bonus="rmw"
#func



ExpName="_p34_motivation"
bonus=""
indexSet=(1 3 5 7 9)
ops=("50M")
runModeSet=('raw')
checkrepeat="checkrepeat"
#func
#
#bonus="rmw"
#func

#indexSet=(5)
#runModeSet=('bkv')
##bonus="rmw"
#func
#
#bonus=""
#func

indexSet=(1 3 5 7 9)
runModeSet=('bkv' 'kv')
func

bonus="rmw"
func

bonus="rmw"
ExpName="_p35_exp1_ycsb"
indexSet=(5 95 10 5) # A, B, C, F
ops=("20M")
runModeSet=('bkv' 'kv' 'raw')
checkrepeat=""
func

ops=("50M")
runModeSet=('bkv' 'kv' 'raw')
func
exit

ExpName="_p33_motivation_buffer2M"
bonus="rmw"
indexSet=(5)
runModeSet=('bkv')
func

bonus=""
func

ops=("40M")
indexSet=(1 3 5 7 9)
func

runModeSet=('raw')
func

bonus="rmw"
func
