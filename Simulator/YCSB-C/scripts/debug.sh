#!/bin/bash

func() {
    for bs in "${blocksizes[@]}"; do
        for ((j=0; j<${#flengths[@]}; j++)); do
            for ((si=0; si<${#sstSizes[@]}; si++)); do
                sst=${sstSizes[$si]}
                memtable=$(( mtSizes[$si] ))
                l1sz=$(( l1Sizes[$si] ))

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
                                    bucketNumber=$maxBucketNumber
                                    ratio="0.$index"
                                    if [[ $index -eq 10 ]]; then
                                        ratio="1"
                                    fi
                                    echo $runMode $ratio

                                    if [[ "$ratio" == "1" && $runMode =~ "kd" ]]; then
                                        continue
                                    fi

                                    if [[ "$runMode" == "raw" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "bkvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( $kdc ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} $checkrepeat #paretokey
                                    elif [[ "$runMode" == "kvkd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( $kdc ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchTestNum${batchTestNum} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} #paretokey
# gcThres0.6 splitThres0.3
                                    elif [[ "$runMode" == "kd" ]]; then
                                        if [[ "$ratio" == "1" ]]; then
                                            bucketNumber=1024 
                                        fi
                                        kdcacheSize=$(( $kdc ))
                                        blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
                                        if [[ "$bonus" == "rmw" ]]; then
                                            kdcacheSize=$(( ${cacheSize} - 1 ))
                                            blockCacheSize=$(( 1 ))
                                        fi
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$blockCacheSize kdcache${kdcacheSize} \
                                            threads$threadNumber workerT$works gcT$gcs bn$bucketNumber splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} ${bonus6} ${bonus5} mem${mem} ${bonus} ${bonus4} ${bonus2} ${bonus3} #paretokey
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
fcl=10
batchTestNum=10k
indexSet=(1 3 5 7 9 10)

indexSet=(1 5 10)
cacheSizes=(2048 2048 2048 4096 4096 4096 4096 4096 4096 4096 1024 1024 1024 1024)

indexSet=(5 1)
blocksizes=(65536)
reqs=("25M")
sstSizes=(16)
mtSizes=(64)
l1Sizes=(64)

cacheSizes=(4096)
indexSet=(0 5 1 10)
# memSizes the same


### Base test!!!
cacheSizes=(2048)
indexSet=(1 3 5 7 9 10)
ops=("10M")
ops=("20M")
reqs=("40M" "10M" "100M" "25M")
reqs=("100M" "25M")

bonus="rmw"

indexSet=(1 3 5 7 9)
reqs=("100M")
ops=("30M")

bonus=""
indexSet=(1)
works=8
splitThres=0.8
gcWriteBackSize=500


sstSizes=(8)
mtSizes=(8)
l1Sizes=(32)

sstSizes=(16)
mtSizes=(64)
l1Sizes=(256)

kdc=512
mem=""

ExpName="_d4"
flengths=("100" "400")
reqs=("100M" "25M")
flengths=("400")
reqs=("25M")
flengths=("100")
reqs=("10M")
cacheSizes=(3072 4096 5120 6144)
cacheSizes=(4096)
ops=("50M")
indexSet=(1)
runModeSet=('kv' 'bkv' 'raw' 'kvkd' 'bkvkd' 'kd')
runModeSet=('kvkd')
if [[ $(diff ycsbc ycsbc_debug | wc -l ) -eq 1 ]]; then
    bonus="noterelease"
else
    bonus="notedebug"
fi
#maxBucketNumber=128
maxBucketNumber=32768
bonus2="initBit7"
#bonus3="workloade"
bonus3="ec"
bonus3="di"
bonus4="nodirectreads"

bonus2=""
bonus3=""
bonus4="" # "shortprepare"
gcWriteBackSize=100000
checkrepeat=""
fcl=10
kdc=64

##################### Part 2
flengths=(100)
reqs=("10M")
ops=("10M")

checkrepeat=""
maxBucketNumber=32768
runModeSet=('raw' 'bkv')
runModeSet=('kd')
bonus2=""
bonus3="initBit8"
bonus4="shortprepare"
bonus5=""
bonus6=""
indexSet=(1) 
func

#bonus6=""
#func
