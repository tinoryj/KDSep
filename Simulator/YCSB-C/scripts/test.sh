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
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # paretokey 
                                    elif [[ "$runMode" == "bkv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
                                            readRatio$ratio Exp$ExpName bs${bs} batchTestNum${batchTestNum} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat # load no_store #paretokey
                                    elif [[ "$runMode" == "kv" ]]; then
                                        scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                            cache$cacheSize \
                                            threads$threadNumber readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
                                            threads$threadNumber workerT$works gcT$gcs  bn$bucketNumber batchTestNum${batchTestNum} splitThres${splitThres} gcWriteBackSize${gcWriteBackSize} \
                                            readRatio$ratio Exp$ExpName bs${bs} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
                                            readRatio$ratio Exp$ExpName bs${bs} batchTestNum${batchTestNum} mem${mem} ${bonus} ${bonus5} ${bonus4} ${bonus3} ${bonus2} $checkrepeat #paretokey
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
batchTestNum=20k
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

bonus="workloade" # E
checkrepeat="checkrepeat"
runModeSet=('bkv' 'raw' 'kv')
ops=("1M")
#func

#### 1.1 Test: 16G memory, not cutting KD cache

bonus5="nodirectreads"
cutKDCache="false"
#checkrepeat=""

mem="16g"
reqs=("105M")

if [[ 1 -eq 2 ]]; then
    bonus="rmw"
    ExpName="_p35_exp1_test"
    indexSet=(5 95 10) # A, B, C, ignore F
    ops=("20M")
    runModeSet=('bkv' 'bkvkd' 'kv' 'kvkd' 'raw' 'kd')
    func

    indexSet=(5) 
    bonus="workloadd" # D 
    runModeSet=('bkv' 'bkvkd' 'kv' 'kvkd' 'raw' 'kd')
    func

    bonus="workloade" # E 
    ops=("1M")
    runModeSet=('bkv' 'raw' 'kv')
    func
    exit
fi
##exit

#func

#checkrepeat="scanThreads8"
#func

#checkrepeat="scanThreads32"
#func

cacheSizes=(4096)
ops=("20M")
###########################################################

#### 2. Performance
checkrepeat="checkrepeat"
bonus=""
indexSet=(1) 
ops=("100M")
ExpName="_p36_exp2_r10"
runModeSet=('bkv' 'raw' 'kv' 'bkvkd' 'kvkd' 'kd')
#func

### Test part
ExpName="_p36_test_part2"
ExpName="_p36_test_part3"
maxBucketNumber=32768
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

maxBucketNumber=16384
runModeSet=('bkvkd' 'kvkd' 'kd')
#func

runModeSet=('bkv' 'kv' 'raw')
mem=""
bonus2=""
bonus5=""
#func
#exit

checkrepeat=""
maxBucketNumber=16384

rs=('bkvkd' 'kvkd' 'kd')
runModeSet=('bkvkd' 'kvkd' 'kd')

mem=""
bonus2=""
bonus3="initBit10"
bonus4="ep"
bonus5="timeout7200"
splitThres=0.8
cacheSizes=(3584)

maxBucketNumber=32768
gcWriteBackSize=600
############ TODO
#func
#exit

#func

#for ((irs=0; irs<${#rs[@]}; irs++)); do
#    runModeSet=("${rs[$irs]}")
#    for ((igc=6; igc<=6; igc++)); do
#        gcWriteBackSize=$(( $igc * 100 ))
#        for ((ibn=8; ibn<=8; ibn++)); do
#            maxBucketNumber=$(( $ibn * 4096 ))
##            func
#            break
#        done
#    done
#done

mem=""
bonus2=""
frs=('kvkd' 'kd')

#for ((irs=0; irs<${#rs[@]}; irs++)); do
#    runModeSet=("${rs[$irs]}")
#    for ((igc=6; igc<=6; igc++)); do
#        gcWriteBackSize=$(( $igc * 100 ))
#        for ((ibn=6; ibn<=8; ibn++)); do
#            maxBucketNumber=$(( $ibn * 4096 ))
##            func
#        done
#    done
#done

runModeSet=('bkv' 'kv' 'raw')
cacheSizes=(4096)
#mem="16g"
#bonus2="nodirectreads"
#bonus5=""
#func

#mem="8g"
#bonus2="nodirectreads"
#bonus3="nommap"
#bonus4=""
#bonus5="nodirect"
#func
#exit

#checkrepeat="checkrepeat"

#### 4. value size

ExpName="_p37_exp4_test"
indexSet=(1) 
fcs=(10 20 40 80 160 320 640)
rreqs=("100M" "50M" "25M" "13M" "6M" "4M" "2M")

fcs=(20 40 80)
rreqs=("50M" "25M" "13M")

runModeSet=('bkv' 'raw' 'kv')
maxBucketNumber=32768
flengths=(100)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
#    func
done

runModeSet=('bkvkd' 'kvkd' 'kd') 
cacheSizes=(3584)

for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    reqs=(${rreqs[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
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
#    exit
done

# Test
maxBucketNumber=262144
for ((ri=0; ri<${#rreqs[@]}; ri++)); do
    fcl=${fcs[$ri]}
    flengths=(${fls[$ri]})
    gcWriteBackSize=$((${fcl} * ${flengths[0]} / 2))
#    func
done

############### 5. Read ratio

ExpName="_p38_exp5_ratio"
bonus5="timeout"

fcl=10
fls=(100)
flengths=(100)
reqs=("105M")
indexSet=(1 3 5 7 9) 

runModeSet=('kvkd' 'kd') 
cacheSizes=(3584)
maxBucketNumber=32768
func

runModeSet=('bkv' 'kv' 'raw') 
cacheSizes=(4096)
func

indexSet=(9) 
runModeSet=('bkvkd') 
cacheSizes=(3584)
maxBucketNumber=32768
func
