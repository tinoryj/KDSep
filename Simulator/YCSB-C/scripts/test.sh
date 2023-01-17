#!/bin/bash
ExpName=2
works=24
gcs=8
indexSet=(1 3 5 7 9 10)
runModeSet=('raw' 'bkv')

blocksizes=(16384 8192 4096 2048 1024)
flengths=(800 400 200 100)
reqs=("10M" "20M" "40M" "80M")
batchSize=10K

indexSet=(1 5 10)
indexSet=(10)
runModeSet=('bkv' 'kv' 'raw' 'kd')
runModeSet=('raw' 'kv' 'bkv')
runModeSet=('raw')
blocksizes=(4096)
flengths=(100)
reqs=("40M")
op="10M"
cacheSizes=(2048 2048 2048 4096 4096 4096 4096 4096 4096 4096 1024 1024 1024 1024)
blobCacheSizes=(1536 1024 512 3584 3072 2560 2048 1536 1048 512 512 256 128 64)
cacheSizes=(2048)
blobCacheSizes=(1024)
cacheSizes=(0)
blobCacheSizes=(0)
sstSizes=(64)
sstSizes=(4 8 16 32)
# memSizes the same

for bs in "${blocksizes[@]}"; do
    for ((j=0; j<${#flengths[@]}; j++)); do
        for ((si=0; si<${#sstSizes[@]}; si++)); do
            sst=${sstSizes[$si]}
            memtable=${sst}
            l1sz=$(( ${sst} * 4 ))

            for runMode in "${runModeSet[@]}"; do
                threadNumber=45
                threadNumber=4
                if [[ $runMode == "bkvkd" ]]; then
                    threadNumber=9
                elif [[ $runMode == "kd" ]]; then
                    threadNumber=9
                elif [[ $runMode == "kvkd" ]]; then
                    threadNumber=9
                fi

                fl=${flengths[$j]}
                req=${reqs[$j]}
                for index in "${indexSet[@]}"; do
                    lastCacheSize=999999999
                    for ((k=0; k<${#cacheSizes[@]}; k++)); do
                        cacheSize=${cacheSizes[$k]}
                        blobCacheSize=${blobCacheSizes[$k]}
                        blockCacheSize=$(( $cacheSize - $blobCacheSize ))
                        bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
                        ratio="0.$index"
                        if [[ $index -eq 10 ]]; then
                            ratio="1"
                        fi

#                    scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} cache$cacheSize threads$threadNumber workerT$works gcT$gcs batchSize$batchSize readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${bs}
                        if [[ "$runMode" == "raw" ]]; then
                            if [[ "$lastCacheSize" -ne "$cacheSize" ]]; then
#                                kvcacheSize=$(( ${cacheSize} / 2 ))
#                                cacheSize=$(( $cacheSize / 2 ))
#                                scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
#                                    cache$cacheSize kvcache${kvcacheSize} \
#                                    threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif #checkrepeat # paretokey 
                                scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                    cache$cacheSize \
                                    threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif #checkrepeat # paretokey 
                                cacheSize=$(( $cacheSize * 2 ))
                            fi
                            lastCacheSize=$cacheSize
                        elif [[ "$runMode" == "bkv" ]]; then
#                            scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
#                                cache$blockCacheSize kvcache${blobCacheSize} \
#                                threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif # checkrepeat #paretokey
                            scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                cache$cacheSize \
                                threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif # checkrepeat #paretokey
                        elif [[ "$runMode" == "kv" ]]; then
#                            kvcacheSize=$(( ${cacheSize} / 2 ))
#                            cacheSize=$(( ${cacheSize} - $kvcacheSize ))
#                            scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
#                                cache$cacheSize kvcache${kvcacheSize} \
#                                threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif #checkrepeat #paretokey
                            scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                cache$cacheSize \
                                threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif #checkrepeat #paretokey
                        elif [[ "$runMode" == "kd" ]]; then
                            if [[ $ratio -eq 1 ]]; then
                                continue
                            fi
                            kvcacheSize=$(( ${cacheSize} / 2 ))
                            kdcacheSize=$(( ${cacheSize} / 4 ))
                            cacheSize=$(( ${cacheSize} / 4 ))
                            scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
                                cache$cacheSize kvcache${kvcacheSize} kdcache${kdcacheSize} \
                                threads$threadNumber workerT$works gcT$gcs bucketNum$bucketNumber \
                                readRatio$ratio Exp$ExpName blockSize${bs} cif #paretokey
                        fi
                    done
                done
#            scripts/run.sh $runMode req${req} op30M fc10 fl${fl} cache8192 threads$threadNumber workerT$works gcT$gcs batchSize$batchSize Exp$ExpName blockSize${bs} clean 
            done
        done
    done
done
