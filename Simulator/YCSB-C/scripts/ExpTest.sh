#!/bin/bash
ExpName=2
works=24
gcs=8
ratioSet=(1)
runModeSet=('kd' 'raw' 'kvkd')
opNumber=40
fieldlen=100
blocksizes=4096
allCacheSize=1024
for runMode in "${runModeSet[@]}"; do
    threadNumber=45
    if [[ $runMode == "kvkd" ]]; then
        threadNumber=8
    elif [[ $runMode == "kd" ]]; then
        threadNumber=9
    fi
    for ratio in "${ratioSet[@]}"; do
        ratio="0.$ratio"
        if [[ "$runMode" == "raw" ]]; then
            cacheSize=${allCacheSize}
            scripts/run.sh $runMode req40M op${opNumber} fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} threads$threadNumber batchSize10K readRatio$ratio Exp$ExpName blockSize${blocksizes} cif
        elif [[ "$runMode" == "kdkv" ]]; then
            kvcacheSize=$(((${allCacheSize} / 10) * 9))
            cacheSize=$((${allCacheSize} - $kvcacheSize))
            bucketNumber=$(echo "( ${opNumber}000000 * (10 - $ratio) * 138 ) / 262144 / 0.5" | bc)
            scripts/run.sh $runMode req${req} op1M fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} threads$threadNumber batchSize10K workerT$works gcT$gcs readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${blocksizes} cif #paretokey
        elif [[ "$runMode" == "kd" ]]; then
            kvcacheSize=$(((${allCacheSize} / 10) * 8))
            kdcacheSize=$((${allCacheSize} / 10))
            cacheSize=$((${allCacheSize} - $kvcacheSize - $kdcacheSize))
            bucketNumber=$(echo "( ${opNumber}000000 * (10 - $ratio) * 138 ) / 262144 / 0.5" | bc)
            scripts/run.sh $runMode req${req} op10M fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} kdcache${kdcacheSize} threads$threadNumber batchSize10K workerT$works gcT$gcs bucketNum$bucketNumber readRatio$ratio Exp$ExpName blockSize${bs} cif #paretokey
        fi
    done
done
