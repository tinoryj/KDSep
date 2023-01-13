#!/bin/bash
ExpName=0
works=24
gcs=8
ratioSet=(1)
runModeSet=('kvkd' 'raw' 'kvkd')
opNumber=40
fieldlen=100
blocksizes=4096
allCacheSize=2048
bucketSize=262144
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
            kvcacheSize=$(((${allCacheSize} / 10) * 9))
            cacheSize=$((${allCacheSize} - $kvcacheSize))
            scripts/run.sh $runMode req40M op${opNumber}M fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} threads$threadNumber batchSize10K readRatio$ratio Exp$ExpName blockSize${blocksizes} cif
        elif [[ "$runMode" == "kvkd" ]]; then
            kvcacheSize=$(((${allCacheSize} / 10) * 8))
            kdcacheSize=$((${allCacheSize} / 10))
            cacheSize=$((${allCacheSize} / 10))
            bucketNumber=$(echo "( ${opNumber} * 100000 * (10 - $ratio) * 138 ) / $bucketSize / 0.5" | bc)
            scripts/run.sh $runMode req40M op${opNumber}M fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} kdcache${kdcacheSize} threads$threadNumber batchSize10K workerT$works gcT$gcs bucketSize$bucketSize readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${blocksizes} cif #paretokey
            exit
        elif [[ "$runMode" == "kd" ]]; then
            kvcacheSize=$(((${allCacheSize} / 10) * 8))
            kdcacheSize=$((${allCacheSize} / 10))
            cacheSize=$((${allCacheSize} / 10))
            bucketNumber=$(echo "( ${opNumber} * 100000 * (10 - $ratio) * 138 ) / $bucketSize / 0.5" | bc)
            scripts/run.sh $runMode req40M op${opNumber}M fc10 fl${fieldlen} cache$cacheSize kvcache${kvcacheSize} kdcache${kdcacheSize} threads$threadNumber batchSize10K workerT$works gcT$gcs bucketSize$bucketSize bucketNum$bucketNumber readRatio$ratio Exp$ExpName blockSize${blocksizes} cif #paretokey
        fi
    done
done
