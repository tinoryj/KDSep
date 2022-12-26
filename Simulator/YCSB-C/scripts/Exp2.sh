#!/bin/bash
ExpName=2
works=24
gcs=8
indexSet=(1 3 5 7 9)
runModeSet=('kvkd' 'kv' 'kd' 'raw' 'bkv' )
for runMode in "${runModeSet[@]}"; do
    threadNumber=45
    if [[ $runMode == "kvkd" ]]; then
        threadNumber=8
    elif [[ $runMode == "kd" ]]; then
        threadNumber=9
    fi
    for index in "${indexSet[@]}"; do
        bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
        scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber workerT$works gcT$gcs batchSize2K round1 readRatio0.$index bucketNum$bucketNumber Exp$ExpName
    done
done

#  scripts/runTest.sh kvkd req40M op1M fc10 fl100 cache1024 threads8 workerT24 gcT8 batchSize2K round1 readRatio0.1 bucketNum50 Exp2