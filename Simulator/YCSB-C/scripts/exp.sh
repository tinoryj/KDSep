#!/bin/bash
ExpName=2
works=24
    gcs=8
indexSet=(10 1 3 5 7 9)
    runModeSet=('raw')
    for runMode in "${runModeSet[@]}"; do
    threadNumber=16 # 45
    if [[ $runMode == "bkvkd" ]]; then
        threadNumber=9
    elif [[ $runMode == "kd" ]]; then
        threadNumber=9
    elif [[ $runMode == "kvkd" ]]; then
        threadNumber=8
    elif [[ $runMode == "kv" ]]; then
        threadNumber=44
    fi
    for index in "${indexSet[@]}"; do
        bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
        if [[ $index -eq 10 ]]; then
            scripts/runTestNew.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber workerT$works gcT$gcs batchSize2K round1 readRatio1 bucketNum$bucketNumber noteNocache 
        else
            scripts/runTestNew.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber workerT$works gcT$gcs batchSize2K round1 readRatio0.$index bucketNum$bucketNumber noteNocache # Exp$ExpName
        fi
    done
done
