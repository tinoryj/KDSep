#!/bin/bash

runModeSet=('kvkd')
for runMode in "${runModeSet[@]}"; do
    threadNumber=15
    if [[ $runMode == "kvkd" ]]; then
        threadNumber=8
    fi
    workerThreadSet=(4 8 12 16)
    gcThreadSet=(1 2 3 4)
    batchSizeSet=(1 2 5 10)
    for works in "${workerThreadSet[@]}"; do
        for gcs in "${gcThreadSet[@]}"; do
            for batchSize in "${batchSizeSet[@]}"; do
                scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber workerT$works gcT$gcs batchSize${batchSize}K round1 readRatio0.1 ExpParameters
            done
        done
    done
done