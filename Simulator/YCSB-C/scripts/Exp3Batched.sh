#!/bin/bash

runModeSet=('kvkd')
for runMode in "${runModeSet[@]}"; do
    threadNumber=15
    if [[ $runMode == "kvkd" ]]; then
        threadNumber=8
    fi
    workerThreadSet=(16 24 32)
    gcThreadSet=(6 8)
    indexSet=(1 3 5 7 9)
    for works in "${workerThreadSet[@]}"; do
        for gcs in "${gcThreadSet[@]}"; do
            for index in "${indexSet[@]}"; do
                scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber workerT$works gcT$gcs batchSize2K round1 readRatio0.$index ExpParameters
            done
        done
    done
done
