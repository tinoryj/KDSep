#!/bin/bash

runModeSet=('kvkd' 'kv' 'bkv' 'raw' 'kd')
for runMode in "${runModeSet[@]}"; do
    threadNumber=16
    if [[ $runMode == "kv" ]]; then
        threadNumber=15
    elif [[ $runMode == "kvkd" ]]; then
        threadNumber=12
    elif [[ $runMode == "kd" ]]; then
        threadNumber=13
    fi

    scripts/runTest.sh load $runMode req10M op10M fc10 fl400 cache1024 threads$threadNumber round1 Exp3NoBatch

    scripts/runTest.sh $runMode req10M op5M fc10 fl400 cache1024 threads$threadNumber round1 readRatio0.5 Exp3NoBatch

    scripts/runTest.sh load $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber round1 Exp3NoBatch

    indexSet=(1 3 5 7 9)

    for index in "${indexSet[@]}"; do
        scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber round1 readRatio0.$index Exp3NoBatch
    done
done