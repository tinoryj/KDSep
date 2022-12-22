#!/bin/bash

runModeSet=('kvkd' 'kv')
for runMode in "${runModeSet[@]}"; do
    scripts/runTest.sh load $runMode req10M op10M fc10 fl400 cache1024 threads8 round1

    scripts/runTest.sh $runMode req10M op5M fc10 fl400 cache1024 threads8 round1 readRatio0.5

    scripts/runTest.sh load $runMode req40M op5M fc10 fl100 cache1024 threads8 round1

    indexSet=(1 3 5 7 9)

    for index in "${indexSet[@]}"; do
        scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads8 round1 readRatio0.$index
    done
done