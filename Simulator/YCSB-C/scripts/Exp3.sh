#!/bin/bash

scripts/runTest.sh load kvkd req10M op10M fc10 fl400 cache1024 threads8 round1
scripts/runTest.sh load kv req10M op10M fc10 fl400 cache1024 threads14 round1
scripts/runTest.sh load req10M op10M fc10 fl400 cache1024 threads15 round1
scripts/runTest.sh load bkv req10M op10M fc10 fl400 cache1024 threads15 round1
# scripts/runTest.sh load kd req10M op10M fc10 fl400 cache1024 threads10 round1

scripts/runTest.sh kvkd req10M op5M fc10 fl400 cache1024 threads8 round1 readRatio0.5
scripts/runTest.sh kv req10M op5M fc10 fl400 cache1024 threads14 round1 readRatio0.5
# scripts/runTest.sh kd req10M op5M fc10 fl400 cache1024 threads16 round1 readRatio0.5
scripts/runTest.sh bkv req10M op5M fc10 fl400 cache1024 threads15 round1 readRatio0.5
scripts/runTest.sh req10M op5M fc10 fl400 cache1024 threads15 round1 readRatio0.5

scripts/runTest.sh load kvkd req40M op5M fc10 fl100 cache1024 threads8 round1
scripts/runTest.sh load kv req40M op5M fc10 fl100 cache1024 threads14 round1
scripts/runTest.sh load req40M op5M fc10 fl100 cache1024 threads15 round1
scripts/runTest.sh load bkv req40M op5M fc10 fl100 cache1024 threads15 round1
# scripts/runTest.sh load kd req40M op5M fc10 fl100 cache1024 threads13 round1

indexSet=(1 3 5 7 9)

for index in "${indexSet[@]}"; do
scripts/runTest.sh kvkd req40M op5M fc10 fl100 cache1024 threads8 round1 readRatio0.$index
scripts/runTest.sh kv req40M op5M fc10 fl100 cache1024 threads14 round1 readRatio0.$index
scripts/runTest.sh req40M op5M fc10 fl100 cache1024 threads15 round1 readRatio0.$index
scripts/runTest.sh bkv req40M op5M fc10 fl100 cache1024 threads15 round1 readRatio0.$index
# scripts/runTest.sh kd req40M op5M fc10 fl100 cache1024 threads15 round1 readRatio0.$index
done