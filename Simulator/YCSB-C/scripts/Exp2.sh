#!/bin/bash
# scripts/runTest.sh load kvkd req10M op10M fc10 fl400 cache1024 threads12 round1
# scripts/runTest.sh load kd req10M op10M fc10 fl400 cache1024 threads13 round1
# scripts/runTest.sh load kv req10M op10M fc10 fl400 cache1024 threads15 round1
# scripts/runTest.sh load bkv req10M op10M fc10 fl400 cache1024 threads16 round1
# scripts/runTest.sh load req10M op10M fc10 fl400 cache1024 threads16 round1
# exit

scripts/runTest.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1 readRatio0.1
scripts/runTest.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1 readRatio0.3
scripts/runTest.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1 readRatio0.5
scripts/runTest.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1 readRatio0.7
scripts/runTest.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1 readRatio0.9