#!/bin/bash
scripts/runTest.sh load kvkd req10M op10M fc10 fl400 cache1 threads12 round1
scripts/runTest.sh kvkd req10M op10M fc10 fl400 cache1 threads12 round1
scripts/runTest.sh load kd req10M op10M fc10 fl400 cache1 threads13 round1
scripts/runTest.sh kd req10M op10M fc10 fl400 cache1 threads13 round1
scripts/runTest.sh load kv req10M op10M fc10 fl400 cache1 threads15 round1
scripts/runTest.sh kv req10M op10M fc10 fl400 cache1 threads15 round1
scripts/runTest.sh load bkv req10M op10M fc10 fl400 cache1 threads15 round1
scripts/runTest.sh bkv req10M op10M fc10 fl400 cache1 threads15 round1
scripts/runTest.sh load req10M op10M fc10 fl400 cache1 threads16 round1
scripts/runTest.sh req10M op10M fc10 fl400 cache1 threads16 round1
