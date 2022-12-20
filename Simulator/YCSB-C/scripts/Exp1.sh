#!/bin/bash
scripts/runTestWithYCSB.sh load kvkd req10M op10M fc10 fl400 cache1024 threads12 round1
scripts/runTestWithYCSB.sh kvkd req10M op10M fc10 fl400 cache1024 threads12 round1
scripts/runTestWithYCSB.sh load kd req10M op10M fc10 fl400 cache1024 threads13 round1
scripts/runTestWithYCSB.sh kd req10M op10M fc10 fl400 cache1024 threads13 round1
scripts/runTestWithYCSB.sh load kv req10M op10M fc10 fl400 cache1024 threads15 round1
scripts/runTestWithYCSB.sh kv req10M op10M fc10 fl400 cache1024 threads15 round1
scripts/runTestWithYCSB.sh load bkv req10M op10M fc10 fl400 cache1024 threads15 round1
scripts/runTestWithYCSB.sh bkv req10M op10M fc10 fl400 cache1024 threads15 round1
scripts/runTestWithYCSB.sh load req10M op10M fc10 fl400 cache1024 threads16 round1
scripts/runTestWithYCSB.sh req10M op10M fc10 fl400 cache1024 threads16 round1
