#!/bin/bash
scripts/runTest.sh load req40M op5M fc10 fl100 cache1024 threads16 round1 readRatio0.5
scripts/runTest.sh load bkv req40M op5M fc10 fl100 cache1024 threads16 round1 readRatio0.5

scripts/runTest.sh req40M op5M fc10 fl100 cache1024 threads16 round1 readRatio0.5
scripts/runTest.sh bkv req40M op5M fc10 fl100 cache1024 threads16 round1 readRatio0.5