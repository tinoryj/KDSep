#!/bin/bash

works=8
rounds=1
batchSize=2
cacheSize=4096
splitThres=0.8
bucketNumber=32768
kdcacheSize=512
fcl=10
flength=100
req="100M"
op="50M"

#### 0. Motivation

ExpName="_motivation"
readRatios=(1 3 5 7 9)
op="50M"
runModeSet=('raw')
bonus="rmw"
func

bonus=""
func

readRatios=(5)
runModeSet=('bkv')
bonus="rmw"
func

bonus=""
func
