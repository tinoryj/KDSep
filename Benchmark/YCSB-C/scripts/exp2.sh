#!/bin/bash

source scripts/common.sh

#### 2. UP2X workloads 

ExpName="_exp2_up2x"
req="1000M" # 1 billion KV pairs
op="200M"

## RocksDB, BlobDB, vLogDB
runModeSet=('raw' 'bkv' 'kv')
bonus4="up2x"
func

## Add KDSep 
cacheSize=3584
runModeSet=('kd' 'bkvkd' 'kvkd')
func
