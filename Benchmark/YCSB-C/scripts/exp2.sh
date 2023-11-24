#!/bin/bash

source scripts/common.sh

#### 2. UP2X workloads 

ExpName="_exp2_up2x"
req="1000M" # 1 billion KV pairs
op="200M"
bonus2="up2x"

## RocksDB, BlobDB, vLogDB
runModeSet=('raw' 'kd' 'bkv' 'bkvkd' 'kv' 'kvkd')
func
