#!/bin/bash

source scripts/common.sh

#### 7. Vary k 

ExpName="_exp7_splitmerge"
runModeSet=('kvkd' 'bkvkd' 'kd')

#### 7.1 Disable split and merge 

bonus2="nosplit"
bonus5="nomerge"
func

#### 7.2 Enable split and merge 

bonus2=""
bonus5=""
func
