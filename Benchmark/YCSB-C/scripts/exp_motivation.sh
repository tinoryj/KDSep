#!/bin/bash

source scripts/common.sh

#### 0. Motivation

ExpName="_motivation"
readRatios=(1 3 5 7 9)
op="50M"
runModeSet=('raw')
bonus2="rmw"
func

bonus2=""
func

readRatios=(5)
runModeSet=('bkv')
bonus2="rmw"
func

bonus2=""
func
