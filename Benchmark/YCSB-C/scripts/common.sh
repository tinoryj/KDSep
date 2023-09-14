#!/bin/bash

func() {
    for runMode in "${runModeSet[@]}"; do
	opnum=`echo $op | sed 's/M/000000/g' | sed 's/K/000/g'`
	for readRatio in "${readRatios[@]}"; do
	    ratio="0.$readRatio"
	    if [[ $readRatio -eq 10 ]]; then
		ratio="1"
	    fi

	    if [[ "$runMode" == "raw" ]]; then
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$cacheSize \
		    readRatio$ratio Exp$ExpName batchSize${batchSize} ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit} \
                    sst${sst} memtable${memtable} l1sz${l1sz}
	    elif [[ "$runMode" == "bkv" ]]; then
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$cacheSize \
		    readRatio$ratio Exp$ExpName batchSize${batchSize} ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit}
	    elif [[ "$runMode" == "bkvkd" ]]; then
		if [[ "$ratio" == "1" ]]; then
		    bucketNumber=1024 
		fi
		blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$blockCacheSize kdcache${kdcacheSize} \
		    workerT$works bn$bucketNumber splitThres${splitThres} \
		    readRatio$ratio Exp$ExpName batchSize${batchSize} ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit}
	    elif [[ "$runMode" == "kv" ]]; then
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$cacheSize \
		    readRatio$ratio Exp$ExpName batchSize${batchSize} ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit}
	    elif [[ "$runMode" == "kvkd" ]]; then
		if [[ "$ratio" == "1" ]]; then
		    bucketNumber=1024 
		fi
		blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$blockCacheSize kdcache${kdcacheSize} \
		    workerT$works bn$bucketNumber batchSize${batchSize} splitThres${splitThres} \
		    readRatio$ratio Exp$ExpName ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit} 
	    elif [[ "$runMode" == "kd" ]]; then
		if [[ "$ratio" == "1" ]]; then
		    bucketNumber=1024 
		fi
		blockCacheSize=$(( ${cacheSize} - $kdcacheSize ))
		scripts/run.sh $runMode req${req} op${op} fc${fcl} fl${flength} \
		    cache$blockCacheSize kdcache${kdcacheSize} \
		    workerT$works bn$bucketNumber splitThres${splitThres} \
		    readRatio$ratio Exp$ExpName batchSize${batchSize} ${bonus} ${bonus2} ${bonus5} ${bonus4} ${initBit}
	    fi
	done
    done
}
