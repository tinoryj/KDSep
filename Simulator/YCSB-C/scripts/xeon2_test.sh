#!/bin/bash
ExpName=2
works=24
gcs=8
indexSet=(1 3 5 7 9 10)
runModeSet=('raw' 'bkv')

indexSet=(1 5 10)
cacheSizes=(2048 2048 2048 4096 4096 4096 4096 4096 4096 4096 1024 1024 1024 1024)
blobCacheSizes=(1536 1024 512 3584 3072 2560 2048 1536 1048 512 512 256 128 64)

indexSet=(10)
blocksizes=(65536 32768)
flengths=(400)
reqs=("25M")
sstSizes=(8)
runModeSet=('bkv' 'kv' 'raw')
op="10M"
cacheSizes=(2048 4096 1024)
blobCacheSizes=(2048 4096 1024)
bfs=(10)
rounds=2
# memSizes the same

for bs in "${blocksizes[@]}"; do
    for ((j=0; j<${#flengths[@]}; j++)); do
        for ((si=0; si<${#sstSizes[@]}; si++)); do
	    for ((bfi=0; bfi<${#bfs[@]}; bfi++)); do
		sst=${sstSizes[$si]}
		memtable=${sst}
		l1sz=$(( ${sst} * 4 ))
		bf=${bfs[$bfi]}

		for runMode in "${runModeSet[@]}"; do
		    threadNumber=45
		    threadNumber=4
		    if [[ $runMode == "bkvkd" ]]; then
			threadNumber=9
		    elif [[ $runMode == "kd" ]]; then
			threadNumber=9
		    elif [[ $runMode == "kvkd" ]]; then
			threadNumber=9
		    fi

		    fl=${flengths[$j]}
		    req=${reqs[$j]}

		    for ((roundi=1; roundi<=${rounds}; roundi++)); do
			for index in "${indexSet[@]}"; do
			    for ((k=0; k<${#cacheSizes[@]}; k++)); do
				cacheSize=${cacheSizes[$k]}
				blobCacheSize=${blobCacheSizes[$k]}
				blockCacheSize=$(( $cacheSize - $blobCacheSize ))
				bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
				ratio="0.$index"
				if [[ $index -eq 10 ]]; then
				    ratio="1"
				fi

				if [[ "$runMode" == "raw" ]]; then
				    scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
					cache$cacheSize \
					threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} bf${bf} cif #checkrepeat # paretokey 
				elif [[ "$runMode" == "bkv" ]]; then
				    scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
					cache$cacheSize \
					threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} bf${bf} cif # checkrepeat #paretokey
				elif [[ "$runMode" == "kv" ]]; then
				    scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
					cache$cacheSize \
					threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} bf${bf} cif #checkrepeat #paretokey
				elif [[ "$runMode" == "kd" ]]; then
				    if [[ $ratio -eq 1 ]]; then
					continue
				    fi
				    kvcacheSize=$(( ${cacheSize} / 2 ))
				    kdcacheSize=$(( ${cacheSize} / 4 ))
				    cacheSize=$(( ${cacheSize} / 4 ))
				    scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
					cache$cacheSize kvcache${kvcacheSize} kdcache${kdcacheSize} \
					threads$threadNumber workerT$works gcT$gcs bucketNum$bucketNumber \
					readRatio$ratio Exp$ExpName blockSize${bs} bf${bf} cif #paretokey
				fi
			    done
			done
		    done
		scripts/run.sh $runMode req${req} op${op} fc10 fl${fl} sst${sst} memtable${memtable} l1sz${l1sz} \
		    cache$cacheSize \
		    readRatio$ratio Exp$ExpName blockSize${bs} bf${bf} cif clean 
		done
	    done
        done
    done
done
