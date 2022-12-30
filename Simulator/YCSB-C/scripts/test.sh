#!/bin/bash
ExpName=2
works=24
gcs=8
indexSet=(1 3 5 7 9 10)
indexSet=(10)
runModeSet=('raw' 'bkv')

blocksizes=(16384 8192 4096 2048 1024)
flengths=(800 400 200 100)
reqs=("10M" "20M" "40M" "80M")
batchSize=10K
cacheSizes=(8192 4096 2048 1024)

for bs in "${blocksizes[@]}"; do
    for ((j=0; j<${#flengths[@]}; j++)); do
	for runMode in "${runModeSet[@]}"; do
	    threadNumber=45
	    if [[ $runMode == "bkvkd" ]]; then
		threadNumber=9
	    elif [[ $runMode == "kd" ]]; then
		threadNumber=9
	    fi

	    fl=${flengths[$j]}
	    req=${reqs[$j]}
	    scripts/run.sh $runMode req${req} op5M fc10 fl${fl} cache8192 threads$threadNumber workerT$works gcT$gcs batchSize$batchSize round1 Exp$ExpName blockSize${bs} load
	    for cacheSize in "${cacheSizes[@]}"; do
		for index in "${indexSet[@]}"; do
		    bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
		    ratio="0.$index"
		    if [[ $index -eq 10 ]]; then
			ratio="1"
		    fi
		    if [[ $bs -eq 16384 && $fl -eq 800 && $cacheSize -eq 4096 ]]; then
			continue
		    fi
		    scripts/run.sh $runMode req${req} op5M fc10 fl${fl} cache$cacheSize threads$threadNumber workerT$works gcT$gcs batchSize$batchSize round1 readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${bs}
		done
	    done
	    scripts/run.sh $runMode req${req} op5M fc10 fl${fl} cache8192 threads$threadNumber workerT$works gcT$gcs batchSize$batchSize round1 Exp$ExpName blockSize${bs} clean 
	done
    done
done
