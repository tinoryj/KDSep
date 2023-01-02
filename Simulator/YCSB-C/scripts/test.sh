#!/bin/bash
ExpName=2
works=24
gcs=8
indexSet=(1 3 5 7 9 10)
runModeSet=('raw' 'bkv')

blocksizes=(16384 8192 4096 2048 1024)
flengths=(800 400 200 100)
reqs=("10M" "20M" "40M" "80M")
batchSize=10K

indexSet=(10)
runModeSet=('raw' 'bkv')
blocksizes=(4096)
flengths=(100)
reqs=("40M")
cacheSizes=(4096 4096 4096 4096 4096 4096 4096)
blobCacheSizes=(3584 3072 2560 2048 1536 1048 512)
cacheSizes=(2048 2048 2048)
blobCacheSizes=(1536 1024 512)

for bs in "${blocksizes[@]}"; do
    for ((j=0; j<${#flengths[@]}; j++)); do
	for runMode in "${runModeSet[@]}"; do
	    threadNumber=45
	    threadNumber=4
	    if [[ $runMode == "bkvkd" ]]; then
		threadNumber=9
	    elif [[ $runMode == "kd" ]]; then
		threadNumber=9
	    fi

	    fl=${flengths[$j]}
	    req=${reqs[$j]}
            lastCacheSize=0
	    for ((k=0; k<${#cacheSizes[@]}; k++)); do
                cacheSize=${cacheSizes[$k]}
                blobCacheSize=${blobCacheSizes[$k]}
                blockCacheSize=$(( $cacheSize - $blobCacheSize ))
		for index in "${indexSet[@]}"; do
		    bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
		    ratio="0.$index"
		    if [[ $index -eq 10 ]]; then
			ratio="1"
		    fi

#                    scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize threads$threadNumber workerT$works gcT$gcs batchSize$batchSize readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${bs}
                    if [[ "$runMode" == "raw" ]]; then
                        if [[ "$lastCacheSize" -ne "$cacheSize" ]]; then
#                            scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif 
                            scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif nodirect nommap 
                        else
                            continue
                        fi
                        lastCacheSize=$cacheSize
                    elif [[ "$runMode" == "bkv" ]]; then
#                        scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$blockCacheSize blobcache${blobCacheSize} threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif 
                        scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$blockCacheSize blobcache${blobCacheSize} threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif nodirect nommap 
                    fi
		done
            done
#            scripts/run.sh $runMode req${req} op30M fc10 fl${fl} cache8192 threads$threadNumber workerT$works gcT$gcs batchSize$batchSize Exp$ExpName blockSize${bs} clean 
        done
    done
done
