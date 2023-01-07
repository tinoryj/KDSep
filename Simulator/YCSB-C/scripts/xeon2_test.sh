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

indexSet=(5 9)
indexSet=(9)
runModeSet=('raw')
blocksizes=(4096)
flengths=(100)
reqs=("40M")
cacheSizes=(1024 2048 2048 2048 4096 4096 4096 4096 4096 4096 4096)
blobCacheSizes=(128 1536 1024 512 3584 3072 2560 2048 1536 1048 512)
kdcacheSizes=(128 256 512  128  256  512  1024 1536 2048 2560)

cacheSizes=(1024 1024 2048 2048 4096 4096 4096)
kdcacheSizes=(128 256 256 512 256 512 1024)

for bs in "${blocksizes[@]}"; do
    for ((j=0; j<${#flengths[@]}; j++)); do
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
	    for index in "${indexSet[@]}"; do
		lastCacheSize=0
		for ((k=0; k<${#cacheSizes[@]}; k++)); do
		    cacheSize=${cacheSizes[$k]}
		    blobCacheSize=${blobCacheSizes[$k]}
		    blockCacheSize=$(( $cacheSize - $blobCacheSize ))
		    bucketNumber=$(echo "( 500000 * (10 - $index) * 138 ) / 262144 / 0.5"|bc)
		    ratio="0.$index"
		    if [[ $index -eq 10 ]]; then
			ratio="1"
		    fi

#                    scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize threads$threadNumber workerT$works gcT$gcs batchSize$batchSize readRatio$ratio bucketNum$bucketNumber Exp$ExpName blockSize${bs}
                    if [[ "$runMode" == "raw" ]]; then
                        if [[ "$lastCacheSize" -ne "$cacheSize" ]]; then
			    kvcacheSize=$(( ${cacheSize} / 2 ))
			    cacheSize=$(( ${cacheSize} / 2 ))
                            scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize kvcache${kvcacheSize} threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif 
			fi
                        lastCacheSize=$(( $cacheSize * 2 ))
                    elif [[ "$runMode" == "bkv" ]]; then
                        scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$blockCacheSize blobcache${blobCacheSize} threads$threadNumber readRatio$ratio Exp$ExpName blockSize${bs} cif 
                    elif [[ "$runMode" == "kd" ]]; then
			kdcacheSize=${kdcacheSizes[$k]}
			kvcacheSize=$(( ${cacheSize} / 2 - $kdcacheSize ))
			cacheSize=$(( ${cacheSize} - $kdcacheSize - $kvcacheSize ))
                        scripts/run.sh $runMode req${req} op10M fc10 fl${fl} cache$cacheSize kdcache${kdcacheSize} kvcache${kvcacheSize} \
			    threads$threadNumber workerT$works gcT$gcs bucketNum$bucketNumber readRatio$ratio Exp$ExpName blockSize${bs} cif 
                    fi
		done
            done
            scripts/run.sh $runMode req${req} op30M fc10 fl${fl} cache8192 threads$threadNumber workerT$works gcT$gcs batchSize$batchSize Exp$ExpName blockSize${bs} clean 
        done
    done
done
