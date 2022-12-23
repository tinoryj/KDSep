runModeSet=('kvkd' 'kv' 'bkv' 'raw')
for runMode in "${runModeSet[@]}"; do
    threadNumber=15
    if [[ $runMode == "kv" ]]; then
        threadNumber=14
    elif [[ $runMode == "kvkd" ]]; then
        threadNumber=4
    elif [[ $runMode == "kd" ]]; then
        threadNumber=5
    fi
    # other threads for usage: batched, writeback+mergeGC in deltaStore; KV-1; KD-worker+gc=8
    # scripts/runTest.sh load $runMode req10M op10M fc10 fl400 cache1024 threads$threadNumber round1 Exp3Batched

    # scripts/runTest.sh $runMode req10M op5M fc10 fl400 cache1024 threads$threadNumber round1 readRatio0.5 Exp3Batched

    # scripts/runTest.sh load $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber round1 Exp3Batched

    indexSet=(1 3 5 7 9)

    for index in "${indexSet[@]}"; do
        scripts/runTest.sh $runMode req40M op5M fc10 fl100 cache1024 threads$threadNumber round1 readRatio0.$index Exp3Batched
    done
done