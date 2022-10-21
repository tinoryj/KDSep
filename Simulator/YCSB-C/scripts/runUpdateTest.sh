#!/bin/bash
pwd

if [ ! -d "ResultLogs" ]; then
    mkdir -p  ResultLogs
fi

for i in {1..9}
do 
    for multiRun in {1..10}
    do
        let UpdateProportion=10-i
        echo "Modify spec, Read/Update ratio = $i:$UpdateProportion"
        cp workloads/workloada-test.spec ./workloada-temp.spec
        sed -i "15s/0.5/0.$i/g" workloada-temp.spec
        sed -i "16s/0.5/0.$UpdateProportion/g" workloada-temp.spec
        echo "Modified spec file content =======>"
        cat workloada-temp.spec | head -n 16 | tail -n 2

        # Running the ycsb-benchmark
        make clean
        make
        echo "Loading the database =====================>"
        ./ycsbc -db rocksdb -dbfilename dbtest -threads 16 -P workloada-temp.spec  -phase load  -configpath configDir/leveldb_config.ini
        echo "Benchmark the database =====================>"
        ./ycsbc -db rocksdb -dbfilename dbtest -threads 16 -P workloada-temp.spec  -phase load  -configpath configDir/leveldb_config.ini > ./ResultLogs/Read-0.$i-Update-0.$UpdateProportion-Round-$multiRun.log
    done
done
