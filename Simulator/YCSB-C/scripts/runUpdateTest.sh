#!/bin/bash
pwd
ResultLogFolder="ResultLogs"
DB_Name="dbtest"
Thread_number="1"
if [ ! -d $ResultLogFolder ]; then
    mkdir -p  $ResultLogFolder
fi

for multiRun in {1..10}
do

    for ReadProportion in {1..9}
    do 
        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
        let UpdateProportion=10-ReadProportion
        echo "Modify spec, Read/Update ratio = $ReadProportion:$UpdateProportion"
        cp workloads/workloada-test.spec ./workloada-temp.spec
        sed -i "15s/0.5/0.$ReadProportion/g" workloada-temp.spec
        sed -i "16s/0.5/0.$UpdateProportion/g" workloada-temp.spec
        echo "<===================== Modified spec file content =====================>"
        cat workloada-temp.spec | head -n 16 | tail -n 2

        # Running the ycsb-benchmark
        # make clean
        # make
        if [ -f=d $DB_Name ]; then
            rm -rf $DB_Name
            echo "Deleted old database folder"
        fi
        echo "<===================== Loading the database (Round $multiRun) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec  -phase load  -configpath configDir/leveldb_config.ini
        echo "<===================== Benchmark the database (Round $multiRun) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec  -phase run  -configpath configDir/leveldb_config.ini > $ResultLogFolder/Read-0.$ReadProportion-Update-0.$UpdateProportion-Round-$multiRun.log
        echo "<===================== Benchmark the database (Round $multiRun) done =====================>"
    done

    for ReadProportion in {1..9}
    do 
        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
        let RMWProportion=10-ReadProportion
        echo "Modify spec, Read/RMW ratio = $ReadProportion:$RMWProportion"
        cp workloads/workloada-test-RMW.spec ./workloada-temp.spec
        sed -i "15s/0.5/0.$ReadProportion/g" workloada-temp.spec
        sed -i "17s/0.5/0.$RMWProportion/g" workloada-temp.spec
        echo "<===================== Modified spec file content =====================>"
        cat workloada-temp.spec | head -n 17 | tail -n 3

        # Running the ycsb-benchmark
        # make clean
        # make
        if [ -f=d $DB_Name ]; then
            rm -rf $DB_Name
            echo "Deleted old database folder"
        fi
        echo "<===================== Loading the database (Round $multiRun) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec  -phase load  -configpath configDir/leveldb_config.ini
        echo "<===================== Benchmark the database (Round $multiRun) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec  -phase run  -configpath configDir/leveldb_config.ini > $ResultLogFolder/Read-0.$ReadProportion-RMW-0.$RMWProportion-Round-$multiRun.log
        echo "<===================== Benchmark the database (Round $multiRun) done =====================>"
    done

done