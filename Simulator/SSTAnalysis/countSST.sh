#!/bin/bash

targetAnalysisPath=$1

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files:"
echo $sstablesSet

exit
if [ ! -d $ResultLogFolder ]; then
    mkdir -p $ResultLogFolder
fi

for ((roundIndex = 1; roundIndex <= $MAXRunTimes; roundIndex++)); do

    for ReadProportion in ${ReadRatioSet[@]}; do
        # Running Update

        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
        UpdateProportion=$(echo "1.0-$OverWriteRatio-$ReadProportion" | bc)
        echo "Modify spec, Read/Update ratio = $ReadProportion:0$UpdateProportion"
        cp workloads/workloada-test.spec ./workloada-temp.spec
        sed -i "9s/NaN/$KVPairsNumber/g" workloada-temp.spec
        sed -i "10s/NaN/$OperationsNumber/g" workloada-temp.spec
        sed -i "15s/0/$ReadProportion/g" workloada-temp.spec
        sed -i "16s/0/0$UpdateProportion/g" workloada-temp.spec
        sed -i "18s/0/$OverWriteRatio/g" workloada-temp.spec
        echo "<===================== Modified spec file content =====================>"
        cat workloada-temp.spec | head -n 20 | tail -n 6

        # Running the ycsb-benchmark
        if [ -d $DB_Name ]; then
            rm -rf $DB_Name
            echo "Deleted old database folder"
        fi
        echo "<===================== Loading the database (Round $roundIndex) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath configDir/leveldb_config.ini
        exit
        echo "<===================== Benchmark the database (Round $roundIndex) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath configDir/leveldb_config.ini >$ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # Running RMW

        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
        RMWProportion=$(echo "1.0-$OverWriteRatio-$ReadProportion" | bc)
        echo "Modify spec, Read/RMW ratio = $ReadProportion:0$RMWProportion"
        cp workloads/workloada-test.spec ./workloada-temp.spec
        sed -i "9s/NaN/$KVPairsNumber/g" workloada-temp.spec
        sed -i "10s/NaN/$OperationsNumber/g" workloada-temp.spec
        sed -i "15s/0/$ReadProportion/g" workloada-temp.spec
        sed -i "17s/0/0$RMWProportion/g" workloada-temp.spec
        sed -i "18s/0/$OverWriteRatio/g" workloada-temp.spec
        echo "<===================== Modified spec file content =====================>"
        cat workloada-temp.spec | head -n 20 | tail -n 6

        # Running the ycsb-benchmark
        if [ -d $DB_Name ]; then
            rm -rf $DB_Name
            echo "Deleted old database folder"
        fi
        echo "<===================== Loading the database (Round $roundIndex) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath configDir/leveldb_config.ini
        echo "<===================== Benchmark the database (Round $roundIndex) =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath configDir/leveldb_config.ini >$ResultLogFolder/Read-$ReadProportion-RMW-0$RMWProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # Cleanup
        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
    done

done
