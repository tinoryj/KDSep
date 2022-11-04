#!/bin/bash
pwd
ReadRatioSet=(0.1)
OverWriteRatio=0.1
ResultLogFolder="ResultLogs"
DB_Name="loadedDB"
DB_Loaded_Path="."
KVPairsNumber=3000    #"300000000"
OperationsNumber=1000 #"300000000"
MAXRunTimes=1
Thread_number=1

if [ ! -d $ResultLogFolder ]; then
    mkdir -p $ResultLogFolder
fi

if [ -f workloada-temp.spec ]; then
    rm -rf workloada-temp.spec
    echo "Deleted old workload spec"
fi

echo "Modify spec for load"
cp workloads/workloada-test.spec ./workloada-temp.spec
sed -i "9s/NaN/$KVPairsNumber/g" workloada-temp.spec
sed -i "10s/NaN/$OperationsNumber/g" workloada-temp.spec

echo "<===================== Loading the database =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath configDir/leveldb_config.ini >$ResultLogFolder/LoadDB.log
exit

for ((roundIndex = 1; roundIndex <= MAXRunTimes; roundIndex++)); do

    for ReadProportion in "${ReadRatioSet[@]}"; do
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
        cp -r $DB_Loaded_Path/$DB_Name ./
        echo "Copy loaded database"
        if [ ! -d $DB_Name ]; then
            echo "Retrived loaded database error"
            exit
        fi

        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath configDir/leveldb_config.ini >$ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # Running DB count:
        echo "<===================== Count the database Info (Round $roundIndex) start =====================>"
        mkdir -p $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis
        ./countSST.sh $DB_Name
        mv SSTablesAnalysis.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        mv manifest.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        mv levelBasedCount.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        echo "<===================== Count the database Info (Round $roundIndex) done =====================>"

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
        cp -r $DB_Loaded_Path/$DB_Name ./
        echo "Copy loaded database"
        if [ ! -d $DB_Name ]; then
            echo "Retrived loaded database error"
            exit
        fi

        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath configDir/leveldb_config.ini >$ResultLogFolder/Read-$ReadProportion-RMW-0$RMWProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # Cleanup
        if [ -f workloada-temp.spec ]; then
            rm -rf workloada-temp.spec
            echo "Deleted old workload spec"
        fi
    done

done
