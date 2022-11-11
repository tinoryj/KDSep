#!/bin/bash
pwd
ulimit -n 63356
ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8)
OverWriteRatio=0.1
ResultLogFolder="ResultLogs"
DB_Name="loadedDB"
DB_Loaded_Path="/mnt/sn640"
KVPairsNumber=100000000    #"300000000"
OperationsNumber=100000000 #"300000000"
MAXRunTimes=1
Thread_number=1
rawConfigPath="configDir/leveldb_config.ini"

cp $rawConfigPath ./temp.ini
sed -i "30s/NaN/$1/g" temp.ini
configPath="temp.ini"

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
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath $configPath >$ResultLogFolder/LoadDB.log
mv operations.log $ResultLogFolder/load-operations.log
cp -r $DB_Name $DB_Loaded_Path/ # Copy loaded DB

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
        ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath $configPath >$ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        mv operations.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-operations.log
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # # Running DB count:
        # echo "<===================== Count the database Info (Round $roundIndex) start =====================>"
        # mkdir -p $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis
        # ./countSST.sh $DB_Name
        # mv SSTablesAnalysis.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        # mv manifest.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        # mv levelBasedCount.log $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-DB-Analysis/
        # echo "<===================== Count the database Info (Round $roundIndex) done =====================>"

        # Running RMW

        # if [ -f workloada-temp.spec ]; then
        #     rm -rf workloada-temp.spec
        #     echo "Deleted old workload spec"
        # fi
        # RMWProportion=$(echo "1.0-$OverWriteRatio-$ReadProportion" | bc)
        # echo "Modify spec, Read/RMW ratio = $ReadProportion:0$RMWProportion"
        # cp workloads/workloada-test.spec ./workloada-temp.spec
        # sed -i "9s/NaN/$KVPairsNumber/g" workloada-temp.spec
        # sed -i "10s/NaN/$OperationsNumber/g" workloada-temp.spec
        # sed -i "15s/0/$ReadProportion/g" workloada-temp.spec
        # sed -i "17s/0/0$RMWProportion/g" workloada-temp.spec
        # sed -i "18s/0/$OverWriteRatio/g" workloada-temp.spec
        # echo "<===================== Modified spec file content =====================>"
        # cat workloada-temp.spec | head -n 20 | tail -n 6

        # # Running the ycsb-benchmark
        # if [ -d $DB_Name ]; then
        #     rm -rf $DB_Name
        #     echo "Deleted old database folder"
        # fi
        # cp -r $DB_Loaded_Path/$DB_Name ./
        # echo "Copy loaded database"
        # if [ ! -d $DB_Name ]; then
        #     echo "Retrived loaded database error"
        #     exit
        # fi

        # echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        # ./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath $configPath >$ResultLogFolder/Read-$ReadProportion-RMW-0$RMWProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
        # echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"

        # # Cleanup
        # if [ -f workloada-temp.spec ]; then
        #     rm -rf workloada-temp.spec
        #     echo "Deleted old workload spec"
        # fi
    done
done
