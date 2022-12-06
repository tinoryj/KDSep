#!/bin/bash

usage() {
    echo "Usage: $0 [kv] [kd] [bkv]"
    echo "       kv: use KV separation (vLog)"
    echo "       kd: use KD separation (Delta store)"
    echo "      bkv: use BlobDB"
}

generate_file_name() {
  i=1
  file=$1

  while [[ $i -lt 100 ]]; do
    filename="${file}-Round-${i}"
    if [[ -f "$filename.log" || -f "$filename" ]]; then
      i=$(($i+1))
      continue
    fi
    break
  done

  echo "$filename.log"
}

pwd
ulimit -n 63356
ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8)
OverWriteRatio=0.1
KVPairsNumber=40000000    #"300000000"
OperationsNumber=40000000 #"300000000"
DB_Working_Path="/mnt/sn640/jhli/"
DB_Loaded_Path="/mnt/sn640"
DB_Name="loadedDB"
ResultLogFolder="ResultLogs"
MAXRunTimes=1
Thread_number=1
rawConfigPath="configDir/deltakv_config.ini"

usage

cp $rawConfigPath ./temp.ini

echo "Usage: $0 [kv] [kd] [bkv]"
suffix=""

for param in $*; do
    if [[ $param == "kv" ]]; then
        suffix=${suffix}_kv
        sed -i "29s/false/true/g" temp.ini
    elif [[ $param == "kd" ]]; then
        suffix=${suffix}_kd
        sed -i "30s/false/true/g" temp.ini
    elif [[ $param == "bkv" ]]; then
        suffix=${suffix}_bkv
        sed -i "31s/false/true/g" temp.ini
    fi
done
size="$(( $KVPairsNumber / 1000000 ))M"
suffix="${suffix}_${size}"

DB_Name=${DB_Name}${suffix}
ResultLogFolder=${ResultLogFolder}${suffix}
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

if [[ ! -d ${DB_Loaded_Path}/${DB_Name} ]]; then
    output_file=`generate_file_name $ResultLogFolder/LoadDB`
    ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath $configPath > ${output_file}
    if [[ -f ${DB_Working_Path}/operations.log ]]; then
        mv ${DB_Working_Path}/operations.log $ResultLogFolder/load-operations.log
    fi
    cp -r ${DB_Working_Path}/$DB_Name $DB_Loaded_Path/ # Copy loaded DB
fi
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
        if [ -d ${DB_Working_Path}/DB_Name ]; then
            rm -rf ${DB_Working_Path}/DB_Name
            echo "Deleted old database folder"
        fi
        cp -r $DB_Loaded_Path/$DB_Name ${DB_Working_Path} 
        echo "Copy loaded database"
        if [ ! -d $DB_Working_Path}/$DB_Name ]; then
            echo "Retrived loaded database error"
            exit
        fi

        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath $configPath >$ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-Round-$roundIndex.log
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
