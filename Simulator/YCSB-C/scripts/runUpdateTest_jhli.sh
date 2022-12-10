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
ulimit -n 1048576 
ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8)
ReadRatioSet=(0.1)
OverWriteRatio=0.1
KVPairsNumber=40000000    #"300000000"
OperationsNumber=40000000 #"300000000"
fieldlength=100
fieldcount=10
DB_Working_Path="/mnt/data/jhli/"
DB_Loaded_Path="/mnt/data/load/"
DB_Name="loadedDB"
ResultLogFolder="ResultLogs"
MAXRunTimes=1
Thread_number=1
rawConfigPath="configDir/deltakv.ini"

usage

cp $rawConfigPath ./temp.ini

echo "Usage: $0 [kv] [kd] [bkv]"
suffix=""
run_suffix=""
only_copy=""
only_load=""

usekv="false"
usekd="false"
usekvkd="false"
gc="false"
minbits="16"

for param in $*; do
    if [[ $param == "kv" ]]; then
        suffix=${suffix}_kv
        usekv="true"
        cp "configDir/deltakv_kv.ini" temp.ini
    elif [[ $param == "kd" ]]; then
        suffix=${suffix}_kd
        usekd="true"
        cp "configDir/deltakv_kd.ini" temp.ini
    elif [[ $param == "kvkd" ]]; then
        suffix=${suffix}_kvkd
        usekvkd="true"
        cp "configDir/deltakv_kv_kd.ini" temp.ini
    elif [[ $param == "testkd" ]]; then
        suffix=${suffix}_testkd
        usekd="true"
        cp "configDir/deltakv_testkd.ini" temp.ini
    elif [[ $param == "bkv" ]]; then
        suffix=${suffix}_bkv
        sed -i "31s/false/true/g" temp.ini
    elif [[ $param == "copy" ]]; then
        only_copy="true"
    elif [[ $param == "load" ]]; then
        only_load="true"
    elif [[ $param == "gc" ]]; then
        gc="true"
    elif [[ `echo $param | grep "req" | wc -l` -eq 1 ]]; then
        num=`echo $param | sed 's/req//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
        KVPairsNumber=$num
        OperationsNumber=$num
    elif [[ `echo $param | grep "op" | wc -l` -eq 1 ]]; then
        num=`echo $param | sed 's/op//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
        OperationsNumber=$num
    elif [[ `echo $param | grep "fc" | wc -l` -eq 1 ]]; then
        suffix=${suffix}-${param}
        run_suffix=${run_suffix}-${param}
        num=`echo $param | sed 's/fc//g'`
        fieldcount=$num
    elif [[ `echo $param | grep "fl" | wc -l` -eq 1 ]]; then
        suffix=${suffix}-${param}
        run_suffix=${run_suffix}-${param}
        num=`echo $param | sed 's/fl//g'`
        fieldlength=$num
    elif [[ `echo $param | grep "minbits" | wc -l` -eq 1 ]]; then
        minbits=`echo $param | sed 's/minbits//g'`
    fi
done

if [[ "$usekv" == "true" ]]; then
  valueCacheSize=$(( 896 * 1024 * 1024 / ($fieldcount * $fieldlength + $fieldcount - 1)))
  echo usekv $valueCacheSize
  sed -i "/valueCache/c\\valueCacheSize = $valueCacheSize" temp.ini 
elif [[ "$usekd" == "true" ]]; then
  deltaLogCacheObjectNumber=$(( 128 * 1024 * 1024 / ($fieldlength + 2)))
  echo usekd $deltaLogCacheObjectNumber
  sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $deltaLogCacheObjectNumber" temp.ini 
elif [[ "$usekvkd" == "true" ]]; then
  valueCacheSize=$(( 768 * 1024 * 1024 / ($fieldcount * $fieldlength + $fieldcount - 1)))
  deltaLogCacheObjectNumber=$(( 128 * 1024 * 1024 / ($fieldlength + 2)))
  echo usekvkd $valueCacheSize $deltaLogCacheObjectNumber
  sed -i "/valueCache/c\\valueCacheSize = $valueCacheSize" temp.ini 
  sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $deltaLogCacheObjectNumber" temp.ini 
fi

if [[ "$gc" == "true" ]]; then
  sed -i "/deltaStoreEnableGC/c\\deltaStoreEnableGC = true" temp.ini 
  run_suffix=${run_suffix}_gc
fi

sed -i "/deltaLogPrefixMinBitNumber/c\\deltaLogPrefixMinBitNumber = ${minbits}" temp.ini 
if [[ "$minbits" -ne 16 ]]; then
  run_suffix=${run_suffix}_minbits${minbits}
fi

size="$(( $KVPairsNumber / 1000000 ))M"
if [[ $size == "0M" ]]; then
    size="$(( $KVPairsNumber / 1000 ))K"
elif [[ "$(( $KVPairsNumber % 1000000 ))" -ne 0 ]]; then
    size="${size}$(( ($KVPairsNumber % 1000000) / 1000 ))K"
fi

ops="op$(( $OperationsNumber / 1000000 ))M"
if [[ $ops == "0M" ]]; then
    ops="op$(( $OperationsNumber / 1000 ))K"
elif [[ "$(( $OperationsNumber % 1000000 ))" -ne 0 ]]; then
    size="${size}$(( ($OperationsNumber % 1000000) / 1000 ))K"
fi

suffix="${suffix}_${size}"
run_suffix="${run_suffix}-${ops}"

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
sed -i "24s/NaN/$fieldcount/g" workloada-temp.spec
sed -i "25s/NaN/$fieldlength/g" workloada-temp.spec

echo "<===================== Loading the database =====================>"

if [[ "$only_load" == "true" ]]; then
    rm -rf ${DB_Loaded_Path}/${DB_Name} 
    if [[ -d ${DB_Working_Path}/$DB_Name ]]; then 
        rm -rf ${DB_Working_Path}/$DB_Name
    fi
    output_file=`generate_file_name $ResultLogFolder/LoadDB`
    ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath $configPath > ${output_file}
    echo "output at $output_file"
    cp -r ${DB_Working_Path}/$DB_Name $DB_Loaded_Path/ # Copy loaded DB
    exit
fi

if [[ ! -d ${DB_Loaded_Path}/${DB_Name} ]]; then
    if [[ -d ${DB_Working_Path}/$DB_Name ]]; then 
        rm -rf ${DB_Working_Path}/$DB_Name
    fi
    output_file=`generate_file_name $ResultLogFolder/LoadDB`
    ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workloada-temp.spec -phase load -configpath $configPath > ${output_file}
    cp -r ${DB_Working_Path}/$DB_Name $DB_Loaded_Path/ # Copy loaded DB
fi

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
        sed -i "24s/NaN/$fieldcount/g" workloada-temp.spec
        sed -i "25s/NaN/$fieldlength/g" workloada-temp.spec
        echo "<===================== Modified spec file content =====================>"
        cat workloada-temp.spec | head -n 20 | tail -n 6

        # Running the ycsb-benchmark
        if [ -d ${DB_Working_Path}/${DB_Name} ]; then
            rm -rf ${DB_Working_Path}/${DB_Name}
            echo "Deleted old database folder"
        fi
        echo "cp -r $DB_Loaded_Path/$DB_Name ${DB_Working_Path}"
        cp -r $DB_Loaded_Path/$DB_Name ${DB_Working_Path} 
        echo "Copy loaded database"
        if [ ! -d ${DB_Working_Path}/$DB_Name ]; then
            echo "Retrived loaded database error"
            exit
        fi

        if [[ $only_copy == "true" ]]; then
            exit
        fi

        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        output_file=`generate_file_name $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-OverWrite-$OverWriteRatio-${run_suffix}`
        ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workloada-temp.spec -phase run -configpath $configPath > $output_file
        cat temp.ini >> $output_file
        echo "output at $output_file"
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
