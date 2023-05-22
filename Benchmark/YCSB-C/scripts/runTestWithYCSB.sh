#!/bin/bash

usage() {
    echo "Usage: $0 [kv] [kd] [bkv] [bs1000] [req1m]"
    echo "       kv: use KV separation (vLog)"
    echo "       kd: use KD separation (Delta store)"
    echo "      bkv: use BlobDB"
    echo "   bs1000: Bucket size 1000"
    echo "    req1m: Totally 1M KV pairs"
    echo "     load: Load the database again"
    echo "     copy: Copy the database and do not run ycsbc"
}

generate_file_name() {
    i=1
    file=$1

    while [[ $i -lt 100 ]]; do
        filename="${file}-Round-${i}"
        if [[ -f "$filename.log" || -f "$filename" ]]; then
            i=$(($i + 1))
            continue
        fi
        break
    done

    echo "$filename.log"
}

pwd
ulimit -n 1048576
echo $@
WorkloadSet=('a' 'b' 'c' 'd' 'e' 'f')
DB_Working_Path="./loadedDB/Exp"
DB_Loaded_Path="./loadedDB/backupDB"
if [ ! -d $DB_Working_Path ]; then
    mkdir -p $DB_Working_Path
fi
if [ ! -d $DB_Loaded_Path ]; then
    mkdir -p $DB_Loaded_Path
fi
DB_Name="loadedDB"
ResultLogFolder="ResultLogs"
MAXRunTimes=1
Thread_number=1
RocksDBThreadNumber=16
rawConfigPath="configDir/KDSep.ini"
bucketSize="1048576"
cacheSize="$((1024 * 1024 * 1024))"

# usage

cp $rawConfigPath ./temp.ini

echo "Usage: $0 [kv] [kd] [bkv]"
suffix=""
run_suffix=""
only_copy=""
only_load=""

usekv="false"
usekd="false"
usekvkd="false"
gc="true"
maxFileNumber="16"
reads="0.1"

WorkloadSet=("$reads")

for param in $*; do
    if [[ $param == "kv" ]]; then
        suffix=${suffix}_kv
        usekv="true"
        sed -i "18s/false/true/g" temp.ini
    elif [[ $param == "kd" ]]; then
        suffix=${suffix}_kd
        usekd="true"
        sed -i "19s/false/true/g" temp.ini
    elif [[ $param == "kvkd" ]]; then
        suffix=${suffix}_kvkd
        usekvkd="true"
        sed -i "18s/false/true/g" temp.ini
        sed -i "19s/false/true/g" temp.ini
    elif [[ $param == "bkv" ]]; then
        suffix=${suffix}_bkv
        sed -i "20s/false/true/g" temp.ini
    elif [[ $param == "copy" ]]; then
        only_copy="true"
    elif [[ $param == "load" ]]; then
        only_load="true"
    elif [[ $(echo $param | grep "req" | wc -l) -eq 1 ]]; then # req10m
        num=$(echo $param | sed 's/req//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g')
        KVPairsNumber=$num
    elif [[ $(echo $param | grep "op" | wc -l) -eq 1 ]]; then # op2m
        num=$(echo $param | sed 's/op//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g')
        OperationsNumber=$num
    elif [[ $(echo $param | grep "fc" | wc -l) -eq 1 ]]; then
        suffix=${suffix}-${param}
        run_suffix=${run_suffix}-${param}
        num=$(echo $param | sed 's/fc//g')
        fieldcount=$num
    elif [[ $(echo $param | grep "fl" | wc -l) -eq 1 ]]; then
        suffix=${suffix}-${param}
        run_suffix=${run_suffix}-${param}
        num=$(echo $param | sed 's/fl//g')
        fieldlength=$num
    elif [[ $(echo $param | grep "threads" | wc -l) -eq 1 ]]; then
        RocksDBThreadNumber=$(echo $param | sed 's/threads//g')
    elif [[ $(echo $param | grep "round" | wc -l) -eq 1 ]]; then
        MAXRunTimes=$(echo $param | sed 's/round//g')
    elif [[ $(echo $param | grep "maxFileNumber" | wc -l) -eq 1 ]]; then
        maxFileNumber=$(echo $param | sed 's/maxFileNumber//g')
    elif [[ $(echo $param | grep "cache" | wc -l) -eq 1 ]]; then
        num=$(echo $param | sed 's/cache//g')
        cacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_$param
    fi
done

if [[ "$usekv" == "true" ]]; then
    valueCacheSize=$(($cacheSize / 8 * 7 / ($fieldcount * $fieldlength + $fieldcount - 1)))
    cacheSize=$(($cacheSize / 8))
    echo usekv $valueCacheSize $cacheSize
    sed -i "/valueCache/c\\valueCacheSize = $valueCacheSize" temp.ini
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini

elif [[ "$usekd" == "true" ]]; then
    deltaLogCacheObjectNumber=$(($cacheSize / 8 / ($fieldlength + 2)))
    cacheSize=$(($cacheSize / 8 * 7))
    echo usekd $deltaLogCacheObjectNumber $cacheSize
    sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $deltaLogCacheObjectNumber" temp.ini
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini

elif [[ "$usekvkd" == "true" ]]; then
    valueCacheSize=$(($cacheSize / 8 * 6 / ($fieldcount * $fieldlength + $fieldcount - 1)))
    deltaLogCacheObjectNumber=$(($cacheSize / 8 / ($fieldlength + 2)))
    cacheSize=$(($cacheSize / 8))
    echo usekvkd $valueCacheSize $deltaLogCacheObjectNumber $cacheSize
    sed -i "/valueCache/c\\valueCacheSize = $valueCacheSize" temp.ini
    sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $deltaLogCacheObjectNumber" temp.ini
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini

else
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
fi

sed -i "/numThreads/c\\numThreads = ${RocksDBThreadNumber}" temp.ini

# sed -i "/deltaLogMaxFileNumber/c\\deltaLogMaxFileNumber = ${maxFileNumber}" temp.ini
if [[ "$maxFileNumber" -ne 16 ]]; then
    run_suffix=${run_suffix}_maxBucketNumber${maxFileNumber}
fi

size="$(($KVPairsNumber / 1000000))M"
if [[ $size == "0M" ]]; then
    size="$(($KVPairsNumber / 1000))K"
elif [[ "$(($KVPairsNumber % 1000000))" -ne 0 ]]; then
    echo "$(($KVPairsNumber % 1000000))"
    size="${size}$((($KVPairsNumber % 1000000) / 1000))K"
fi

ops="op$(($OperationsNumber / 1000000))M"
if [[ $ops == "0M" ]]; then
    ops="op$(($OperationsNumber / 1000))K"
elif [[ "$(($OperationsNumber % 1000000))" -ne 0 ]]; then
    ops="${ops}$((($OperationsNumber % 1000000) / 1000))K"
fi

suffix="${suffix}_${size}"
run_suffix="${run_suffix}-${ops}"

DB_Name=${DB_Name}${suffix}
ResultLogFolder=${ResultLogFolder}${suffix}
configPath="temp.ini"

if [ ! -d $ResultLogFolder ]; then
    mkdir -p $ResultLogFolder
fi

if [ -f workload-temp.spec ]; then
    rm -rf workload-temp.spec
    echo "Deleted old workload spec"
fi

echo "Copy workload"
cp workloads/workloada.spec ./workload-temp.spec

echo "<===================== Loading the database =====================>"

if [[ "$only_load" == "true" ]]; then
    rm -rf ${DB_Loaded_Path}/${DB_Name}
    if [[ -d ${DB_Working_Path}/$DB_Name ]]; then
        rm -rf ${DB_Working_Path}/$DB_Name
    fi
    output_file=$(generate_file_name $ResultLogFolder/LoadDB)
    ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath >${output_file}
    echo "output at $output_file"
    cp -r ${DB_Working_Path}/$DB_Name $DB_Loaded_Path/ # Copy loaded DB
    exit
fi

if [[ ! -d ${DB_Loaded_Path}/${DB_Name} ]]; then
    if [[ -d ${DB_Working_Path}/$DB_Name ]]; then
        rm -rf ${DB_Working_Path}/$DB_Name
    fi
    output_file=$(generate_file_name $ResultLogFolder/LoadDB)
    ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath >${output_file}
    echo "output at $output_file"
    cp -r ${DB_Working_Path}/$DB_Name $DB_Loaded_Path/ # Copy loaded DB
fi

for ((roundIndex = 1; roundIndex <= MAXRunTimes; roundIndex++)); do

    for workloadType in "${WorkloadSet[@]}"; do
        # Running Update

        if [ -f workload-temp.spec ]; then
            rm -rf workload-temp.spec
            echo "Deleted old workload spec"
        fi
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

        cp workloads/workload${workloadType}.spec ./workload-temp.spec

        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        output_file=$(generate_file_name $ResultLogFolder/Read-$workloadType-Update-0$UpdateProportion-${run_suffix})
        ./ycsbc -db rocksdb -dbfilename ${DB_Working_Path}/$DB_Name -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath >$output_file
        echo "-------- smallest deltas ---------" >>$output_file
        echo "-------- smallest deltas ---------"
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | sort -n -k5 | head >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | sort -n -k5 | head
        echo "-------- largest deltas ----------" >>$output_file
        echo "-------- largest deltas ----------"
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | sort -n -k5 | tail >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | sort -n -k5 | tail
        echo "-------- delta sizes and counts --" >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | awk '{s[$5]++;} END {for (i in s) {print i " " s[i];}}' | sort -k1 -n >>$output_file
        echo "--------- total delta sizes ------" >>$output_file
        echo "--------- total delta sizes ------"
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB delta";}' >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "delta" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB delta";}'
        ls -lt ${DB_Working_Path}/$DB_Name | grep "sst" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB sst";}' >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "sst" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB sst";}'
        ls -lt ${DB_Working_Path}/$DB_Name | grep "blob" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB blob";}' >>$output_file
        ls -lt ${DB_Working_Path}/$DB_Name | grep "blob" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB blob";}'
        echo "----------------------------------" >>$output_file
        cat temp.ini >>$output_file
        echo "output at $output_file"
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"
        # Cleanup
        if [ -f workload-temp.spec ]; then
            rm -rf workload-temp.spec
            echo "Deleted old workload spec"
        fi
        if [ -f temp.ini ]; then
            rm -rf temp.ini
            echo "Deleted old KDSep spec"
        fi
    done
done
