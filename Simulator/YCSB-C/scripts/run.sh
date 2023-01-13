#!/bin/bash

usage() {
    echo "Usage: $0 [kv] [kd] [bkv] [bs1000] [req1m]"
    echo "       kv: use KV separation (vLog)"
    echo "       kd: use KD separation (Delta store)"
    echo "      bkv: use BlobDB"
    echo "      raw: use RocksDB"
    echo "     kvkd: use DeltaKV"
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
      i=$(($i+1))
      continue
    fi
    break
  done

  echo "$filename.log"
}

log_db_status() {
    DBPath=$1
    ResultLogFile=$2

    output_file=tmpappend
    rm -rf $output_file 
    echo "-------- smallest deltas ---------" >> $output_file 
    ls -lt $DBPath | grep "delta" | sort -n -k5 | head >> $output_file
    echo "-------- largest deltas ----------" >> $output_file
    ls -lt $DBPath | grep "delta" | sort -n -k5 | tail >> $output_file
    echo "-------- delta sizes and counts --" >> $output_file
    ls -lt $DBPath | grep "delta" | awk '{s[$5]++;} END {for (i in s) {print i " " s[i];}}' | sort -k1 -n >> $output_file
    echo "--------- total delta sizes ------" >> $output_file
    ls $DBPath/hashStoreFileManifestFile.* | while read line; do
	awk '{if (NR % 6 == 0) s+=$1;} END {print s / 1024 / 1024 " MiB delta";}' $line >> $output_file 
	awk '{if (NR % 6 == 0) mp[$1]++;} END {for (i in mp) print i " " mp[i];}' $line | sort -n -k1 >> $output_file 
    done
    ls -lt $DBPath | grep "delta" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB delta, num = " t;}' >> $output_file
    ls -lt $DBPath | grep "sst" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB sst, num = " t;}' >> $output_file 
    ls -lt $DBPath | grep "blob" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB blob, num = " t;}' >> $output_file 
    ls -lt $DBPath | grep "c0" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB vLog";}' >> $output_file
    echo "---------- total size ------------" >> $output_file
    du -d1 $DBPath | tail -n 1 | awk '{print $1 / 1024 " MiB all";}' >> $output_file 
    echo "----------------------------------" >> $output_file

    cat $output_file
    cat $output_file >> $ResultLogFile
    cat temp.ini >> $ResultLogFile

# dump LOG
    cp $DBPath/LOG $ResultLogFile-LOG

# dump OPTIONS 
    OPTIONS=`ls -lht $DBPath/OPTIONS-* | head -n 1 | awk '{print $NF;}'`
    cat $OPTIONS >> $ResultLogFile-LOG
}

pwd
ulimit -n 10240
ulimit -s 102400
echo $@
# ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)
ReadProportion=0.1
OverWriteRatio=0.0
bucketNumber=4000
KVPairsNumber=10000000    #"300000000"
OperationsNumber=10000000 #"300000000"
fieldlength=400
fieldcount=10
DB_Working_Path="/mnt/g/deltakv/working"
DB_Loaded_Path="/mnt/e/deltakvload"
if [[ ! -d "/mnt/g" ]]; then
    DB_Working_Path="/mnt/lvm/deltakv/working"
    DB_Loaded_Path="/mnt/lvm/deltakv"
fi
ResultLogFolder="Exp3/ResultLogs"
DB_Name="loadedDB"
MAXRunTimes=1
Thread_number=1
RocksDBThreadNumber=16
rawConfigPath="configDir/deltakv.ini"
bucketSize="1048576"
cacheSize="$(( 1024 * 1024 * 1024 ))"
kvCacheSize=0
blobCacheSize=0
kdcache=0
workerThreadNumber=12
gcThreadNumber=2
batchSize=2000
cacheIndexFilter=false
paretokey="false"
nogc="false"
# usage

cp $rawConfigPath ./temp.ini

suffix=""
run_suffix=""
only_copy=""
only_load=""

cleanFlag="false"
usekv="false"
usekd="false"
usekvkd="false"
usebkvkd="false"
gc="true"
maxFileNumber="16"
blockSize=4096
fake="false"
nodirect="false"
nommap="false"

for param in $*; do
    if [[ $param == "kv" ]]; then
        suffix=${suffix}_kv
        usekv="true"
        sed -i "18s/false/true/g" temp.ini
    elif [[ $param == "kd" ]]; then
        suffix=${suffix}_kd
        usekd="true"
        sed -i "19s/false/true/g" temp.ini
    elif [[ $param == "raw" ]]; then
        suffix=${suffix}_raw
    elif [[ $param == "kvkd" ]]; then
        suffix=${suffix}_kvkd
        usekvkd="true"
        sed -i "18s/false/true/g" temp.ini
        sed -i "19s/false/true/g" temp.ini
    elif [[ $param == "bkv" ]]; then
        suffix=${suffix}_bkv
        sed -i "20s/false/true/g" temp.ini
    elif [[ $param == "bkvkd" ]]; then
        suffix=${suffix}_bkvkd
        usebkvkd="true"
        sed -i "19s/false/true/g" temp.ini
        sed -i "20s/false/true/g" temp.ini
    elif [[ $param == "copy" ]]; then
        only_copy="true"
    elif [[ $param == "load" ]]; then
        only_load="true"
    elif [[ `echo $param | grep "req" | wc -l` -eq 1 ]]; then  # req10m
        num=`echo $param | sed 's/req//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
        KVPairsNumber=$num
    elif [[ `echo $param | grep "op" | wc -l` -eq 1 ]]; then  # op2m
        num=`echo $param | sed 's/op//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
        OperationsNumber=$num
    elif [[ `echo $param | grep "fc" | wc -l` -eq 1 ]]; then
        num=`echo $param | sed 's/fc//g'`
        fieldcount=$num
    elif [[ `echo $param | grep "fl" | wc -l` -eq 1 ]]; then
        num=`echo $param | sed 's/fl//g'`
        fieldlength=$num
    elif [[ `echo $param | grep "readRatio" | wc -l` -eq 1 ]]; then
        ReadProportion=`echo $param | sed 's/readRatio//g'`
    elif [[ `echo $param | grep "bucketNum" | wc -l` -eq 1 ]]; then
        bucketNumber=`echo $param | sed 's/bucketNum//g'`
    elif [[ `echo $param | grep "Exp" | wc -l` -eq 1 ]]; then
        ExpID=`echo $param | sed 's/Exp//g'`
#        DB_Working_Path="/mnt/lvm/Exp$ExpID/RunDB"
#        DB_Loaded_Path="/mnt/lvm/Exp$ExpID/BackupDB"
        ResultLogFolder="Exp$ExpID/ResultLogs"
        if [ ! -d $DB_Working_Path ]; then
            mkdir -p $DB_Working_Path
        fi
        if [ ! -d $DB_Loaded_Path ]; then
            mkdir -p $DB_Loaded_Path
        fi
    elif [[ `echo $param | grep "note" | wc -l` -eq 1 ]]; then
        note=`echo $param | sed 's/note//g'`
	run_suffix=${run_suffix}_${note}
    elif [[ `echo $param | grep "threads" | wc -l` -eq 1 ]]; then
        RocksDBThreadNumber=`echo $param | sed 's/threads//g'`
    elif [[ `echo $param | grep "gcT" | wc -l` -eq 1 ]]; then
        gcThreadNumber=`echo $param | sed 's/gcT//g'`
    elif [[ `echo $param | grep "workerT" | wc -l` -eq 1 ]]; then
        workerThreadNumber=`echo $param | sed 's/workerT//g'`
    elif [[ `echo $param | grep "batchSize" | wc -l` -eq 1 ]]; then
        num=`echo $param | sed 's/batchSize//g' | sed 's/k/000/g' | sed 's/K/000/g'`
        batchSize=$num
    elif [[ `echo $param | grep "round" | wc -l` -eq 1 ]]; then
        MAXRunTimes=`echo $param | sed 's/round//g'`
    elif [[ `echo $param | grep "maxFileNumber" | wc -l` -eq 1 ]]; then
        maxFileNumber=`echo $param | sed 's/maxFileNumber//g'`
    elif [[ "$param" =~ ^cache[0-9]+$ ]]; then
        num=`echo $param | sed 's/cache//g'`
        cacheSize=$(( $num * 1024 * 1024 ))
        run_suffix=${run_suffix}_$param
    elif [[ "$param" =~ ^kvcache[0-9]+$ ]]; then
        num=`echo $param | sed 's/kvcache//g'`
        kvCacheSize=$(( $num * 1024 * 1024 ))
        run_suffix=${run_suffix}_$param
    elif [[ "$param" =~ ^blobcache[0-9]+$ ]]; then
        num=`echo $param | sed 's/blobcache//g'`
        blobCacheSize=$(( $num * 1024 * 1024 ))
        run_suffix=${run_suffix}_$param
    elif [[ "$param" =~ ^kdcache[0-9]+$ ]]; then
        num=`echo $param | sed 's/kdcache//g'`
        kdcache=$(( $num * 1024 * 1024 ))
        run_suffix=${run_suffix}_$param
    elif [[ "$param" =~ ^blockSize[0-9]+$ ]]; then
        blockSize=`echo $param | sed 's/blockSize//g'`
        if [[ $blockSize -ne 4096 ]]; then
            run_suffix=${run_suffix}_$param
        fi
    elif [[ `echo $param | grep "clean" | wc -l` -eq 1 ]]; then
	cleanFlag="true"
	exit
    elif [[ "$param" == "cif" ]]; then
        cacheIndexFilter="true"
        run_suffix=${run_suffix}_cif
    elif [[ "$param" == "fake" ]]; then
        fake="true"
        run_suffix=${run_suffix}_fake
    elif [[ "$param" == "nodirect" ]]; then
        nodirect="true"
        run_suffix=${run_suffix}_nodirect
    elif [[ "$param" == "nommap" ]]; then
        nommap="true"
        run_suffix=${run_suffix}_nommap
    elif [[ "$param" == "paretokey" ]]; then
        paretokey="true"
    elif [[ "$param" == "nogc" ]]; then
        nogc="true"
    fi
done

# KV cache

if [[ $kvCacheSize -ne 0 ]]; then
    if [[ "$usekd" == "false" && "$usebkvkd" == "false" ]]; then
	bucketNumber=0
    fi
    deltaKVCacheSize=$(( ( $kvCacheSize - $bucketNumber * 4096 ) / ($fieldcount * $fieldlength + $fieldcount - 1) ))
    sed -i "/enableDeltaKVCache/c\\enableDeltaKVCache = true" temp.ini
    sed -i "/deltaKVCacheObjectNumber/c\\deltaKVCacheObjectNumber = $deltaKVCacheSize" temp.ini 
else
    sed -i "/enableDeltaKVCache/c\\enableDeltaKVCache = false" temp.ini
fi

# KD cache (append only)

if [[ $kdcache -ne 0 ]]; then
    deltaLogCacheSize=$(( $kdcache / ($fieldcount * $fieldlength / 2) ))
    sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $deltaLogCacheSize" temp.ini
fi

if [[ "$usekd" == "true" || "$usebkvkd" == "true" || "$usekvkd" == "true" ]]; then
    echo usekd $cacheSize
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
    sed -i "/deltaLogMaxFileNumber/c\\deltaLogMaxFileNumber = $bucketNumber" temp.ini 
    sed -i "/deltaStore_worker_thread_number_limit_/c\\deltaStore_worker_thread_number_limit_ = $workerThreadNumber" temp.ini
    sed -i "/deltaStore_gc_thread_number_limit_/c\\deltaStore_gc_thread_number_limit_ = $gcThreadNumber" temp.ini
    sed -i "/deltaKVWriteBatchSize/c\\deltaKVWriteBatchSize = $batchSize" temp.ini
fi

sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
sed -i "/blobCacheSize/c\\blobCacheSize = ${blobCacheSize}" temp.ini

totCacheSize=$(( ( ${kvCacheSize} + $kdcache + $cacheSize + $blobCacheSize) / 1024 / 1024 ));
run_suffix=${run_suffix}_totcache${totCacheSize}

sed -i "/numThreads/c\\numThreads = ${RocksDBThreadNumber}" temp.ini 
sed -i "/blockSize/c\\blockSize = ${blockSize}" temp.ini 

if [[ "$maxFileNumber" -ne 16 ]]; then
  run_suffix=${run_suffix}_maxBucketNumber${maxFileNumber}
fi

if [[ "$cacheIndexFilter" == "true" ]]; then
    sed -i "/cacheIndexAndFilterBlocks/c\\cacheIndexAndFilterBlocks = true" temp.ini 
fi

if [[ "$fake" == "true" ]]; then
    sed -i "/fakeDirectIO/c\\fakeDirectIO = true" temp.ini 
fi

if [[ "$nodirect" == "true" ]]; then
    sed -i "/directIO/c\\directIO = false" temp.ini 
fi

if [[ "$nommap" == "true" ]]; then
    sed -i "/useMmap/c\\useMmap = false" temp.ini 
fi

size="$(( $KVPairsNumber / 1000000 ))M"
if [[ $size == "0M" ]]; then
    size="$(( $KVPairsNumber / 1000 ))K"
elif [[ "$(( $KVPairsNumber % 1000000 ))" -ne 0 ]]; then
    echo "$(( $KVPairsNumber % 1000000 ))" 
    size="${size}$(( ($KVPairsNumber % 1000000) / 1000 ))K"
fi

ops="op$(( $OperationsNumber / 1000000 ))M"
if [[ $ops == "0M" ]]; then
    ops="op$(( $OperationsNumber / 1000 ))K"
elif [[ "$(( $OperationsNumber % 1000000 ))" -ne 0 ]]; then
    ops="${ops}$(( ($OperationsNumber % 1000000) / 1000 ))K"
fi

if [[ $paretokey == "true" ]]; then
    suffix=${suffix}_fc${fieldcount}_paretokey_${size}
else
    suffix=${suffix}_fc${fieldcount}_fl${fieldlength}_${size}
fi

if [[ $nogc == "true" ]]; then
    sed -i "/deltaStoreEnableGC/c\\deltaStoreEnableGC = false" temp.ini 
fi

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

echo "Modify spec for load"
cp workloads/workloadTemplate.spec ./workload-temp.spec
sed -i "9s/NaN/$KVPairsNumber/g" workload-temp.spec
sed -i "10s/NaN/$OperationsNumber/g" workload-temp.spec
sed -i "24s/NaN/$fieldcount/g" workload-temp.spec
sed -i "25s/NaN/$fieldlength/g" workload-temp.spec
if [[ $paretokey == "true" ]]; then
    sed -i "/field_len_dist/c\\field_len_dist=paretokey" workload-temp.spec
fi

loaded="false"
workingDB=${DB_Working_Path}/workingDB
loadedDB=${DB_Loaded_Path}/${DB_Name}

echo "<===================== Loading the database =====================>"

if [[ "$cleanFlag" == "true" ]]; then
    set -x
    rm -rf $loadedDB 
    rm -rf $workingDB
    exit
fi

if [[ ! -d $loadedDB || "$only_load" == "true" ]]; then
    rm -rf $loadedDB 
    if [[ -d $workingDB ]]; then 
        rm -rf $workingDB 
    fi
    output_file=`generate_file_name $ResultLogFolder/LoadDB${run_suffix}`
    echo "output at $output_file"
    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath > ${output_file}
#    gdb --args ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath # > ${output_file}
    loaded="true"
    echo "output at $output_file"
    if [[ $? -ne 0 ]]; then
	echo "Exit. return number $?"
	exit
    fi
    log_db_status $workingDB $output_file

    # Running Update
    SPEC="./workload-temp-prepare.spec"
    cp workloads/workloadTemplate.spec $SPEC 
    sed -i "9s/NaN/$KVPairsNumber/g" $SPEC 
    sed -i "10s/NaN/3000000/g" $SPEC 
    sed -i "15s/0/1/g" $SPEC
    sed -i "16s/0/0/g" $SPEC
    sed -i "24s/NaN/$fieldcount/g" $SPEC
    sed -i "25s/NaN/$fieldlength/g" $SPEC
    echo "<===================== Prepare =====================>"
    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P ${SPEC} -phase run -configpath $configPath > ${output_file}-prepare
    echo "output at ${output_file}-prepare"

    cp -r $workingDB $loadedDB # Copy loaded DB
    if [[ "$only_load" == "true" ]]; then
	exit
    fi
fi

run_suffix="${run_suffix}-${ops}"

for ((roundIndex = 1; roundIndex <= MAXRunTimes; roundIndex++)); do


        if [ -f workload-temp.spec ]; then
            rm -rf workload-temp.spec
            echo "Deleted old workload spec"
        fi
        UpdateProportion=$(echo "1.0-$ReadProportion" | bc)
        echo "Modify spec, Read/Update ratio = $ReadProportion:0$UpdateProportion"
        cp workloads/workloadTemplate.spec ./workload-temp.spec
        sed -i "9s/NaN/$KVPairsNumber/g" workload-temp.spec
        sed -i "10s/NaN/$OperationsNumber/g" workload-temp.spec
        sed -i "15s/0/$ReadProportion/g" workload-temp.spec
        sed -i "16s/0/0$UpdateProportion/g" workload-temp.spec
        sed -i "24s/NaN/$fieldcount/g" workload-temp.spec
        sed -i "25s/NaN/$fieldlength/g" workload-temp.spec
        if [[ $paretokey == "true" ]]; then
            sed -i "/field_len_dist/c\\field_len_dist=paretokey" workload-temp.spec
        fi
        echo "<===================== Modified spec file content =====================>"
        cat workload-temp.spec | head -n 25 | tail -n 17
        # Running the ycsb-benchmark
        if [[ "$loaded" == "false" ]]; then
            if [ -d $workingDB ]; then
                rm -rf $workingDB 
                echo "Deleted old database folder"
            fi
            echo "cp -r $loadedDB $workingDB"
            cp -r $loadedDB $workingDB
            echo "Copy loaded database"
        fi
        if [ ! -d $workingDB ]; then
            echo "Retrived loaded database error"
            exit
        fi
        if [[ $only_copy == "true" ]]; then
            exit
        fi


        echo "<===================== Benchmark the database (Round $roundIndex) start =====================>"
        output_file=`generate_file_name $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-${run_suffix}-gcT${gcThreadNumber}-workerT${workerThreadNumber}-BatchSize-${batchSize}`
        if [[ "$usekd" != "true" && "$usekvkd" != "true" && "$usebkvkd" != "true" ]]; then
            output_file=`generate_file_name $ResultLogFolder/Read-$ReadProportion-Update-0$UpdateProportion-${run_suffix}`
        fi
	echo "output at $output_file"
        ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath > $output_file
        loaded="false"
        echo "output at $output_file"
	log_db_status $workingDB $output_file
        echo "<===================== Benchmark the database (Round $roundIndex) done =====================>"
        # Cleanup
        if [ -f workload-temp.spec ]; then
            rm -rf workload-temp.spec
            echo "Deleted old workload spec"
        fi
        if [[ $roundIndex -eq $MAXRunTimes ]]; then
            if [ -f temp.ini ]; then
                rm -rf temp.ini
                echo "Deleted old workload config"
            fi
        fi
done
