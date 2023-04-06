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
            i=$(($i + 1))
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
    echo "-------- smallest deltas ---------" >>$output_file
    ls -lt $DBPath | grep "delta" | sort -n -k5 | head >>$output_file
    echo "-------- largest deltas ----------" >>$output_file
    ls -lt $DBPath | grep "delta" | sort -n -k5 | tail >>$output_file
    echo "-------- delta sizes and counts --" >>$output_file
    ls -lt $DBPath | grep "delta" | awk '{s[$5]++;} END {for (i in s) {print i " " s[i];}}' | sort -k1 -n >>$output_file
    echo "--------- total delta sizes ------" >>$output_file
    ls $DBPath/hashStoreFileManifestFile.* | while read line; do
        awk '{if (NR % 6 == 0) s+=$1;} END {print s / 1024 / 1024 " MiB delta";}' $line >>$output_file
        awk '{if (NR % 6 == 0) mp[$1]++;} END {for (i in mp) print i " " mp[i];}' $line | sort -n -k1 >>$output_file
    done
    ls -lt $DBPath | grep "delta" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB delta, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "sst" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB sst, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "blob" | awk '{s+=$5; t++;} END {print s / 1024 / 1024 " MiB blob, num = " t;}' >>$output_file
    ls -lt $DBPath | grep "c0" | awk '{s+=$5;} END {print s / 1024 / 1024 " MiB vLog";}' >>$output_file
    echo "---------- total size ------------" >>$output_file
    du -d1 $DBPath | tail -n 1 | awk '{print $1 / 1024 " MiB all";}' >>$output_file
    echo "----------------------------------" >>$output_file

    lines=`wc -l $output_file | awk '{print $1;}'`
    if [[ $lines -lt 50 ]]; then
        cat $output_file
    else
        head -n 30 $output_file
        tail -n 15 $output_file
    fi
    cat $output_file >>$ResultLogFile
    cat temp.ini >>$ResultLogFile

    # dump LOG
    echo "Do not dump LOG for now"
#    cp $DBPath/LOG $ResultLogFile-LOG

# dump OPTIONS 
    OPTIONS=`ls -lht $DBPath/OPTIONS-* | head -n 1 | awk '{print $NF;}'`
#    cat $OPTIONS >> $ResultLogFile-LOG

#    if [[ `echo "$ResultLogFile" | grep "Load" | wc -l` -ne 0 ]]; then
#	if [[ -f /mnt/lvm/cleanLOG.sh ]]; then
#	    /mnt/lvm/cleanLOG.sh $ResultLogFile-LOG
#	fi
#    fi
}

pwd
ulimit -n 204800 
ulimit -s 102400
echo $@
# ReadRatioSet=(0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9)
ReadProportion=0.1
OverWriteRatio=0.0
bn=4000
KVPairsNumber=10000000    #"300000000"
OperationsNumber=10000000 #"300000000"
fieldlength=400
fieldcount=10
DB_Working_Path="/mnt/g/deltakv/working"
DB_Loaded_Path="/mnt/d/deltakvload"
if [[ ! -d "/mnt/g" ]]; then
    DB_Working_Path="/mnt/lvm/deltakv/working"
    DB_Loaded_Path="/mnt/lvm/deltakv"
fi
if [[ ! -d "/mnt/lvm" && ! -d "/mnt/g" ]]; then
    DB_Working_Path="/mnt/sn640/deltakvjhli/working"
    DB_Loaded_Path="/mnt/sn640/deltakvjhli"
fi
ResultLogFolder="Exp2/ResultLogs"
DB_Name="loadedDB"
MAXRunTimes=1
Thread_number=1
RocksDBThreadNumber=16
rawConfigPath="configDir/deltakv.ini"
bucketSize="$(( 256 * 1024 ))"
cacheSize="$((1024 * 1024 * 1024))"
kvCacheSize=0
blobCacheSize=0
kdcache=0
workerThreadNumber=12
gcThreadNumber=2
deltaLogGCThreshold=0.9
deltaLogSplitGCThreshold=0.45
batchSize=100000
cacheIndexFilter="true"
paretokey="false"
nogc="false"
bloomBits=10
maxOpenFiles=1048576
gcWriteBackSize=1000
# usage

cp $rawConfigPath ./temp.ini

suffix=""
run_suffix=""
only_copy=""
only_load=""
no_store="false"

cleanFlag="false"
usekv="false"
usekd="false"
usekvkd="false"
usebkvkd="false"
keep="false"
workloadd="false"
workloade="false"
gc="true"
maxFileNumber="16"
blockSize=65536
fake="false"
nodirect="false"
nommap="false"
checkRepeat="false"
rmw="false"
sstsz=64
l1sz=256
memtable=64

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
    elif [[ $param == "no_store" ]]; then
        no_store="true"
    elif [[ "$param" == "req" || "$param" == "op" || "$param" == "readRatio" ]]; then
        echo "Param error: $param"
        exit
    elif [[ "$param" =~ ^req[0-9]+[mMkK]*$ ]]; then  # req10m
        num=`echo $param | sed 's/req//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
        KVPairsNumber=$num
    elif [[ "$param" =~ ^op[0-9]+[mMkK]*$ ]]; then
        num=`echo $param | sed 's/op//g' | sed 's/m/000000/g' | sed 's/M/000000/g' | sed 's/k/000/g' | sed 's/K/000/g'`
#        if [[ $num -eq 20000000 ]]; then
#            echo "Alert! temporary change"
#            num=50000000
#        fi
        OperationsNumber=$num
    elif [[ "$param" =~ ^fc[0-9]+$ ]]; then
        num=$(echo $param | sed 's/fc//g')
        fieldcount=$num
    elif [[ "$param" =~ ^fl[0-9]+$ ]]; then
        num=$(echo $param | sed 's/fl//g')
        fieldlength=$num
    elif [[ "$param" =~ ^readRatio[0-9].[0-9]*$ || "$param" == "readRatio1" ]]; then
        ReadProportion=`echo $param | sed 's/readRatio//g'`
    elif [[ "$param" =~ ^bn[0-9]+$ ]]; then
        bn=`echo $param | sed 's/bn//g'`
        run_suffix=${run_suffix}_${param}
    elif [[ "$param" =~ ^Exp[0-9a-zA-Z_]+$ ]]; then
        ExpID=`echo $param | sed 's/Exp//g'`
        ResultLogFolder="Exp$ExpID/ResultLogs"
        if [ ! -d $DB_Working_Path ]; then
            mkdir -p $DB_Working_Path
        fi
        if [ ! -d $DB_Loaded_Path ]; then
            mkdir -p $DB_Loaded_Path
        fi
    elif [[ "$param" =~ ^note ]]; then
        note=$(echo $param | sed 's/note//g')
        run_suffix=${run_suffix}_${note}
    elif [[ "$param" =~ ^threads[0-9]+$ ]]; then
        RocksDBThreadNumber=$(echo $param | sed 's/threads//g')
    elif [[ "$param" =~ ^gcT[0-9]+$ ]]; then
        gcThreadNumber=$(echo $param | sed 's/gcT//g')
    elif [[ "$param" =~ ^gcThres[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/gcThres//g')
        if [[ "$tmp" != "$deltaLogGCThreshold" ]]; then
            deltaLogGCThreshold=$(echo $param | sed 's/gcThres//g')
            run_suffix=${run_suffix}_${param}
        fi
    elif [[ "$param" =~ ^splitThres[0-9.]+$ ]]; then
        tmp=$(echo $param | sed 's/splitThres//g')
        if [[ "$tmp" != "$deltaLogSplitGCThreshold" ]]; then
            deltaLogSplitGCThreshold=$(echo $param | sed 's/splitThres//g')
            run_suffix=${run_suffix}_sp${tmp}
        fi
    elif [[ "$param" =~ ^workerT[0-9]+$ ]]; then
        workerThreadNumber=$(echo $param | sed 's/workerT//g')
    elif [[ "$param" =~ ^bucketSize[0-9]+$ ]]; then
        bucketSize=$(echo $param | sed 's/bucketSize//g')
        run_suffix=${run_suffix}_${param}
    elif [[ "$param" =~ ^batchSize[0-9kK]+$ ]]; then
        num=$(echo $param | sed 's/batchSize//g' | sed 's/k/000/g' | sed 's/K/000/g')
        batchSize=$num
    elif [[ "$param" =~ ^round[0-9]+$ ]]; then
        MAXRunTimes=$(echo $param | sed 's/round//g')
    elif [[ "$param" =~ ^maxFileNumber[0-9]+$ ]]; then
        maxFileNumber=$(echo $param | sed 's/maxFileNumber//g')
    elif [[ "$param" =~ ^cache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/cache//g')
        cacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_bkc${num}
    elif [[ "$param" =~ ^kvcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/kvcache//g')
        kvCacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_kvc${num}
    elif [[ "$param" =~ ^blobcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/blobcache//g')
        blobCacheSize=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_blc${num}
    elif [[ "$param" =~ ^kdcache[0-9]+$ ]]; then
        num=$(echo $param | sed 's/kdcache//g')
        kdcache=$(($num * 1024 * 1024))
        run_suffix=${run_suffix}_kdc${num}
    elif [[ "$param" =~ ^blockSize[0-9]+$ ]]; then
        blockSize=$(echo $param | sed 's/blockSize//g')
        if [[ $blockSize -ne 65536 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^gcWriteBackSize[0-9]+$ ]]; then
        gcWriteBackSize=$(echo $param | sed 's/gcWriteBackSize//g')
        if [[ $gcWriteBackSize -ne 1000 ]]; then
            run_suffix=${run_suffix}_gcwbsz${gcWriteBackSize}
        fi
    elif [[ "$param" =~ ^sst[0-9]+$ ]]; then
        sstsz=`echo $param | sed 's/sst//g'`
        if [[ $sstsz -ne 64 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^l1sz[0-9]+$ ]]; then
        l1sz=`echo $param | sed 's/l1sz//g'`
        if [[ $l1sz -ne 256 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^memtable[0-9]+$ ]]; then
        memtable=`echo $param | sed 's/memtable//g'`
        if [[ $memtable -ne 64 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^bf[0-9]+$ ]]; then
        bloomBits=`echo $param | sed 's/bf//g'`
        if [[ $bloomBits -ne 10 ]]; then
            suffix=${suffix}_$param
        fi
    elif [[ "$param" =~ ^open[0-9]+$ ]]; then
        maxOpenFiles=`echo $param | sed 's/open//g'`
        if [[ $maxOpenFiles -ne 1048576 ]]; then
            run_suffix=${run_suffix}_$param
        fi
    elif [[ "$param" =~ ^clean$ ]]; then
        cleanFlag="true"
    elif [[ "$param" == "keep" ]]; then
        keep="true"
        run_suffix=${run_suffix}_keep
    elif [[ "$param" == "workloadd" ]]; then
        workloadd="true"
        run_suffix=${run_suffix}_workloadd
    elif [[ "$param" == "workloade" ]]; then
        workloade="true"
        run_suffix=${run_suffix}_workloade
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
    elif [[ "$param" == "checkrepeat" ]]; then
        checkRepeat="true"
    elif [[ "$param" == "rmw" ]]; then
        rmw="true"
    fi
done

# KV cache

if [[ $kvCacheSize -ne 0 ]]; then
    if [[ "$usekd" == "false" && "$usebkvkd" == "false" && "$usekvkd" == "false" ]]; then
        bn=0
    fi
    deltaKVCacheSize=$(( ($kvCacheSize - $bn * 4096) / ($fieldcount * $fieldlength + $fieldcount - 1)))
    sed -i "/enableDeltaKVCache/c\\enableDeltaKVCache = true" temp.ini
    sed -i "/deltaKVCacheObjectNumber/c\\deltaKVCacheObjectNumber = $deltaKVCacheSize" temp.ini
else
    sed -i "/enableDeltaKVCache/c\\enableDeltaKVCache = false" temp.ini
fi

# KD cache (append only)

if [[ $kdcache -ne 0 ]]; then
    sed -i "/deltaLogCacheObjectNumber/c\\deltaLogCacheObjectNumber = $kdcache" temp.ini
fi

if [[ "$usekd" == "true" || "$usebkvkd" == "true" || "$usekvkd" == "true" ]]; then
    sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
    sed -i "/deltaLogMaxFileNumber/c\\deltaLogMaxFileNumber = $bn" temp.ini
    sed -i "/deltaStore_worker_thread_number_limit_/c\\deltaStore_worker_thread_number_limit_ = $workerThreadNumber" temp.ini
    sed -i "/deltaStore_gc_thread_number_limit_/c\\deltaStore_gc_thread_number_limit_ = $gcThreadNumber" temp.ini
    sed -i "/deltaLogGCThreshold/c\\deltaLogGCThreshold = $deltaLogGCThreshold" temp.ini
    sed -i "/deltaLogSplitGCThreshold/c\\deltaLogSplitGCThreshold = $deltaLogSplitGCThreshold" temp.ini
    sed -i "/deltaLogFileSize/c\\deltaLogFileSize = $bucketSize" temp.ini
    sed -i "/deltaKVWriteBatchSize/c\\deltaKVWriteBatchSize = $batchSize" temp.ini
    sed -i "/deltaStoreGcWriteBackDeltaSizeThreshold/c\\deltaStoreGcWriteBackDeltaSizeThreshold = $gcWriteBackSize" temp.ini
fi

sed -i "/blockCache/c\\blockCache = $cacheSize" temp.ini
sed -i "/blobCacheSize/c\\blobCacheSize = ${blobCacheSize}" temp.ini
sed -i "/numThreads/c\\numThreads = ${RocksDBThreadNumber}" temp.ini
sed -i "/blockSize/c\\blockSize = ${blockSize}" temp.ini

totCacheSize=$(((${kvCacheSize} + $kdcache + $cacheSize + $blobCacheSize) / 1024 / 1024));
run_suffix=${run_suffix}_tc${totCacheSize}

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

numMainSegment="$(( $KVPairsNumber * $fieldcount * $fieldlength / 5 * 6 / 1048576))"
sed -i "/numMainSegment/c\\numMainSegment = $numMainSegment" temp.ini

size="$(( $KVPairsNumber / 1000000 ))M"
if [[ $size == "0M" ]]; then
    size="$(( $KVPairsNumber / 1000 ))K"
elif [[ "$(( $KVPairsNumber % 1000000 ))" -ne 0 ]]; then
    echo "$(( $KVPairsNumber % 1000000 ))" 
    size="${size}$(( ($KVPairsNumber % 1000000) / 1000 ))K"
fi

ops="op$(($OperationsNumber / 1000000))M"
if [[ $ops == "0M" ]]; then
    ops="op$(($OperationsNumber / 1000))K"
elif [[ "$(($OperationsNumber % 1000000))" -ne 0 ]]; then
    ops="${ops}$((($OperationsNumber % 1000000) / 1000))K"
fi

if [[ $paretokey == "true" ]]; then
    suffix=${suffix}_fc${fieldcount}_paretokey_${size}
else
    suffix=${suffix}_fc${fieldcount}_fl${fieldlength}_${size}
fi

if [[ $nogc == "true" ]]; then
    sed -i "/deltaStoreEnableGC/c\\deltaStoreEnableGC = false" temp.ini 
fi

maxKeyValueSize=$(( (${fieldcount} * ${fieldlength} + 4095) / 4096 * 4096))
sed -i "/maxKeyValueSize/c\\maxKeyValueSize = $maxKeyValueSize" temp.ini 
sed -i "/memtable/c\\memtable = $(( $memtable * 1024 * 1024 ))" temp.ini 
sed -i "/targetFileSizeBase/c\\targetFileSizeBase = $(( $sstsz * 1024 ))" temp.ini 
sed -i "/maxBytesForLevelBase/c\\maxBytesForLevelBase = $(( $l1sz * 1024 ))" temp.ini 
sed -i "/bloomBits/c\\bloomBits = $bloomBits" temp.ini 
sed -i "/maxOpenFiles/c\\maxOpenFiles = $maxOpenFiles" temp.ini 

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

if [[ "$only_load" != "true" ]]; then
    if [[ "$usekd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/kd/raw/g")"
    elif [[ "$usebkvkd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/bkvkd/bkv/g")"
    elif [[ "$usekvkd" == "true" ]]; then
        loadedDB="$(echo $loadedDB | sed "s/kvkd/kv/g")"
    fi
    echo "Real loadedDB $loadedDB"
fi

if [[ ! -d $loadedDB ]]; then
    echo "no loaded db $loadedDB"
fi

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
    ./ycsbc_release -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath >${output_file}
    #    gdb --args ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase load -configpath $configPath # > ${output_file}
    retvalue=$?
    loaded="true"
    echo "output at $output_file"
    if [[ "$no_store" == "true" ]]; then
        echo "no store, exit"
        exit
    fi

    t_output_file=$output_file
    log_db_status $workingDB $t_output_file
    output_file=${t_output_file}

    if [[ $retvalue -ne 0 ]]; then
	echo "Exit. return number $retvalue"
	exit
    fi

    # Running Update
    echo "Read loaded DB to complete compaction"
    if [ -f workload-temp.spec ]; then
        rm -rf workload-temp.spec
        echo "Deleted old workload spec"
    fi

    SPEC="./workload-temp-prepare.spec"
    cp workloads/workloadTemplate.spec $SPEC 
    sed -i "9s/NaN/$KVPairsNumber/g" $SPEC 
    sed -i "10s/NaN/10000000/g" $SPEC 
#    sed -i "10s/NaN/10000/g" $SPEC 
    sed -i "15s/0/1/g" $SPEC
    sed -i "16s/0/0/g" $SPEC
    sed -i "24s/NaN/$fieldcount/g" $SPEC
    sed -i "25s/NaN/$fieldlength/g" $SPEC
    echo "<===================== Prepare =====================>"
    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P ${SPEC} -phase run -configpath $configPath > ${output_file}-prepare
    echo "output at ${output_file}-prepare"
    rm -f $SPEC

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
    UpdateProportion=$(echo "" | awk '{print 1.0-'"$ReadProportion"';}')
    echo "Modify spec, Read/Update ratio = $ReadProportion:$UpdateProportion"
    cp workloads/workloadTemplate.spec ./workload-temp.spec
    sed -i "9s/NaN/$KVPairsNumber/g" workload-temp.spec
    sed -i "10s/NaN/$OperationsNumber/g" workload-temp.spec

    if [[ "$workloadd" == "true" ]]; then
        sed -i "23s/zipfian/latest/g" workload-temp.spec
        sed -i "20s/0/0.05/g" workload-temp.spec
        ReadProportion=0.95
        UpdateProportion=0
    fi

    if [[ "$workloade" == "true" ]]; then
        sed -i "19s/0/0.95/g" workload-temp.spec
        sed -i "20s/0/0.05/g" workload-temp.spec
        ReadProportion=0
        UpdateProportion=0
    fi

    sed -i "15s/0/$ReadProportion/g" workload-temp.spec
    if [[ "$rmw" == "false" ]]; then
        sed -i "16s/0/$UpdateProportion/g" workload-temp.spec
    else
        sed -i "17s/0/$UpdateProportion/g" workload-temp.spec
    fi
    sed -i "24s/NaN/$fieldcount/g" workload-temp.spec
    sed -i "25s/NaN/$fieldlength/g" workload-temp.spec
    if [[ $paretokey == "true" ]]; then
        sed -i "/field_len_dist/c\\field_len_dist=paretokey" workload-temp.spec
    fi
    echo "<===================== Modified spec file content =====================>"
    cat workload-temp.spec | head -n 25 | tail -n 17

    fileprefix=$ResultLogFolder/Rd-$ReadProportion-Ud-$UpdateProportion-${run_suffix}
    if [[ "$rmw" == "true" ]]; then
        fileprefix=$ResultLogFolder/Rd-$ReadProportion-RMW-$UpdateProportion-${run_suffix}
    fi

    output_file=$(generate_file_name ${fileprefix}-gcT${gcThreadNumber}-workerT${workerThreadNumber}-fixB2k)
    if [[ "$usekd" != "true" && "$usekvkd" != "true" && "$usebkvkd" != "true" ]]; then
        output_file=`generate_file_name ${fileprefix}`
    else
        t="${fileprefix}"
        if [[ "$gcThreadNumber" != "2" ]]; then
            t="${t}-gcT${gcThreadNumber}" 
        fi
        if [[ "$workerThreadNumber" != "8" ]]; then
            t="${t}-workerT${workerThreadNumber}" 
        fi
        t="${t}-fixB2k"
        output_file=`generate_file_name ${t}`
    fi

    echo "output at $output_file"
    if [[ "$checkRepeat" == "true" ]]; then
        if [[ `echo $output_file | grep "Round-1" | wc -l` -eq 0 ]]; then
            echo "exit because of check repeated: $output_file"
            exit
        fi
    fi

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

    ./ycsbc -db rocksdb -dbfilename $workingDB -threads $Thread_number -P workload-temp.spec -phase run -configpath $configPath >$output_file
    retvalue=$?
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
