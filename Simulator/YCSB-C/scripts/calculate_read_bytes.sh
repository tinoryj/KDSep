#!/bin/bash

echo "r_lsm"$'\t'"r_blob"$'\t'"actual"$'\t'"act_bl"$'\t'"sst_sz"$'\t'"rss_gb"$'\t'"cache-g"$'\t'"d_read"$'\t'"d_size"$'\t'"br_sz"$'\t'"v_read"$'\t'"thpt"$'\t'"fname"

for file in $*; do
    num1=`grep "rocksdb.last.level.read.bytes" $file | awk '{print $NF;}'`
    num2=`grep "rocksdb.non.last.level.read.bytes" $file | awk '{print $NF;}'`
    actual=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    actual_blob=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    r_blob=`grep "rocksdb.blobdb.blob.file.bytes.read" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    sst_num=`grep "sst, num" $file | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1024.0;}' | cut -c1-7`
    resident=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}' | cut -c1-7`
    deltaRead=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    deltaSize=`grep "MiB delta" $file | awk 'BEGIN {t=1000000000;} {if ($1<t) t=$1;} END {print t / 1024.0;}' | cut -c1-7`

    br_count=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t;}' | cut -c1-7`
    br_sz=`echo "$actual_blob $br_count" | awk '{print $1/($2+0.01)*1024*1024;}'`

    thpt=`grep "workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}' | cut -c1-7` 
    if [[ "$thpt" == "0" ]]; then
        thpt=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}' | cut -c1-7`
    fi
    cache_gb=`grep "GetUsage()" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`

    v_read=`grep "Disk     0" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
#    echo $num1
#    echo $num2
    echo "`echo $num1 $num2 | awk '{print ($1+$2) / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`"$'\t'"$r_blob"$'\t'"$actual"$'\t'"$actual_blob"$'\t'"$sst_num"$'\t'"$resident"$'\t'"$cache_gb"$'\t'"$deltaRead"$'\t'"$deltaSize"$'\t'"$br_sz"$'\t'"$v_read"$'\t'"$thpt"$'\t'"$file"
done
