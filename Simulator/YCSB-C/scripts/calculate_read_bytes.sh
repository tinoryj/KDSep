#!/bin/bash

echo "r_lsm"$'\t'"r_blob"$'\t'"sst_sz"$'\t'"rss_gb"$'\t'"cache-g"$'\t'"d_read"$'\t'"d_size"$'\t'"thpt"$'\t'"fname"

for file in $*; do
    num1=`grep "rocksdb.last.level.read.bytes" $file | awk '{print $NF;}'`
    num2=`grep "rocksdb.non.last.level.read.bytes" $file | awk '{print $NF;}'`
    r_blob=`grep "rocksdb.blobdb.blob.file.bytes.read" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    sst_num=`grep "sst, num" $file | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1024.0;}'`
    resident=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
    deltaRead=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    deltaSize=`grep "MiB delta" $file | awk 'BEGIN {t=1000000000;} {if ($1<t) t=$1;} END {print t / 1024.0;}'`
    thpt=`grep "workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    cache_gb=`grep "GetUsage()" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`
#    echo $num1
#    echo $num2
    echo "`echo $num1 $num2 | awk '{print ($1+$2) / 1024.0 / 1024.0 / 1024.0;}' `"$'\t'"$r_blob"$'\t'"$sst_num"$'\t'"$resident"$'\t'"$cache_gb"$'\t'"$deltaRead"$'\t'"$deltaSize"$'\t'"$thpt"$'\t'"$file"
done
