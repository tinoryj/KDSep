#!/bin/bash

echo "read_sst_gb sst_size_gb resident_gb delta_read delta_size filename"

for file in $*; do
    num1=`grep "rocksdb.last.level.read.bytes" $file | awk '{print $NF;}'`
    num2=`grep "rocksdb.non.last.level.read.bytes" $file | awk '{print $NF;}'`
    sst_num=`grep "sst, num" $file | awk '{print $1 / 1024.0;}'`
    resident=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
    deltaRead=`grep "dStore OP Physical read bytes" $file | awk '{print $7 / 1024.0 / 1024.0 / 1024.0;}'`
    deltaSize=`grep "MiB delta" $file | awk 'BEGIN {t=1000000000;} {if ($1<t) t=$1;} END {print t / 1024.0;}'`
#    echo $num1
#    echo $num2
    echo "`echo $num1 $num2 | awk '{print ($1+$2) / 1024.0 / 1024.0 / 1024.0;}' ` $sst_num $resident $deltaRead $deltaSize $file"
done
