#!/bin/bash

for file in $*; do
    sst_size=`grep "sst, num = " $file | awk '{print $1;}'`
    keys=`grep "rocksdb.number.keys.written" $file | awk '{print $NF;}'`
    num=`echo "$keys / $sst_size " | bc`
    echo $sst_size $num $file
done
