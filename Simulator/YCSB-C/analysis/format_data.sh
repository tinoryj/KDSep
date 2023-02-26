#!/bin/bash

echo "readRatio valueSize scheme throughput readLatency mergeLatency"

for file in $*; do
    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    readRatio=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Read") print $(i+1);}}'`
    flength=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "fl") print $i "0";}}' | sed "s/fl//g"`
    scheme=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "Result") print $(i+1);}}' | sed "s/fl//g"`
    readLatency=`grep "per read" $file | awk '{print $(NF-1) * 1000;}'`
    mergeLatency=`grep "per update" $file | awk '{print $(NF-1) * 1000;}'`

    echo $readRatio $flength $scheme $thpt $readLatency $mergeLatency
done
