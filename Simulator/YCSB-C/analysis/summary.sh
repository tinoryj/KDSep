#!/bin/bash

DN=`dirname $0`
source $DN/common.sh

concatFunc "sst_sz" "rss" "c_sz" "read_lt" "rmw_lt" "thpt" "file"

files=$*

if [[ "$sortedByTime" == "true" ]]; then
    files=`ls -lht $* | awk '{print $NF;}'`
fi

for file in ${files[@]}; do
    readLatency=`grep "per read" $file | awk 'BEGIN {t=0;} {t = $(NF-1) * 1000;} END {print t;}'`
    mergeLatency=`grep "per update" $file | awk 'BEGIN {t=0;} {t = $(NF-1) * 1000;} END {print t;}'`
    if [[ $(echo "$mergeLatency" | grep "nan" | wc -l) -ne 0 ]]; then
        mergeLatency=`grep "per R-M-W" $file | awk 'BEGIN {t=0;} {t = $(NF-1) * 1000;} END {print t;}'`
    fi
    sst_sz=`grep "sst, num" $file | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1024.0;}'`
    rss=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`

    c_sz=`grep "GetUsage()" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}'`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    concatFunc "$sst_sz" "$rss" "$c_sz" "$readLatency" "$mergeLatency" "$thpt" "$file"
done
