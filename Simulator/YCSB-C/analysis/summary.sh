#!/bin/bash

DN=`dirname $0`
source $DN/common.sh

concatFunc "sst_sz" "rss" "c_sz" "read_lt" "rmw_lt" "tot_rw" "thpt" "file"

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

    rock_r=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    rock_w=`grep "rocksdb.compact.write.bytes\|rocksdb.flush.write.bytes\|rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1024 / 1024 / 1024;}'`
    rock_io=`echo $rock_r $rock_w | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}'`
    v_rw=`grep "Total disk" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}'`
    d_rw=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}'`
    tot_rw=`echo $d_rw $v_rw $rock_io | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}'`

    concatFunc "$sst_sz" "$rss" "$c_sz" "$readLatency" "$mergeLatency" "$tot_rw" "$thpt" "$file"
done
