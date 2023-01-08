#!/bin/bash

echo "rd_rock"$'\t'"r_blob"$'\t'"actual"$'\t'"act_bl"$'\t'"comp_w"$'\t'"flush"$'\t'"wal"$'\t'"rock_io"$'\t'"sst_sz"$'\t'"rss_gb"$'\t'"cache-g"$'\t'"d_rw"$'\t'"br_sz"$'\t'"v_rw"$'\t'"thpt"$'\t'"fname"

for file in $*; do
    rd_rock=`grep "rocksdb.last.level.read.bytes\|rocksdb.non.last.level.read.bytes" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print $NF / 1024 / 1024 / 1024;}' | cut -c1-7`
    r_blob=`grep "rocksdb.blobdb.blob.file.bytes.read " $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    actual=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    act_bl=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    comp_w=`grep "rocksdb.compact.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    flush=`grep "rocksdb.flush.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    wal=`grep "rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    rock_io=`echo $actual $comp_w $flush $wal | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}' | cut -c1-7`

    sst_num=`grep "sst, num" $file | awk 'BEGIN {t=0;} {t=$1;} END {print t / 1024.0;}' | cut -c1-7`
    rss=`grep "resident" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}' | cut -c1-7`
    deltaRead=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    deltaSize=`grep "MiB delta" $file | awk 'BEGIN {t=1000000000;} {if ($1<t) t=$1;} END {print t / 1024.0;}' | cut -c1-7`

    br_count=`grep "rocksdb.blobdb.blob.file.read.micros" $file | awk 'BEGIN {t=0;} {t=$(NF-3);} END {print t;}'`
    br_sz=`echo "$act_bl $br_count" | awk '{print $1/($2+0.01)*1024*1024*1024;}'`


    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}' | cut -c1-7` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}' | cut -c1-7`
    fi
    cache_gb=`grep "GetUsage()" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`

    v_rw=`grep "Disk     0" $file | awk 'BEGIN {t=0;} {t+=$NF + $(NF-2);} END {print t/1024/1024/1024;}' | cut -c1-7`
#    echo $num1
#    echo $num2
    echo "$rd_rock"$'\t'"$r_blob"$'\t'"$actual"$'\t'"$act_bl"$'\t'"$comp_w"$'\t'"$flush"$'\t'"$wal"$'\t'"$rock_io"$'\t'"$sst_num"$'\t'"$rss"$'\t'"$cache_gb"$'\t'"$deltaRead"$'\t'"$br_sz"$'\t'"$v_rw"$'\t'"$thpt"$'\t'"$file"
done
