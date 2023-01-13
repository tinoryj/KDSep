#!/bin/bash

############ 
# Units: 1 K = 1000, 1 GiB = 2^30 bytes
# Reads consist of: SST reads, blob reads (considering both compaction and Get()), delta OP/GC reads, vLog reads
# Writes consist of: Compaction writes (considering both SST and blob), flush writes, WAL writes, delta OP/GC writes, vLog writes
# 
# sst_r   - (GiB) SST reads 
# sst_rc  - (K)   The number of SST reads 
# blob_r  - (GiB) Blob reads
# blob_rc - (K)   The number of blob reads
# d_gc_r  - (GiB) Delta GC physical reads
# d_op_r  - (GiB) Delta OP physical reads
# d_gc_rc - (K)   The number of delta GC reads
# g_op_rc - (K)   The number of delta OP reads
# v_r     - (GiB) vLog reads
# v_rc    - (K)   The number of vLog reads
#
# rock_io - (GiB) Total I/O in RocksDB, including SST reads, blob reads, compaction writes, flush writes, WAL writes
# d_rw    - (GiB) Total I/O in deltas, including delta OP/GC reads/writes
# v_rw    - (GiB) Total I/O in vLog, including vLog reads/writes 
# tot_rw  - (GiB) Total I/O = rock_io + d_rw + v_rw

line1="ind_rc"$'\t'"fil_rc"$'\t'"dat_rc"$'\t'"ind_r"$'\t'"fil_r"$'\t'"dat_r"$'\t'"ind_pr"$'\t'"fil_pr"$'\t'"dat_pr"$'\t'

echo "$line1$line2$line3$line4" 
OUTPUT="reads.csv"

for file in $*; do
    ind_rc=`grep "rocksdb.block.cache.index.add" $file | awk '{t+=$NF;} END {print t/1000000;}' | cut -c1-7`
    fil_rc=`grep "rocksdb.block.cache.filter.add" $file | awk '{t+=$NF;} END {print t/1000000;}' | cut -c1-7`
    dat_rc=`grep "rocksdb.block.cache.data.add" $file | awk '{t+=$NF;} END {print t/1000000;}' | cut -c1-7`

    ind_r=`grep "rocksdb.block.cache.index.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
    fil_r=`grep "rocksdb.block.cache.filter.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
    dat_r=`grep "rocksdb.block.cache.data.bytes.insert" $file | awk '{t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`

    ind_pr=`echo $ind_rc $ind_r | awk '{t+=$2/$1*1024*1024/1000000;} END {printf "%f", t;}' | cut -c1-7`
    fil_pr=`echo $fil_rc $fil_r | awk '{t+=$2/$1*1024*1024/1000000;} END {printf "%f", t;}' | cut -c1-7`
    dat_pr=`echo $dat_rc $dat_r | awk '{t+=$2/$1*1024*1024/1000000;} END {printf "%f", t;}' | cut -c1-7`

    act_bl=`grep "actual.blob.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    act_bl_count=`grep "blob.read.count\|blob.read.large.count" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1000;}' | cut -c1-7`

    act_sst=`echo $act_sst $act_bl | awk '{print $1-$2;}' | cut -c1-7`
    act_sst_count=`echo $act_sst_count $act_bl_count | awk '{print $1-$2;}' | cut -c1-7`

    comp_w=`grep "rocksdb.compact.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    flush=`grep "rocksdb.flush.write.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    wal=`grep "rocksdb.wal.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`

    comp_w_cnt=`grep "rocksdb.compact.write.count" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000;}' | cut -c1-7`
    flush_cnt=`grep "rocksdb.flush.write.count" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000;}' | cut -c1-7`
    wal_cnt=`grep "rocksdb.write.wal" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000;}' | cut -c1-7`

    rock_io=`echo $act_sst $act_bl $comp_w $flush $wal | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}' | cut -c1-7`

    d_gc_r=`grep "dStore GC Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    d_gc_w=`grep "dStore GC Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    d_op_r=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    d_op_w=`grep "dStore OP Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`

    d_gc_r_cnt=`grep "dStore GC Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}' | cut -c1-7`
    d_gc_w_cnt=`grep "dStore GC Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}' | cut -c1-7`
    d_op_r_cnt=`grep "dStore OP Physical read bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}' | cut -c1-7`
    d_op_w_cnt=`grep "dStore OP Physical write bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}' | cut -c1-7`

    d_rw=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$7;} END {print t / 1024.0 / 1024.0 / 1024.0;}' | cut -c1-7`
    d_rw_cnt=`grep "dStore.*Physical.*bytes" $file | awk 'BEGIN {t=0;} {t+=$10;} END {print t / 1000;}' | cut -c1-7`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}' | cut -c1-7` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}' | cut -c1-7`
    fi

    v_r=`grep "Total disk read" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
    v_w=`grep "Total disk write" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
    v_rw=`grep "Total disk" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t/1024/1024/1024;}' | cut -c1-7`
    v_r_cnt=`grep "GetValueTime" $file | awk 'BEGIN {t=0;} {t+=$(NF-4);} END {print t/1000;}' | cut -c1-7`
    v_w_cnt=`grep "Flush w/o GC" $file | awk 'BEGIN {t=0;} {t+=$(NF-4);} END {print t/1000;}' | cut -c1-7`

    tot_rw=`echo $d_rw $v_rw $rock_io | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}' | cut -c1-7`

    line1="$act_sst"$'\t'"$act_sst_count"$'\t'"$act_bl"$'\t'"$act_bl_count"$'\t'
    line2="$d_gc_r"$'\t'"$d_op_r"$'\t'"$d_gc_r_cnt"$'\t'"$d_op_r_cnt"$'\t'
    line3="$v_r"$'\t'"$v_r_cnt"$'\t'
    line4="$rock_io"$'\t'"$d_rw"$'\t'"$v_rw"$'\t'"$tot_rw"$'\t'"$thpt"$'\t'"$file"
    line1="$ind_rc"$'\t'"$fil_rc"$'\t'"$dat_rc"$'\t'"$ind_r"$'\t'"$fil_r"$'\t'"$dat_r"$'\t'"$ind_pr"$'\t'"$fil_pr"$'\t'"$dat_pr"$'\t'"$thpt"$'\t'"$file"
    echo "$line1" 
done
