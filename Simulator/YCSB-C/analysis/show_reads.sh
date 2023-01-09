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

line1="sst_r"$'\t'"sst_rc"$'\t'"blob_r"$'\t'"blob_rc"$'\t'
line2="d_gc_r"$'\t'"d_op_r"$'\t'"d_gc_rc"$'\t'"d_op_rc"$'\t'
line3="v_r"$'\t'"v_rc"$'\t'
line4="rock_io"$'\t'"d_rw"$'\t'"v_rw"$'\t'"tot_rw"$'\t'"thpt"$'\t'"fname"

echo "$line1$line2$line3$line4" 
OUTPUT="reads.csv"

line1="act_r,r_cnt,act_bl,bl_cnt,"
line2="d_gc_r,d_op_r,d_gc_rc,d_op_rc,"
line3="v_r,v_rc,"
line4="rock_io,d_rw,v_rw,tot_rw,thpt,fname"
echo "$line1$line2$line3$line4" > $OUTPUT 

for file in $*; do
    act_sst=`grep "actual.read.bytes" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1024 / 1024 / 1024;}' | cut -c1-7`
    act_sst_count=`grep "rocksdb.*last.level.read.count" $file | awk 'BEGIN {t=0;} {t+=$NF;} END {print t / 1000;}' | cut -c1-7`

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

    rock_io=`echo $act_r $comp_w $flush $wal | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}' | cut -c1-7`

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

    line1="act_r"$'\t'"r_cnt"$'\t'"act_bl"$'\t'"bl_cnt"$'\t'
    line2="d_gc_r"$'\t'"d_op_r"$'\t'"d_gc_rc"$'\t'"d_op_rc"$'\t'
    line3="v_r"$'\t'"v_rc"$'\t'
    line4="rock_io"$'\t'"d_rw"$'\t'"v_rw"$'\t'"tot_rw"$'\t'"thpt"$'\t'"fname"

    line1="$act_sst"$'\t'"$act_sst_count"$'\t'"$act_bl"$'\t'"$act_bl_count"$'\t'
    line2="$d_gc_r"$'\t'"$d_op_r"$'\t'"$d_gc_r_cnt"$'\t'"$d_op_r_cnt"$'\t'
    line3="$v_r"$'\t'"$v_r_cnt"$'\t'
    line4="$rock_io"$'\t'"$d_rw"$'\t'"$v_rw"$'\t'"$tot_rw"$'\t'"$thpt"$'\t'"$file"
    echo "$line1$line2$line3$line4" 

    line1="$act_sst,$act_sst_count,$act_bl,$act_bl_count,"
    line2="$d_gc_r,$d_op_r,$d_gc_r_cnt,$d_op_r_cnt,"
    line3="$v_r,$v_r_cnt,"
    line4="$rock_io,$d_rw,$v_rw,$tot_rw,$thpt,$file"

    echo "$line1$line2$line3$line4" >> $OUTPUT 
done
