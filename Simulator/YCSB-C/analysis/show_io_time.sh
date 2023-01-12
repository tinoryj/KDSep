#!/bin/bash

############ 
# Units: seconds 
# 
# roc_r   - RocksDB reads
# roc_w   - RocksDB writes (not including fsync) 
# roc_syn - RocksDB fsync 
# d_gc_r  - Delta GC reads 
# d_gc_w  - Delta GC writes 
# d_op_r  - Delta OP reads 
# d_gc_w  - Delta OP writes 
# v_r     - vLog reads
# v_w     - vLog writes 
#
# roc_io  - RocksDB I/O (= roc_r + roc_w + roc_syn) 
# d_io_t  - Delta I/O (= d_gc_r + d_gc_w + d_op_r + d_op_w) 
# v_io_t  - vLog I/O (= v_r + v_w) 
# tot     - Total I/O time (= roc_io + d_io_t + v_io_t) 
# act_t   - Actual run time 

line1="roc_r"$'\t'"roc_w"$'\t'"roc_syn"$'\t'
line2="d_gc_r"$'\t'"d_gc_w"$'\t'"d_op_r"$'\t'"d_op_w"$'\t'
line3="v_r"$'\t'"v_w"$'\t'
line4="roc_io"$'\t'"d_io_t"$'\t'"v_io_t"$'\t'"tot"$'\t'"act_t"$'\t'"thpt"$'\t'"fname"

echo "$line1$line2$line3$line4" 
OUTPUT="reads.csv"

#line1="roc_r"$'\t'"roc_w"$'\t'"roc_syn"$'\t'
#line2="d_gc_r"$'\t'"d_gc_w"$'\t'"d_op_r"$'\t'"d_op_w"$'\t'
#line3="v_r"$'\t'"v_w"$'\t'
#line4="roc_io"$'\t'"d_io_t"$'\t'"v_io_t"$'\t'"tot"$'\t'"thpt"$'\t'"fname"
#echo "$line1$line2$line3$line4" > $OUTPUT 

for file in $*; do
    fname=$file

    comp_r=`grep "rocksdb.compact.read.nanos" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000000;}' | cut -c1-7`
    comp_w=`grep "rocksdb.compact.write.nanos" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000000;}' | cut -c1-7`

    roc_r=`grep "rocksdb.file.reader.read.micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`
    roc_w=`grep "file_write_micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`
    roc_syn=`grep "file_fsync_micros" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t / 1000000;}' | cut -c1-7`


    d_gc_r=`grep "gc read" $file | awk 'BEGIN {t=0;} {t=$5;} END {print t / 1000000;}' | cut -c1-7`
    d_gc_w=`grep "gc write" $file | awk 'BEGIN {t=0;} {t=$5;} END {print t / 1000000;}' | cut -c1-7`
    d_op_r=`grep "worker-get-file-io" $file | awk 'BEGIN {t=0;} {t=$3;} END {print t / 1000000;}' | cut -c1-7`
    d_op_w=`grep "worker-put-file-write" $file | awk 'BEGIN {t=0;} {t=$3;} END {print t / 1000000;}' | cut -c1-7`

    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}' | cut -c1-7` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
	act_t=$loadtime
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}' | cut -c1-7`
    else
	act_t=`grep "run time" $file | sed 's/us//g' | awk 'BEGIN {t=0;} {t=$NF/1000000;} END {print t;}'`
    fi

    v_r=`grep "GetValueTime" $file | awk 'BEGIN {t=0;} {t+=$3;} END {print t/1000000;}' | cut -c1-7`
    v_w=`grep "Flush w/o GC" $file | awk 'BEGIN {t=0;} {t+=$6;} END {print t/1000000;}' | cut -c1-7`

    roc_io=`echo $roc_r $roc_w $roc_syn | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}' | cut -c1-7`
    d_io_t=`echo $d_gc_r $d_gc_w $d_op_r $d_op_w | awk '{t=0; for (i=1; i<=NF;i++) if ($1!=0) t+=$i; print t;}' | cut -c1-7`
    v_io_t=`echo $v_r $v_w | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}' | cut -c1-7`
    tot=`echo $roc_io $d_io_t $v_io_t | awk '{for (i=1;i<=NF;i++) t+=$i; print t;}' | cut -c1-7`

    line1="$roc_r"$'\t'"$roc_w"$'\t'"$roc_syn"$'\t'
    line2="$d_gc_r"$'\t'"$d_gc_w"$'\t'"$d_op_r"$'\t'"$d_op_w"$'\t'
    line3="$v_r"$'\t'"$v_w"$'\t'
    line4="$roc_io"$'\t'"$d_io_t"$'\t'"$v_io_t"$'\t'"$tot"$'\t'"$act_t"$'\t'"$thpt"$'\t'"$fname"

    echo "$line1$line2$line3$line4" 

#    line1="$act_sst,$act_sst_count,$act_bl,$act_bl_count,"
#    line2="$d_gc_r,$d_op_r,$d_gc_r_cnt,$d_op_r_cnt,"
#    line3="$v_r,$v_r_cnt,"
#    line4="$rock_io,$d_rw,$v_rw,$tot_rw,$thpt,$file"
#
#    echo "$line1$line2$line3$line4" >> $OUTPUT 
done
