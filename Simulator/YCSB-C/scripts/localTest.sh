#!/bin/bash
DB_Name="loadedDB"
Thread_number=1
ulimit -n 65536
if [ -d $DB_Name ]; then
    rm -rf $DB_Name
    echo "Deleted old database files"
fi

# cp configDir/rocksdb_test_config.ini ./temp.ini
# sed -i "30s/NaN/$1/g" temp.ini

echo "<===================== Loading the database =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase load -configpath deltakv_kv_kd.ini #>load.log

echo "<===================== Benchmark the database start =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase run -configpath deltakv_kv_kd.ini 2>test.log
echo "<===================== Benchmark the database done =====================>"

# -db rocksdb -dbfilename loadedDB -threads 1 -P workloadTemp.spec -phase load -configpath temp.ini
# -db rocksdb -dbfilename loadedDB -threads 1 -P workloadTemp.spec -phase run -configpath temp.ini
# thread apply all bt
