#!/bin/bash
DB_Name="loadedDB"
Thread_number=1
ulimit -n 1048576
ulimit -s 102400

if [ -d $path$DB_Name ]; then
    rm -rf $DB_Name
    echo "Deleted old database files"
fi

# cp configDir/rocksdb_test_config.ini ./temp.ini
# sed -i "30s/NaN/$1/g" temp.ini

echo "<===================== Loading the database start =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase load -configpath deltakv.ini #2>load.log
echo "<===================== Loading the database done =====================>"
# exit
echo "<===================== Benchmark the database start =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase run -configpath deltakv.ini #2>test.log
echo "<===================== Benchmark the database done =====================>"

# -db rocksdb -dbfilename loadedDB -threads 1 -P workloadTemp.spec -phase load -configpath deltakv.ini
# -db rocksdb -dbfilename loadedDB -threads 1 -P workloadTemp.spec -phase run -configpath deltakv.ini
# thread apply all bt
