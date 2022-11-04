#!/bin/bash
DB_Name="loadedDB"
Thread_number=1

echo "<===================== Loading the database =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase load -configpath configDir/leveldb_config.ini

echo "<===================== Benchmark the database start =====================>"
./ycsbc -db rocksdb -dbfilename $DB_Name -threads $Thread_number -P workloadTemp.spec -phase run -configpath configDir/leveldb_config.ini
echo "<===================== Benchmark the database done =====================>"
