#!/bin/bash

#NUM=`ps -e | grep "buildRelease" | wc -l `
#
#while [[ $NUM -ge 1 ]]; do
#    NUM=`ps -e | grep "buildRelease" | wc -l `
#    echo "`date` $NUM $NUM2"
#    sleep 2
#done

#cd /home/jhli/workspace/deltakv/DeltaKV-work/RocksDBAddons

DIR=`dirname $0`
echo $DIR

cd ${DIR}/../../../RocksDBAddons
scripts/buildRelease.sh
cd ${DIR}/../
cp Makefile_release Makefile
make clean ; make
cp ycsbc ycsbc_release
