#!/bin/bash

#NUM=`ps -e | grep "buildRelease" | wc -l `
#
#while [[ $NUM -ge 1 ]]; do
#    NUM=`ps -e | grep "buildRelease" | wc -l `
#    echo "`date` $NUM $NUM2"
#    sleep 2
#done

DIR=$(dirname $(realpath $0))

cd ${DIR}/../../../KDSep
scripts/buildRelease.sh
cd ${DIR}/../
cp Makefile_release Makefile
make clean
make
cp ycsbc ycsbc_release
