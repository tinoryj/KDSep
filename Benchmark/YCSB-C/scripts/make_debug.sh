#!/bin/bash

#NUM=`ps -e | grep "buildRelease" | wc -l `
#
#while [[ $NUM -ge 1 ]]; do
#    NUM=`ps -e | grep "buildRelease" | wc -l `
#    echo "`date` $NUM $NUM2"
#    sleep 2
#done

#cd /home/jhli/workspace/KDSep/KDSep-work/KDSep

DIR=$(dirname $(realpath $0))

cd ${DIR}/../../../KDSep
scripts/buildDebug.sh
cd ${DIR}/../
cp Makefile_debug Makefile
make clean
make
cp ycsbc ycsbc_debug
