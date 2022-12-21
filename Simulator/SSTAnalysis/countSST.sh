#!/bin/bash

if [ $1 == "clean" ]; then
    rm -rf countSSTInfo
    rm -rf countSSTInfoLevel
    rm -rf *log
    exit
fi

if [ $1 == "make" ]; then
    if [ ! -f countSSTInfo ]; then
        g++ -o countSSTInfo countSSTInfo.cpp
    fi

    if [ ! -f countSSTInfoLevel ]; then
        g++ -o countSSTInfoLevel countSSTInfoLevel.cpp
    fi
    exit
fi

targetAnalysisPath=$1
runningCount=$2

manifestFile=$(ls $targetAnalysisPath/MANIFEST-*)
./ldb manifest_dump --path=$manifestFile >${runningCount}_manifest.log

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files: "
echo $sstablesSet

for SSTable in ${sstablesSet[@]}; do
    SSTFileName=${SSTable:0-10:6}
    ./sst_dump --file=$SSTable --output_hex --command=scan >>$SSTFileName.log
    echo "SST ID = "$SSTFileName >>${runningCount}_SSTablesAnalysis.log
    ./countSSTInfo $SSTFileName.log >>${runningCount}_SSTablesAnalysis.log
    rm -rf $SSTFileName.log
done

./countSSTInfoLevel ${runningCount}_manifest.log ${runningCount}_SSTablesAnalysis.log >${runningCount}_levelBasedCount.log
