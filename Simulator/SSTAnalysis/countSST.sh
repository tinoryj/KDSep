#!/bin/bash

if [ $1 == "clean" ]; then
    rm -rf countSSTInfo
    rm -rf countSSTInfoLevel
    rm -rf *log
    exit
fi

targetAnalysisPath=$1

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files: "
echo $sstablesSet

if [ ! -f countSSTInfo ]; then
    g++ -o countSSTInfo countSSTInfo.cpp
fi

if [ ! -f countSSTInfoLevel ]; then
    g++ -o countSSTInfoLevel countSSTInfoLevel.cpp
fi

for SSTable in ${sstablesSet[@]}; do
    SSTFileName=${SSTable:0-10:6}
    ./sst_dump --file=$SSTable --output_hex --command=scan >>$SSTFileName.log
    echo "SST ID = "$SSTFileName >>SSTablesAnalysis.log
    ./countSSTInfo $SSTFileName.log >>SSTablesAnalysis.log
done

manifestFile=$(ls $targetAnalysisPath/MANIFEST-*)

./ldb manifest_dump --path=$manifestFile >manifest.log
./countSSTInfoLevel manifest.log SSTablesAnalysis.log >levelBasedCount.log
