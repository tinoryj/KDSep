#!/bin/bash

if [ $1 == "clean" ]; then
    rm -rf countSSTInfo
    rm -rf SSTablesContentLog.log
    rm -rf SSTablesAnalysis.log
    exit
fi

targetAnalysisPath=$1

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files:"
echo $sstablesSet

for SSTable in ${sstablesSet[@]}; do
    ./sst_dump --file=$SSTable --output_hex --command=scan >>SSTablesContentLog.log
done

if [ ! -f countSSTInfo ]; then
    g++ -o countSSTInfo countSSTInfo.cpp
fi

./countSSTInfo ./SSTablesContentLog.log >SSTablesAnalysis.log
