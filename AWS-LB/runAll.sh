#!/bin/bash
source setupDbConnection.sh

echo "=========================================================================================="
echo updateMetadata
echo .
pushd updateMetadata
./updateMetadata.sh
popd

echo "=========================================================================================="
echo importStats
echo .
pushd importStats
./importStats.sh
popd

echo "=========================================================================================="
echo initialSQL
echo .
pushd initialSQL
./initialSQL.sh
popd

echo "=========================================================================================="
echo createReports 
echo .
pushd reports
./createReports.sh
popd

# De-implemented 10-Aug-2021
# echo "=========================================================================================="
# echo User Feedback progress reports
# echo .
# pushd ufReports
# ./extract.sh
# popd
