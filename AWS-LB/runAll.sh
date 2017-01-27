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
