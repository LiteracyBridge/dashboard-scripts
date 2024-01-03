#!/bin/bash
source setupDbConnection.sh

echo "=========================================================================================="
echo importStats
echo .
pushd importStats
time ./importStats.sh
popd

echo "=========================================================================================="
echo initialSQL
echo .
pushd initialSQL
time ./initialSQL.sh
popd

echo "=========================================================================================="
echo createReports 
echo .
pushd reports
time ./createReports.sh
popd

