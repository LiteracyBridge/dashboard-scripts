#!/bin/bash
source setupDbConnection.sh

pushd updateMetadata
./updateMetadata.sh
popd
pushd importStats
./importStats.sh
popd
pushd initialSQL
./initialSQL.sh
popd
pushd reports
./createReports.sh
popd
