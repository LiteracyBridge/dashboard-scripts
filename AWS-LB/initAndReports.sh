#!/bin/sh
source setupDbConnection.sh

pushd initialSQL
./initialSQL.sh
popd
pushd reports
./createReports.sh
popd
