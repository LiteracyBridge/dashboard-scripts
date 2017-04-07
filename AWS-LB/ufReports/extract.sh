#!/usr/bin/env bash

if [ -z ${dropbox} ]; then
    dropbox=~/Dropbox
fi

acm=ACM-UWR-FB-2016-14

swpath=${dropbox}/LB-software/ACM-install/ACM/software/
dbx="java -cp ${swpath}acm.jar:${swpath}lib/* org.literacybridge.acm.tools.CSVDatabaseExporter"

if [ ! -f categories.csv ]; then
    ${dbx} ${acm} -f categories.csv
fi
if [ ! -f messages.csv ]; then
    ${dbx} ${acm} -c messages.csv
fi

python uncategorized.py messages.csv --categories categories.csv --summary summary.csv
