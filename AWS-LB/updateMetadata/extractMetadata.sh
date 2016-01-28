#!/bin/bash
#
# This script extracts the metadata from ACMs. It needs to be on a machine with ACM installed, and
# with ACM-MEDA, ACM-CARE, ACM-UWR, etc, available in Dropbox.


#CONFIGURATION
# uncomment next line for script debugging
#set -x

if [ -z "$psql" ]; then
  if [ -e /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
  elif [ -e /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
  elif [ ! -z $(which psql) ]; then
    psql=$(which psql)
  else
    echo "Can't find psql!"
    exit 100
  fi
fi
if [ -z "$dbcxn" ]; then
  dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi
if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi
echo "Processing stats with dropbox:$dropbox, psql:$psql, dbcxn:$dbcxn"

exportdir=$dropbox/AWS-LB/updateMetadata/ACMexports/
exportdir=${exportdir%/}

# Get list of projects (ACM DBs) from database projects table
projects=($($psql $dbcxn -c "SELECT projectcode from projects WHERE id >= 0" -t))

#create a single line from list of projects to pass as parameter to jar
for i in "${projects[@]}"
do
 project_spaced_list=" $project_spaced_list ACM-$i"
done

# This doesn't work on our Linux servers, due to the current Dropbox configuration.
# (The 'processing@literacybridge' user owns ACMs, of which ACM-UWR, -CARE, and -MEDA
# are subdirectories. So, the ACM, on that account, can't do the exports, because
# the data is in the wrong place. 
if [[ $OSTYPE == darwin* ]]; then
    # Move into Java directory with lib & resources subdirectory
    cd $dropbox/LB-software/ACM-install/ACM/software
    echo "Exporting all content metadata and ACMs languages & categories to $exportdir from these ACMs: $project_spaced_list"
    rm $exportdir/*
    mkdir -p $exportdir
    java -cp acm.jar:lib/* org.literacybridge.acm.tools.DBExporter $exportdir $project_spaced_list
else
    echo "*********************************************"
    echo " You've attempted to run extractMetadata, but 
    echo " it can not run on this system."
    echo "*********************************************"
fi

