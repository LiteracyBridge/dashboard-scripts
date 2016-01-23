#!/bin/sh
#CONFIGURATION
# uncomment next line for script debugging
#set -x

if [ -z "$psql" ]; then
  psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
fi
if [ -z "$dbcxn" ]; then
  dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi
if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi

if [ -z "$software" ]; then
  if [ -e ~/code/dashboard-core.git/trunk/target/core-with-deps.jar ]; then
    software=~/code/dashboard-core.git/trunk/target/
  else
    software=$dropbox/LB-software/ACM-install/ACM/software/
  fi
fi
if [ -z "$java" ]; then
  java="java"
fi

importdir=$dropbox/stats/
exportdir=$dropbox/collected-data-processed/

# trim off any trailing slash
importdir=${importdir%/}
exportdir=${exportdir%/}

# Move into Java directory with lib & resources subdirectory
pushd $dropbox/LB-software/ACM-install/ACM/software

echo "Zipping stats and then clearing $importdir."
newStatsDir=$(java -cp acm.jar:lib/* org.literacybridge.acm.utils.MoveStats $importdir $exportdir) 
echo "Zip files are now in $newStatsDir"
popd

if [ -d "$newStatsDir" ]; then
  $java -jar $software/core-with-deps.jar -f -z $newStatsDir
fi
