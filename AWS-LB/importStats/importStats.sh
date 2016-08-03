#!/bin/sh
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

if [ -z "$core" ]; then
  # This lets us test new versions of core-with-deps.jar more easily.
  core=$dropbox/AWS-LB/bin/core-with-deps.jar
fi
if [ -z "$acm" ]; then
  # This lets us test new versions of ACM more easily 
  acm=acm.jar
fi

# Depending on our Dropbox account, the incoming stats may be in one of two different locations.
if [ -d $dropbox/outbox/stats ]; then
  # The processing@ account's incoming stats are located here.  
  importdir=$dropbox/outbox/stats/
else
  # Other accounts, here.  
  importdir=$dropbox/stats/
fi
exportdir=$dropbox/collected-data-processed/

# trim off any trailing slash
importdir=${importdir%/}
exportdir=${exportdir%/}

# Move into Java directory with lib & resources subdirectory
pushd $dropbox/LB-software/ACM-install/ACM/software

echo "Zipping stats and then clearing $importdir."
newStatsDir=$(java -cp $acm:lib/* org.literacybridge.acm.utils.MoveStats $importdir $exportdir) 
echo "Zip files are now in $newStatsDir"
popd

# We're in the importStats directory, which contains a file named dashboard.properties that controls
# the database connection.
if [ -d "$newStatsDir" ]; then
  time java -jar $core -f -z $newStatsDir
fi

