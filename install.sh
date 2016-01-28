#!/bin/sh
# uncomment next line for script debugging
set -x

if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi

# Make sure we know the machine configuration.
reportsDir=$dropbox/AWS-LB
if [ ! -e $reportsDir/runAll.sh ]; then
  echo "Expected to find $reportsDir/runAll.sh."
  echo "If your Dropbox installation directory is not '~/Dropbox', please set dropbox variable."
  exit 100
fi

# Clean up old installations.
if [ -e $reportsDir/ACM_to_RDS ]; then
  echo "Renaming ACM_to_RDS to updateMetadata"
  mv $reportsDir/ACM_to_RDS $reportsDir/updateMetadata
fi
if [ -e $reportsDir/Initial\ Processing\ SQL ]; then
  echo "Renaming Initial\\ Processing\\ SQL to initialSQL"
  mv $reportsDir/Initial\ Processing\ SQL $reportsDir/initialSQL
  rm $reportsDir/initialSQL/INITIAL_PROCESSING_POST_INSERT_OF_NEW_STATS.*
fi

# Copy new files
cp -r ./AWS-LB/* $reportsDir/

# Warn if global dashboard.properties file.
if [ -e /opt/literacybridge/dashboard.properties ]; then
  echo "*** Note that the database connection for 'importStats' will come from the global file /opt/literacybridge/dashboard.properties."
elif [ ! -e $reportsDir/importStats/dashboard.properties ]; then
  echo "Installing $reportsDir/importstats/dashboard.properties file."
  cp $reportsDir/opt/literacybridge/dashboard.properties $reportsDir/importstats/
else
  echo "Leaving existing $reportsDir/importStats"
fi

