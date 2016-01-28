#!/bin/sh
# uncomment next line for script debugging
#set -x

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

# Warn if no dashboard.properties file.
if [ ! -e /opt/literacybridge/dashboard.properties ] && [ ! -e $reportsDir/importStats/dashboard.properties ]; then
  echo "."
  echo "******* NOTE *******"
  echo "There must be a dashboard.properties in either the /opt/literacybridge directory or in"
  echo "the $reportsDir/importStats directory. This is required by the importStats process."
  echo "."
fi

