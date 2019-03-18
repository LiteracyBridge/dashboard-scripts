#!/bin/sh
# uncomment next line for script debugging
#set -x

if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi

# Make sure we know the machine configuration.
awsDir=$dropbox/AWS-LB
if [ ! -e ${awsDir}/runAll.sh ]; then
  echo "Expected to find ${awsDir}/runAll.sh."
  echo "If your Dropbox installation directory is not '~/Dropbox', please set dropbox variable."
  exit 100
fi

# Clean up old installations.
if [ -e ${awsDir}/ACM_to_RDS ]; then
  echo "Renaming ACM_to_RDS to updateMetadata"
  mv ${awsDir}/ACM_to_RDS ${awsDir}/updateMetadata
fi
if [ -e ${awsDir}/Initial\ Processing\ SQL ]; then
  echo "Renaming Initial\\ Processing\\ SQL to initialSQL"
  mv ${awsDir}/Initial\ Processing\ SQL ${awsDir}/initialSQL
  rm ${awsDir}/initialSQL/INITIAL_PROCESSING_POST_INSERT_OF_NEW_STATS.*
fi

# Check that the required binary (or other) files exist.
missing=''
# Testing code.
#if [ ! -e ./AWS-LB/bin/foo-bar.jar ]; then
#    missing="${missing} foo-bar.jar"
#fi
if [ ! -e ./AWS-LB/bin/core-with-deps.jar ]; then
    missing="${missing} core-with-deps.jar"
fi
# If any missing, prompt user if they want to continue (valid if they're not updated, as nothing's removed below).
if [ "${missing}" != "" ]; then
    read -r -p "Missing files:${missing}; are you sure (see bin/README.md) [Y/n]?" response
    if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]; then
        : # do nothing
    else
        exit 99 
    fi
fi

# Copy new files. Rsync -va will list only the files copied, and copy only changed files.
echo "./AWS-LB/* -> ${awsDir}/"
rsync -av --exclude utilities/*.csv --exclude utilities/*.xlsx --exclude '.*' --exclude data ./AWS-LB/* ${awsDir}/

