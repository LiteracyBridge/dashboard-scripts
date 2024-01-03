#!/bin/sh
# uncomment next line for script debugging
#set -x
set -u

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

# Copy new files. 
source=./AWS-LB/
target=s3://acm-stats/AWS-LB/
executables=${source}/executables.list
echo "${source} -> ${target}"
if [ "${OSTYPE:0:6}" == "darwin" ] ; then
    # On MacOS use the permissions (-perm) option, look for 'u+x'
    executable_flags="-perm +100"
else
    # On Linux or Cygwin (or BSD? or ???), use -executable
    executable_flags="-executable"
fi
# Find executable files (not backups), remove any in 'cloudsync' or '__pycache__' directories. Create executables.list
find ${source} -type f ! -iname '*~' ${executable_flags}|grep -v -e cloudsync -e __pycache__>${executables}
dryrun=" "
# uncomment next line to debug
# dryrun=--dryrun
aws s3 sync --delete ${dryrun} ${source} ${target} \
    --exclude '*~' \
    --exclude '.*' --exclude '*/.*' \
    --exclude '*/cloudsync/*' \
    --exclude '*/__pycache__/*'

