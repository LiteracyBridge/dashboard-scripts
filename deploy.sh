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
executablesNew=${executables}.new
echo "deploy s3: ${source} -> ${target}"
if [ "${OSTYPE:0:6}" == "darwin" ] ; then
    # On MacOS use the permissions flag
    find ${source} -type f -perm +100 | grep -v '~' > ${executablesNew}
else
    # On Linux or Cygwin, use -executable
    find ${source} -type f -executable | grep -v '~' > ${executablesNew}
fi
if cmp -s "$executables" "$executablesNew" ; then
    # Same
    rm "${executablesNew}"
else
    # Changed
    if [ "${dryrun-}" == "--dryrun" ] ; then
        diff "${executables}" "${executablesNew}"
    fi
    mv -v "${executablesNew}" "${executables}"
fi

# For a dry run, 
# export dryrun=--dryrun
if [ -z "${dryrun-}" ]; then
    dryrun=" "
fi

aws s3 sync --delete ${dryrun} ${source} ${target} \
    --exclude '*~' \
    --exclude '.*' --exclude '*/.*' \
    --exclude '*/cloudsync/*' \
    --exclude '*/__pycache__/*' \
    --exclude '*/deprecatedQueries/*'

