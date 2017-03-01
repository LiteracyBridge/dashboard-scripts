#!/bin/sh
# uncomment next line for script debugging
#set -x

if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi

# Diff the version controlled directory against the deployment directory
opendiff ./AWS-LB ${dropbox}/AWS-LB 

