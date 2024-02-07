#!/usr/bin/env zsh
# uncomment next line for script debugging
#set -x

bucket="s3://"
source="${0:a:h}/"
directory="AWS-LB"

# Diff the version controlled directory against the deployment directory
bcompare ${source}${directory}  ${bucket}${directory}    

