#!/usr/bin/env bash

. $HOME/.profile

work=$HOME/work
logdir=${work}/cloudsync
mkdir -p ${logdir}

function synchronize() {
    # for debuggability
    echo "Starting progspec sync in $(pwd) at $(date)"
    export PYTHONPATH=${PYTHONPATH}:${HOME}/Dropbox/AWS-LB/bin
    set -x 
    python3 -m cloudsync -vv progspecs
    set +x
    echo "Finished progspec sync at $(date)" 
}

synchronize >${logdir}/progspecs.log 2>&1
