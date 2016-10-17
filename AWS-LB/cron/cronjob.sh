#!/bin/bash
#
# Helper for cronjobs. Captures stdout and stderr to log and err files.
# 
# Useful for jobs that need to run sequentially.

crondir=/home/ec2-user/cron
cronlog=$crondir/cron.log
jobtime=$(date +%Y%m%d-%H%M%S)

echo "Running cron job at $(date) in $(pwd)" > $cronlog 
cd /home/ec2-user
source ./.bash_profile
export PATH=/home/ec2-user:/home/ec2-user/bin:$PATH

function doTask() {
    # 'name' for convenience
    name=$1 
    # set up log & err files, convenience link to latest log
    logfile=$crondir/${jobtime}-${name}.log
    errfile=$crondir/${jobtime}-${name}.err
    touch $logfile
    rm $crondir/${name}.log
    ln -s $logfile $crondir/${name}.log
    rm $crondir/${name}.err
    ln -s $errfile $crondir/${name}.err

    echo "Running ${name} job, logging to $logfile" >>$cronlog
    $crondir/${name}wrapper.sh >>$logfile 2>$errfile

    # delete size-zero error logs
    if [ ! -s $errfile ]; then
      rm $errfile
      rm $crondir/${name}.err
    fi
}

doTask dropbox

doTask stats

