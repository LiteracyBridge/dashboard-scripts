#!/bin/bash
#
# Helper for cronjobs. Captures stdout and stderr to log and err files.
# 
# Useful for jobs that need to run sequentially.

crondir=/home/ubuntu/cron
cronlog=$crondir/cron.log
jobtime=$(date +%Y%m%d-%H%M%S)

echo "Running cron job at $(date) in $(pwd)" > $cronlog 
cd /home/ubuntu
source ./.profile
export PATH=/home/ubuntu:/home/ubuntu/bin:$PATH
echo "PATH=${PATH}"

function doTask() {
    # 'name' for convenience
    name=$1 
    # set up log & err files, convenience link to latest log
    logfile=$crondir/${jobtime}-${name}.log
    touch $logfile
    rm $crondir/${name}.log
    ln -s $logfile $crondir/${name}.log

    echo "Running ${name} job, logging to $logfile" >>$cronlog
    export TIME="\t%E elapsed\n\t%U user\n\t%S sys\n\t%Kk total\n\t%Mk resident\n\t%P %%cpu"
    /usr/bin/time -ao ${logfile} logReader --s3 tbcd >${logfile} 2>&1
    ~/acm-stats/AWS-LB/bin/sendses.py --subject 'TBv2 Stats Import' --body ${logfile} 

}

doTask tbv2stats

