#!/bin/sh
#CONFIGURATION
# uncomment next line for script debugging
#set -x

if [ -z "$psql" ]; then
  if [ -e /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
  elif [ -e /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
  elif [ ! -z $(which psql) ]; then
    psql=$(which psql)
  else
    echo "Can't find psql!"
    exit 100
  fi
fi
if [ -z "$dbcxn" ]; then
  dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi
if [ -z "$dropbox" ]; then
  dropbox=~/Dropbox
fi

if [ -z "$core" ]; then
  # This lets us test new versions of core-with-deps.jar more easily.
  core=$dropbox/AWS-LB/bin/core-with-deps.jar
fi
if [ -z "$acm" ]; then
    acm=$dropbox/LB-software/ACM-install/ACM/software
fi

# Depending on our Dropbox account, the incoming stats may be in one of two different locations.
if [ -d $dropbox/outbox/stats ]; then
  # The processing@ account's incoming stats are located here.  
  importdir=$dropbox/outbox/stats/
else
  # Other accounts, here.  
  importdir=$dropbox/stats/
fi
exportdir=$dropbox/collected-data-processed/

# trim off any trailing slash
importdir=${importdir%/}
exportdir=${exportdir%/}

echo "Zipping stats and then clearing $importdir."
# This date format is used all over the LB software; keep it for compatability. 
timestamp=$(date -u +%Yy%mm%dd%Hh%Mm%Ss)
newStatsDir=${exportdir}/${timestamp}
echo "Will move any stats and/or user feedback to ${newStatsDir}"
time java -cp ${acm}/acm.jar:${acm}/lib/* org.literacybridge.acm.utils.MoveStats ${importdir} ${exportdir} ${timestamp}
if [ $? -eq 0 ]; then
    echo "Zip files are now in $newStatsDir"
    # We're in the importStats directory, which contains a file named dashboard.properties that controls
    # the database connection.
    if [ -d "$newStatsDir" ]; then
        # Capture a list of all the files to be imported
        ls -lR ${newStatsDir}>${newStatsDir}/files.log
        if [ -e acm.log ]; then
            # Log file from MoveStats above.
            mv acm.log ${newStatsDir}/movestats.log
        fi
        if [ -d "${newStatsDir}/userrecordings" ]; then
            FB=org.literacybridge.acm.utils.FeedbackImporter
            REC=${newStatsDir}/userrecordings
            REC_PROCESSED=${newStatsDir}/recordingsprocessed
            REC_SKIPPED=${newStatsDir}/recordingsskipped
            RPT=${newStatsDir}/feedbackreport.html
            mkdir ${REC_PROCESSED}
            mkdir ${REC_SKIPPED}
            echo " User feedback from: ${REC}"
            echo "       Processed to: ${REC_PROCESSED}"
            echo "         Skipped to: ${REC_SKIPPED}"
            echo java -cp ${acm}/acm.jar:${acm}/lib/* ${FB} ${REC} --processed ${REC_PROCESSED} --skipped ${REC_SKIPPED} --report ${RPT}
            java -cp ${acm}/acm.jar:${acm}/lib/* ${FB} ${REC} --processed ${REC_PROCESSED} --skipped ${REC_SKIPPED} --report ${RPT}
            ${email} --body ${RPT} --subject "User feedback imported ${newStatsDir##*/}"
            if [ -e acm.log ]; then
                # Log file from FeedbackImporter
                mv acm.log ${newStatsDir}/feedbackimporter.log
            fi
        fi
        time java -jar $core -f -z $newStatsDir
        if [ -e dashboard_core.log ]; then
            mv dashboard_core.log ${newStatsDir}/
        fi
    fi
else
    echo "No files to import"
fi

