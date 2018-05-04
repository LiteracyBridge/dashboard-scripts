#!/bin/sh
#CONFIGURATION
# uncomment next line for script debugging
#set -x

# Set default values for any settings that aren't externally set.
function setDefaults() {
    if [ -z "${psql-}" ]; then
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
    if [ -z "${dbcxn-}" ]; then
      dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
    fi
    if [ -z "${dropbox-}" ]; then
      dropbox=~/Dropbox
    fi

    if [ -z "${bin-}" ]; then
        bin="${dropbox}/AWS-LB/bin"
    fi
    if [ -z "${core-}" ]; then
      # This lets us test new versions of core-with-deps.jar more easily.
      core=${dropbox}/AWS-LB/bin/core-with-deps.jar
    fi
    if [ -z "${acm-}" ]; then
        acm=${dropbox}/LB-software/ACM-install/ACM/software
    fi
    if [ -z "${email-}" ]; then
        email=${dropbox}/AWS-LB/bin/sendses.py
    fi
    if [ -z "${s3bucket}" ]; then
        s3bucket="s3://acm-stats"
    fi
    needcss=true
    verbose=true
    execute=true
}

function configure() {
    # Depending on our Dropbox account, the incoming stats may be in one of two different locations.
    if [ -d ${dropbox}/outbox/stats ]; then
      # The processing@ account's incoming stats are located here.  
      importdir=${dropbox}/outbox/stats
    else
      # Other accounts, here.  
      importdir=${dropbox}/stats
    fi

    echo "Zipping stats and then clearing $importdir."
    # This date format is used all over the LB software; keep it for compatability. 
    timestamp=$(date -u +%Yy%mm%dd%Hh%Mm%Ss)

    curYear=$(date -u +%Y)
    curMonth=$(date -u +%m)
    curDay=$(date -u +%d)

    dailyDir=${dropbox}/collected-data-processed/${curYear}/${curMonth}/${curDay}
    mkdir -p ${dailyDir}

    timestampedDir=${dailyDir}/${timestamp}
    mkdir -p ${timestampedDir}

    s3import="${s3bucket}/collected-data"
    s3archive="${s3bucket}/archived-data/${curYear}/${curMonth}/${curDay}"

    recipientsfile="${dailyDir}/recipients.csv"
    recipientsmapfile="${dailyDir}/recipients_map.csv"

    report=${dailyDir}/importStats.html
    rm ${report}
    #touch ${report}
    gatheredAny=false

    verbose=true
    execute=true
}

function main() {
    setDefaults
    configure

    gatherFiles
    if ${gatheredAny} ; then
        getRecipientMap ${dailyDir}

        importUserFeedback ${dailyDir}
        importStatistics ${dailyDir}
        importDeployments ${dailyDir}
        sendMail
    fi

    # If the timestampedDir is empty, we don't want it. Same for the dailyDir. If can't remove, ignore error.
    rmdir -p ${timestampedDir}
}


# Gathers the new files, from Dropbox and from s3.
function gatherFiles() {
    # gather from dropbox
    echo "Gather from Dropbox"
    time java -cp ${acm}/acm.jar:${acm}/lib/* org.literacybridge.acm.utils.MoveStats ${importdir} ${dailyDir} ${timestamp} --report movedbx.txt
    if [ $? -eq 0 ]; then
        gatheredAny=true
        if [ -s acm.log ]; then
            # Log file from MoveStats above.
            mv acm.log ${dailyDir}/movedbx.log
        fi
        if [ -s movedbx.txt ]; then
            # Log file from MoveStats above.
            mv movedbx.txt ${dailyDir}/movedbx.txt
        fi
    fi

    # gather from s3
    echo "Gather from s3"
    tmpdir=$(mktemp -d)
    echo "temp:${tmpdir}"

    # pull files from s3
    aws s3 sync ${s3import} ${tmpdir}>reports3.raw

    # save a list of the zip file names. They'll be deleted locally, so get the list now. We'll use
    # the list later, to move the files in s3 to an archival location.
    statslist="$(cd ${tmpdir}; findZips)"

    # process into collected-data
    time java -cp ${acm}/acm.jar:${acm}/lib/* org.literacybridge.acm.utils.MoveStats -b blacklist.txt ${tmpdir} ${dailyDir} ${timestamp}
    if [ $? -eq 0 ]; then
        gatheredAny=true
        if [ -s acm.log ]; then
            # Log file from MoveStats above.
            mv acm.log ${dailyDir}/moves3.log
        fi
    fi

    # move s3 files from import to archive "folder".
    for statfile in ${statslist}; do
#        echo ${statfile}
        aws s3 mv ${s3import}/${statfile} ${s3archive}/${statfile}>>reports3.raw
    done

    # clean up the s3 output, and produce a formatted HTML report.
    cat reports3.raw | tr '\r' '\n' | sed '/^Completed.*remaining/d'>reports3.filtered
    if [ -s reports3.filtered ]; then
        cp reports3.filtered ${dailyDir}/s3.log
        printf "<div class='s3import'><h2>S3 Imports</h2>">rpt.html
        (IFS=''; while read -r line || [[ -n "$line" ]]; do
            printf "<p>%s</p>" "$line">>rpt.html
        done < reports3.filtered)
        printf "</div>\n">>rpt.html
        cat rpt.html >>${report}
    fi
    rmdir ${tmpdir}
}

# Finds files named *.zip in the current directory. Returns one file name per line.
function findZips() {
    for f in $(find . -iname '*.zip'); do
        echo ${f#*/}
    done
}


# Import user feedback to ACM-{project}-FB-{update}
function importUserFeedback() {
    local dailyDir=$1&&shift
    echo "Import user feedback to ACM-{project}-FB-{update}."
    local recordingsDir=${dailyDir}/userrecordings
    local processedDir=${dailyDir}/recordingsprocessed
    local skippedDir=${dailyDir}/recordingsskipped
    if [ -d "${recordingsDir}" ]; then
        # Capture a list of all the files to be imported
        ls -lR ${recordingsDir}>${dailyDir}/files.log
        local importer=org.literacybridge.acm.utils.FeedbackImporter
        mkdir -p ${processedDir}
        mkdir -p ${skippedDir}
        echo " User feedback from: ${recordingsDir}"
        echo "       processed to: ${processedDir}"
        echo "         skipped to: ${skippedDir}"
        java -cp ${acm}/acm.jar:${acm}/lib/* ${importer} ${recordingsDir} --processed ${processedDir} --skipped ${skippedDir} --report ${report}
        if [ -s acm.log ]; then
            # Log file from FeedbackImporter
            mv acm.log ${recordingsDir}/feedbackimporter.log
        fi
    else
        echo "No directory ${recordingsDir}"
    fi
}

# injects css, if not already done
function getCss() {
    # If we haven't yet added the .css to the report file, do that now.
    if ${needcss}; then
        $verbose && echo "Adding css to report."
        $execute && cat importStats.css >>"${report}"
        needcss=false
    fi
}

# Import statistics to PostgreSQL database.
function importStatistics() {
    local dailyDir=$1&&shift
    echo "Import user statistics to database."

    cat importStats.css >>"${report}"

    # These -D settings are needed to turn down the otherwise overwhelming hibernate logging.
    local quiet1=-Dorg.jboss.logging.provider=slf4j
    local quiet2=-Djava.util.logging.config.file=simplelogger.properties
    # iterate the timestamp directories.
    for statdir in $(cd ${dailyDir}; ls); do
        if [ -d "${dailyDir}/${statdir}" ]; then
            local import=(time java ${quiet1} ${quiet2} -jar ${core} -f -z "${dailyDir}/${statdir}" -d "${dailyDir}/${statdir}" -r "${report}")

            $verbose && echo "${import[@]}"
            $execute && "${import[@]}"

            if [ -s dashboard_core.log ]; then
                mv dashboard_core.log "${dailyDir}/${statdir}/"
            fi
       fi
    done

    importAltStatistics ${dailyDir}
}

function importAltStatistics() {
    local dailyDir=$1&&shift
    local recipientsmapfile="${dailyDir}/recipients_map.csv"
    local goodIFS=${IFS}
    IFS=${traditionalIFS}

    getCss
    echo "<h2>Importing Play Statistics to database.</h2>">>${report}
    rm "${report}.tmp"

    local playstatisticsCsv=${dailyDir}/playstatistics.csv

    # Gather the playstatistics.kvp files from the daily directory
    local playstatisticsFiles=$(find "${dailyDir}" -iname 'playstatistics.kvp')
    #
    local extract=("${bin}/kv2csv.py" --2pass --columns @columns.txt --map ${recipientsmapfile} --output ${playstatisticsCsv} ${playstatisticsFiles})
    ${verbose} && echo "${extract[@]}">>"${report}.tmp"
    ${execute} && "${extract[@]}">>"${report}.tmp"

    # Import into db, and update playstatistics
    ${psql} ${dbcxn}  <<EndOfQuery >>"${report}.tmp"
    \\timing
    \\set ECHO all
    create temporary table mstemp as select * from playstatistics where false;
    \copy mstemp from '${playstatisticsCsv}' with delimiter ',' csv header;
    delete from playstatistics d using mstemp t where d.timestamp=t.timestamp and d.tbcdid=t.tbcdid and d.project=t.project and d.deployment=t.deployment and d.talkingbookid=t.talkingbookid and d.contentid=t.contentid;
    insert into playstatistics select * from mstemp on conflict do nothing;
EndOfQuery

    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"
    IFS=${goodIFS}

}

function importDeployments() {
    local dailyDir=$1&&shift
    echo "<h2>Importing Deployment installations to database.</h2>">>${report}
    rm "${report}.tmp"

    local deploymentsfile="${dailyDir}/tbsdeployed.csv"
    # Gather the deploymentsAll.log files from the daily directory
    deploymentsLogs=$(find "${dailyDir}" -iname 'deploymentsAll.kvp')
    #
    local extract=(python "${bin}/tbsdeployed.py" --map ${recipientsmapfile}  --output ${deploymentsfile} ${deploymentsLogs})
    ${verbose} && echo "${extract[@]}">>"${report}.tmp"
    ${execute} && "${extract[@]}">>"${report}.tmp"
  
    # Import into db, and update tbsdeployed
    ${psql} ${dbcxn}  <<EndOfQuery >>"${report}.tmp"
    \\timing
    \\set ECHO all
    create temporary table tbtemp as select * from tbsdeployed where false;
    \copy tbtemp from '${deploymentsfile}' with delimiter ',' csv header;
    delete from tbsdeployed d using tbtemp t where d.talkingbookid=t.talkingbookid and d.deployedtimestamp=t.deployedtimestamp;
    insert into tbsdeployed select * from tbtemp on conflict do nothing;
EndOfQuery

    local partition=("${bin}/dailytbs.py" ${deploymentsfile})
    ${verbose} && echo "${partition[@]}">>"${report}.tmp"
    ${execute} && "${partition[@]}">>"${report}.tmp"

    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"
}

function getRecipientMap() {
    local dailyDir=$1&&shift
    local recipientsmapfile="${dailyDir}/recipients_map.csv"
    local goodIFS=${IFS}
    IFS=${traditionalIFS}

    # Extract data from recipients_map table. Used to associate 'community' directory names to recipientid.
    ${psql} ${dbcxn}  <<EndOfQuery >"${report}.tmp"
    \\timing
    \\set ECHO all
    \COPY (SELECT project, directory, recipientid FROM recipients_map) TO '${recipientsmapfile}' WITH CSV HEADER;
EndOfQuery
    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"
    IFS=${goodIFS}
}


function sendMail() {
    ${email} --subject 'Statistics & User Feedback imported' --body ${report}
}


main "$@"
