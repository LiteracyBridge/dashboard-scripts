#!/bin/sh
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"
goodIFS="${IFS}"
#CONFIGURATION
# uncomment next line for script debugging
#set -x
set -u

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
      echo "dbcxn is ${dbcxn}"
    fi
    if [ -z "${dropbox-}" ]; then
      dropbox=~/Dropbox
      echo "Dropbox in ${dropbox}"
    fi

    if [ -z "${bin-}" ]; then
        bin="${dropbox}/AWS-LB/bin"
    fi
    if [ -z "${core-}" ]; then
      # This lets us test new versions of core-with-deps.jar more easily.
      core=${dropbox}/AWS-LB/bin/core-with-deps.jar
      echo "core is ${core}"
    fi
    if [ -z "${acm-}" ]; then
        acm=${dropbox}/LB-software/ACM-install/ACM/software
        echo "acm is ${acm}"
    fi
    if [ -z "${email-}" ]; then
        email=${dropbox}/AWS-LB/bin/sendses.py
        echo "email is ${email}"
    fi
   
    report=importStats.html
    rm ${report}
    echo "$(date)">${report}
    needcss=true
}

function main() {
    setDefaults
    readArguments "$@"

    if ! $userfeedback && ! $statistics && ! $installations; then
        echo "No function specified, exiting"
        exit 1
    fi

    process
    sendMail
}

function process() {
    skipped=0 
    processed=0

    if [ "${day}" != "" ]; then
        # With day, must have month.
        if [ "${month}" == "" ]; then
            echo "Specifying day requires month as well."
            exit 1
        fi
        processDay "${year}" "${month}" "${day}"
    elif [ "${month}" != "" ]; then
        processMonth "${year}" "${month}"
    else
        processYear "${year}"
    fi
}

function processYear() {
    local year=$1&&shift
 
    local yearDir="${dropbox}/collected-data-processed/${year}"
    if [ ! -d ${yearDir} ]; then
        echo "${yearDir} does not exist";
        exit 1
    fi

    $verbose && echo "Processing ${year}"

    for month in $(cd ${yearDir}; ls); do
        if [ -d ${yearDir}/${month} ]; then
            processMonth ${year} ${month}
            if [ $processed -ge $limit ]; then exit 1; fi
        fi
    done
}

function processMonth() {
    local year=$1&&shift
    local month=$1&&shift

    local monthDir="${dropbox}/collected-data-processed/${year}/${month}"
    if [ ! -d ${monthDir} ]; then
        echo "${monthDir} does not exist";
        exit 1
    fi

    $verbose && echo "Processing ${year}-${month}"

    for day in $(cd ${monthDir}; ls); do
        if [ -d ${monthDir}/${day} ]; then
            processDay ${year} ${month} ${day}
            if [ $processed -ge $limit ]; then exit 1; fi
        fi
    done
}

function processDay() {
    local year=$1&&shift
    local month=$1&&shift
    local day=$1&&shift

    local dailyDir=${dropbox}/collected-data-processed/${year}/${month}/${day}
    if [ ! -d ${dailyDir} ]; then
        echo "${dailyDir} does not exist";
        exit 1
    fi

    $verbose && echo "Processing ${year}-${month}-${day}"

    getRecipientMap ${dailyDir}
    $userfeedback && importUserFeedback ${dailyDir}
    $statistics && importStatistics ${dailyDir}
    $installations && importDeployments ${dailyDir}
}

function importUserFeedback() {
    local dailyDir=$1&&shift
    echo "Import user feedback to ACM-{project}-FB-{update}."
    local recordingsDir=${dailyDir}/userrecordings
    local processedDir=${dailyDir}/recordingsprocessed
    local skippedDir=${dailyDir}/recordingsskipped

    local cmd
    # Move processed & skipped recordings back to recordingsDir
    for f in $(cd "${processedDir}"; find . -type d); do
        cmd=(mkdir -p "${recordingsDir}/${f}")
        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"
    done

    for f in $(cd "${processedDir}"; find . -type f); do
        cmd=(mv "${processedDir}/${f}" "${recordingsDir}/${f}")
        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"
    done

    for f in $(cd "${skippedDir}"; find . -type d); do
        cmd=(mkdir -p "${recordingsDir}/${f}")
        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"
    done

    for f in $(cd "${skippedDir}"; find . -type f); do
        cmd=(mv "${skippedDir}/${f}" "${recordingsDir}/${f}")
        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"
    done


    if [ -d "${recordingsDir}" ]; then
        # Capture a list of all the files to be imported
        lsfile="files"
        lssuffix=0
        while [ -e "${dailyDir}/${lsfile}.log" ] ; do
            lssuffix=$[lssuffix+1]
            lsfile="files-${lssuffix}"
        done
        cmd=(ls -lR "${recordingsDir}")
        $verbose && echo "${cmd[@]}" \> "${dailyDir}/${lsfile}.log"
        $execute && "${cmd[@]}">"${dailyDir}/${lsfile}.log"
        local importer=org.literacybridge.acm.utils.FeedbackImporter
        mkdir -p ${processedDir}
        mkdir -p ${skippedDir}
        echo " User feedback from: ${recordingsDir}"
        echo "       processed to: ${processedDir}"
        echo "         skipped to: ${skippedDir}"
        cmd=(java -cp ${acm}/acm.jar:${acm}/lib/* ${importer} ${recordingsDir} --processed ${processedDir} --skipped ${skippedDir} --report ${report})

        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"

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

    # These -D settings are needed to turn down the otherwise overwhelming hibernate logging.
    local quiet1=-Dorg.jboss.logging.provider=slf4j
    local quiet2=-Djava.util.logging.config.file=simplelogger.properties
    # iterate the timestamp directories.
    for statdir in $(cd ${dailyDir}; ls); do
        # only process if .zip files; user recordings don't have .zip files (fortunately).
        zipcount=$[$(ls -1 ${dailyDir}/${statdir}/*.zip 2>/dev/null | wc -l)]
        if [ ${zipcount} -ne 0 ]; then
            if [ $skipped -lt $skip ]; then skipped=$[skipped+1]; continue; fi
            
            $verbose && echo "Import from ${statdir}." 

            getCss

            # Make the commands, so that they can be displayed and/or executed
            local import=(time java ${quiet1} ${quiet2} -jar ${core} -f ${sqloption} -z "${dailyDir}/${statdir}" -d "${dailyDir}/${statdir}" -r "${report}")

            $verbose && echo "${import[@]}"
            $execute && "${import[@]}"
 
            processed=$[processed+1]
            if [ $processed -ge $limit ]; then exit 1; fi
        elif [ -d "${statdir}" ]; then
            $verbose && echo "No zips in ${statdir}"
        fi
    done

    importAltStatistics ${dailyDir}
}

function importAltStatistics() {
    local dailyDir=$1&&shift
    local recipientsmapfile="${dailyDir}/recipients_map.csv"

    getCss
    echo "<h2>Re-importing Play Statistics to database.</h2>">>${report}
    rm "${report}.tmp"

    local playstatisticsCsv=${dailyDir}/playstatistics.csv

    # Gather the playstatistics.kvp files from the daily directory
    local playstatisticsFiles=$(find "${dailyDir}" -iname 'playstatistics.kvp')
    #
    local extract=("${bin}/kv2csv.py" --2pass --columns @columns.txt --map ${recipientsmapfile} --output ${playstatisticsCsv} ${playstatisticsFiles})
    ${verbose} && echo "${extract[@]}">>"${report}.tmp"
    ${execute} && "${extract[@]}">>"${report}.tmp"

    if $execute; then
        # Import into db, and update playstatistics
        IFS=${traditionalIFS}
        ${psql} ${dbcxn}  <<EndOfQuery | tee -a "${report}.tmp"
        \\timing
        \\set ECHO all
        create temporary table mstemp as select * from playstatistics where false;
        \copy mstemp from '${playstatisticsCsv}' with delimiter ',' csv header;
        delete from playstatistics d using mstemp t where d.timestamp=t.timestamp and d.tbcdid=t.tbcdid and d.project=t.project and d.deployment=t.deployment and d.talkingbookid=t.talkingbookid and d.contentid=t.contentid;
        insert into playstatistics select * from mstemp on conflict do nothing;
EndOfQuery
        IFS=${goodIFS}

        echo '<div class="reportline">'>>"${report}"
        awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
        echo '</div>'>>"${report}"
    fi

}

function importDeployments() {
    local dailyDir=$1&&shift
    local recipientsfile="${dailyDir}/recipients.csv"
    local recipientsmapfile="${dailyDir}/recipients_map.csv"
    local deploymentsfile="${dailyDir}/tbsdeployed.csv"

    getCss
    echo "<h2>Re-importing Deployment installations to database.</h2>">>${report}
    rm "${report}.tmp"

    # Gather the deploymentsAll.kvp files from the daily directory
    deploymentsLogs=$(find "${dailyDir}" -iname 'deploymentsAll.kvp')
    #
    local extract=(python "${bin}/tbsdeployed.py" --map ${recipientsmapfile}  --output ${deploymentsfile} ${deploymentsLogs})
    ${verbose} && echo "${extract[@]}">>"${report}.tmp"
    ${execute} && "${extract[@]}">>"${report}.tmp"
  
    if $execute; then
        # Import into db, and update tbsdeployed
        IFS=${traditionalIFS}
        ${psql} ${dbcxn}  <<EndOfQuery | tee -a "${report}.tmp"
        \\timing
        \\set ECHO all
        create temporary table tbtemp as select * from tbsdeployed where false;
        \copy tbtemp from '${deploymentsfile}' with delimiter ',' csv header;
        delete from tbsdeployed d using tbtemp t where d.talkingbookid=t.talkingbookid and d.deployedtimestamp=t.deployedtimestamp;
        insert into tbsdeployed select * from tbtemp on conflict do nothing;
EndOfQuery
        IFS=${goodIFS}

        local partition=("${bin}/dailytbs.py" ${deploymentsfile})
        ${verbose} && echo "${partition[@]}">>"${report}.tmp"
        ${execute} && "${partition[@]}">>"${report}.tmp"

        echo '<div class="reportline">'>>"${report}"
        awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
        echo '</div>'>>"${report}"
    fi
}

function getRecipientMap() {
    local dailyDir=$1&&shift
    local recipientsmapfile="${dailyDir}/recipients_map.csv"
    local goodIFS=${IFS}

    if $execute; then
        # Extract data from recipients_map table. Used to associate 'community' directory names to recipientid.
        IFS=${traditionalIFS}
        ${psql} ${dbcxn}  <<EndOfQuery | tee "${report}.tmp"
        \\timing
        \\set ECHO all
        \COPY (SELECT project, directory, recipientid FROM recipients_map) TO '${recipientsmapfile}' WITH CSV HEADER;
EndOfQuery
        IFS=${goodIFS}

        echo '<div class="reportline">'>>"${report}"
        awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
        echo '</div>'>>"${report}"
    fi
}

function sendMail() {
    if  ${sendemail} ; then
        local cmd=(${email} --subject 'Statistics Re-imported' --body ${report})
        if [ ! -z ${report} ]; then
            $verbose && echo "${cmd[@]}"
            $execute && "${cmd[@]}"
        else
            $verbose && "No contents in report, not sending email."
        fi
    fi
}

function usage() {
    echo "Options:"
    echo "    -y yyyy    import Year, default current year"
    echo "    -m mm      import Month, default all months"
    echo "    -d dd      import Day, default all days, requires month"
    echo ""
    echo "    -u         reimport User feedback"
    echo "    -s         reimport Statistics. If both user feedback and statistics, user feedback is first."
    echo "    -z           when importing Statistics, do not perform database writes."
    echo "    -i         reimport Deployment Installations. Runs after UF or Stats."
    echo ""
    echo "    -e         no email"
    echo "    -n         dry run, No import"
    echo "    -v         Verbose output"
    echo "    -l n       Limit to n directories imported"
    echo "    -k m       sKip first m directories"
    echo "                 Note that -l and -k apply to BOTH statistics and user feedback combined."
}

declare -a remainingArgs=()
function readArguments() {
    local readopt='getopts $opts opt;rc=$?;[ $rc$opt == 0? ]&&exit 1;[ $rc == 0 ]||{ shift $[OPTIND-1];false; }'
    day=""
    limit=99999
    dryrun=false
    month=""
    skip=0
    sendemail=true
    statistics=false
    userfeedback=false
    installations=false
    verbose=false
    execute=false
    year=$(date -u +%Y)
    sqloption=''
    if [ $# == 0 ]; then help=true; else help=false; fi

    # Day:, Help, Installations, sKip:, Limit:, Month:, No-execute, Stats, Userfeedback, Verbose, -set X, Year:
    opts=ed:hik:l:m:nsuvxy:z

    # Enumerating options
    foundone=false
    while eval $readopt
    do
        #echo OPT:$opt ${OPTARG+OPTARG:$OPTARG}
        case "${opt}" in
        e) sendemail=false;;
        d) day=${OPTARG};;
        h) help=true;;
        i) installations=true;;
        l) limit=${OPTARG};;
        m) month=${OPTARG};;
        n) dryrun=true;;
        s) statistics=true;;
        u) userfeedback=true;;
        v) verbose=true;;
        x) set -x;;
        y) year=${OPTARG};;
        z) sqloption='-x';;
        *) printf "OPT:$opt ${OPTARG+OPTARG:$OPTARG}" >&2;;
        esac
   done

   if  $help ; then
       usage
       exit 1
   fi

   # execute is the opposite of dryrun
    execute=true
    $dryrun && execute=false
    $dryrun && echo "Dry run -- nothing will be imported."

    # Enumerating arguments, collect into an array accessible outside the function
        remainingArgs=()
    for arg
    do
        remainingArgs+=("$arg")
    done
    # When the function returns, the following sets the unprocessed arguments back into $1 .. $9
    # set -- "${remainingArgs[@]}"

    echo "limit: ${limit}, skip: ${skip}"
}

main "$@"
