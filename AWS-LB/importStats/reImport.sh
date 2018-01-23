#!/bin/sh
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"
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
    if [ -z ${dbcxn-} ]; then
      dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
      echo "dbcxn is ${dbcxn}"
    fi
    if [ -z "${dropbox-}" ]; then
      dropbox=~/Dropbox
      echo "Dropbox in ${dropbox}"
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
    bin="${dropbox}/AWS-LB/bin"
    
    report=importStats.html
    rm "${report}"
    needcss=true
}

function main() {
    setDefaults
    readArguments "$@"

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

    for month in $(cd ${yearDir}; ls); do
        processMonth ${year} ${month}
        if [ $processed -ge $limit ]; then exit 1; fi
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
        processDay ${year} ${month} ${day}
        if [ $processed -ge $limit ]; then exit 1; fi
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
        mkdir ${processedDir}
        mkdir ${skippedDir}
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
    local quiet="-Dorg.jboss.logging.provider=slf4j -Djava.util.logging.config.file=simplelogger.properties"
    # iterate the timestamp directories.
    for statdir in $(cd ${dailyDir}; ls); do
        # only process if .zip files; user recordings don't have .zip files (fortunately).
        zipcount=$[$(ls -1 ${dailyDir}/${statdir}/*.zip 2>/dev/null | wc -l)]
        if [ ${zipcount} -ne 0 ]; then
            if [ $skipped -lt $skip ]; then skipped=$[skipped+1]; continue; fi
            
            $verbose && echo "Import from ${statdir}." 

            getCss

            # Make the commands, so that they can be displayed and/or executed
            local import=(time java ${quiet} -jar ${core} -f -z "${dailyDir}/${statdir}" -d "${dailyDir}/${statdir}" -r "${report}")

            $verbose && echo "${import[@]}"
            $execute && "${import[@]}"
 
            processed=$[processed+1]
            if [ $processed -ge $limit ]; then exit 1; fi
        else
            $verbose && echo "No zips in ${statdir}"
        fi
    done
}

function importDeployments() {
set -x
    local dailyDir=$1&&shift
    local recipientsfile="${dailyDir}/recipients.csv"
    local recipientsmapfile="${dailyDir}/recipients_map.csv"
    local deploymentsfile="${dailyDir}/tbsdeployed.csv"
    local goodIFS=${IFS}
    IFS=${traditionalIFS}

    getCss
    echo "<h2>Re-importing Deployment installations to database.</h2>">>${report}

    # Extract data from recipients_map table. Used to associate 'community' directory names to recipientid.
    ${psql} ${dbcxn}  <<EndOfQuery >"${report}.tmp"
    \\timing
    \\set ECHO queries
    \COPY (SELECT project, directory, recipientid FROM recipients_map) TO '${recipientsmapfile}' WITH CSV HEADER;
EndOfQuery

    # Gather the deploymentsAll.log files from the daily directory
    deploymentsLogs=$(find "${dailyDir}" -iname 'deploymentsAll.log')
    #
    local extract=(python "${bin}/tbsdeployed.py" --map ${recipientsmapfile}  --output ${deploymentsfile} ${deploymentsLogs})
    ${verbose} && echo "${extract[@]}">>"${report}.tmp"
    ${execute} && "${extract[@]}">>"${report}.tmp"
   
    # Import into db, and update tbsdeployed
    ${psql} ${dbcxn}  <<EndOfQuery >>"${report}.tmp"
    \\timing
    \\set ECHO queries
    create temporary table tbtemp as select * from tbsdeployed where false;
    \copy tbtemp from '${deploymentsfile}' with delimiter ',' csv header;
    delete from tbsdeployed d using tbtemp t where d.talkingbookid=t.talkingbookid and d.deployedtimestamp=t.deployedtimestamp;
    insert into tbsdeployed select * from tbtemp on conflict do nothing;
EndOfQuery

    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"
    IFS=${goodIFS}
}

function sendMail() {
    local cmd=(${email} --subject 'Statistics Re-imported' --body ${report})
    if [ ! -z ${report} ]; then
        $verbose && echo "${cmd[@]}"
        $execute && "${cmd[@]}"
    else
        $verbose && "No contents in report, not sending email."
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
    echo "    -i         reimport Deployment Installations. Runs after UF or Stats."
    echo ""
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
    statistics=false
    userfeedback=false
    installations=false
    verbose=false
    execute=false
    year=$(date -u +%Y)
    if [ $# == 0 ]; then help=true; else help=false; fi

    # Day:, Help, Installations, sKip:, Limit:, Month:, No-execute, Stats, Userfeedback, Verbose, -set X, Year:
    opts=d:hik:l:m:nsuvy:

    # Enumerating options
    foundone=false
    while eval $readopt
    do
        #echo OPT:$opt ${OPTARG+OPTARG:$OPTARG}
        case "${opt}" in
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
