#!/bin/sh
#CONFIGURATION
# uncomment next line for script debugging
#set -x

# Set default values for any settings that aren't externally set.
function setDefaults() {
    if [ -z "${psql}" ]; then
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
    if [ -z "${dbcxn}" ]; then
      dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
    fi
    if [ -z "${dropbox}" ]; then
      dropbox=~/Dropbox
      echo "Dropbox in ${dropbox}"
    fi
    
    if [ -z "${core}" ]; then
      # This lets us test new versions of core-with-deps.jar more easily.
      core=${dropbox}/AWS-LB/bin/core-with-deps.jar
    fi
    if [ -z "${email}" ]; then
        email=${dropbox}/AWS-LB/bin/sendses.py
    fi
}

function configure() {
    # This date format is used all over the LB software; keep it for compatability. 
    timestamp=$(date -u +%Yy%mm%dd%Hh%Mm%Ss)

    curYear=$(date -u +%Y)
    curMonth=$(date -u +%m)
    curDay=$(date -u +%d)

    dailyDir=${dropbox}/collected-data-processed/${curYear}/${curMonth}/${curDay}

}

function main() {
    setDefaults
    readArguments "$@"

    process
}

declare -a remainingArgs=()
function readArguments() {
    local readopt='getopts $opts opt;rc=$?;[ $rc$opt == 0? ]&&exit 1;[ $rc == 0 ]||{ shift $[OPTIND-1];false; }'
    day=""
    limit=99999
    dryrun=false
    month=""
    skip=0
    verbose=false
    year=$(date -u +%Y)
        
    # day:, limit:, month:, no-execute, skip:, verbose, year:
    opts=d:l:m:ns:vy:

    # Enumerating options
    while eval $readopt
    do
        #echo OPT:$opt ${OPTARG+OPTARG:$OPTARG}
        case "${opt}" in
        d) day=${OPTARG};;
        l) limit=${OPTARG};;
        m) month=${OPTARG};;
        n) dryrun=true;;
        s) skip=${OPTARG};;
        v) verbose=true;;
        y) year=${OPTARG};;
        *) printf "OPT:$opt ${OPTARG+OPTARG:$OPTARG}" >&2;;
        esac
   done
       
   # execute is the opposite of dryrun
    execute=true
    $dryrun && execute=false

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
    if [ ! -d $monthDir} ]; then
        echo "${monthDir} does not exist";
        exit 1
    fi

    ${verbose} && echo "Processing ${year}-${month}"

    for day in $(cd ${onthDir}; ls); do
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

    ${verbose} && echo "Processing ${year}-${month}-${day}"

    importStatistics ${dailyDir}
}

# Import statistics to PostgreSQL database.
function importStatistics() {
    local dailyDir=$1&&shift

    # These -D settings are needed to turn down the otherwise overwhelming hibernate logging.
    local quiet="-Dorg.jboss.logging.provider=slf4j -Djava.util.logging.config.file=simplelogger.properties"
    # iterate the timestamp directories.
    for statdir in $(cd ${dailyDir}; ls); do
        # only process if .zip files; user recordings don't have .zip files (fortunately).
        zipcount=$(ls -1 ${dailyDir}/${statdir}/*.zip 2>/dev/null | wc -l)
        if [ ${zipcount} != 0 ]; then
            if [ $skipped -lt $skip ]; then skipped=$[skipped+1]; continue; fi
            
            ${verbose} && echo "Import from ${statdir}." 

            # Make the commands, so that they can be displayed and/or executed
            local import=(time java ${quiet} -jar ${core} -f -z "${dailyDir}/${statdir}" "${dailyDir}/${statdir}")

            ${verbose} && echo "${import[@]}"
            ${execute} && "${import[@]}"
 
            processed=$[processed+1]
            if [ $processed -ge $limit ]; then exit 1; fi
        else
            ${verbose} && echo "No zips in ${statdir}"
        fi
    done
}

main "$@"
