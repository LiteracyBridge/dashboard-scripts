#!/usr/bin/env bash
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
    fi
    if [ -z "${stats_root-}" ]; then
      stats_root=~/acm-stats
    fi

    if [ -z "${bin-}" ]; then
        bin="${stats_root}/AWS-LB/bin"
    fi
    if [ -z "${core-}" ]; then
        # This lets us test new versions of core-with-deps.jar more easily.
        core=${bin}/core-with-deps.jar
    fi
    if [ -z "${acm-}" ]; then
        acm=${bin}/acm
    fi
    if [ -z "${email-}" ]; then
        email=${bin}/sendses.py
    fi
    if [ -z "${processed_data-}" ]; then
        processed_data="${stats_root}/processed-data"
    fi
    if [ -z "${s3bucket-}" ]; then
        s3bucket="s3://acm-stats"
    fi
    if [ -z "${ufexporter-}" ]; then
        ufexporter=${bin}/ufUtility/ufUtility.py
    fi

}

function main() {
    setDefaults

    readArguments "$@"

    s3import="${s3bucket}/collected-data"
    s3uf="s3://amplio-uf/collected"

    curHour=$(date -u +%H)
    curMinute=$(date -u +%M)
    curSecond=$(date -u +%S)
  
    echo "dbcxn is ${dbcxn}"
    echo "Stats root is ${stats_root}"
    echo "bin in ${bin}"
    echo "core is ${core}"
    echo "acm is ${acm}"
    echo "email is ${email}"
    echo "processed_data in ${processed_data}"
    echo "s3import in ${s3import}"
    echo "s3uf in ${s3uf}"
    report=importStats.html
    rm ${report}
    echo "$(date)">${report}
    needcss=true


    if ! $userfeedback && ! $statistics && ! $deployments; then
        echo "No function specified, exiting"
        exit 1
    fi

    process
    sendMail

    if [ $userfeedback ]; then
        echo "user feedback is NYI. Use reImportUF.sh"
    fi
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
 
    local yearDir="${processed_data}/${year}"
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

    local monthDir="${processed_data}/${year}/${month}"
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

    local dailyDir=${processed_data}/${year}/${month}/${day}
    if ${fromArchive} ; then
        reGatherFiles ${year} ${month} ${day}
    elif [ ! -d ${dailyDir} ]; then
        echo "${dailyDir} does not exist";
        exit 1
    fi

    $verbose && echo "Processing ${year}-${month}-${day}"

    getRecipientMap ${dailyDir}
    $userfeedback && importUserFeedback ${dailyDir}
    $statistics && importStatistics ${dailyDir}
    $deployments && importDeployments ${dailyDir}

    if [ ${uploadToS3} ]; then
        # Adds and updates files, but won't remove anything.
        s3DailyDir=${s3bucket}/processed-data/${year}/${month}/${day}

        echo "aws s3 sync ${dailyDir} ${s3DailyDir}"
        set -x
        aws s3 sync ${dailyDir} ${s3DailyDir}
        set +x
    fi


}

# Re-creates gathering files from s3, using the archived originals.
function reGatherFiles() {
    local year=$1&&shift
    local month=$1&&shift
    local day=$1&&shift

    local dailyDir=${processed_data}/${year}/${month}/${day}
    local timestamp="${year}y${month}m${day}d${curHour}h${curMinute}m${curSecond}s"

    mkdir -p ${dailyDir}

    set -x
    # gather from s3
    echo "-------- gatherFiles: Re-gathering the collected data from archived-data --------"

    echo "Re-gather from s3"
    tmpdir=$(mktemp -d)
    echo "temp:${tmpdir}"

    s3import=${s3bucket}/archived-data/${year}/${month}/${day}/

    # pull files from s3
    aws s3 sync ${s3import} ${tmpdir}>reports3.raw

    # process into collected-data
    echo "Re-process into collected-data"
    time java -cp ${acm}/acm.jar:${acm}/lib/* org.literacybridge.acm.utils.MoveStats -b blacklist.txt ${tmpdir} ${dailyDir} ${timestamp}
    if [ $? -eq 0 ]; then
        gatheredAny=true
        if [ -s acm.log ]; then
            # Log file from MoveStats above.
            mv acm.log ${dailyDir}/moves3.log
        fi
    fi

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
    set +x
}

function importUserFeedback() {
    echo "importUserFeedback Not Yet Implemented"
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

# Extract the tb-loader artifacts tbsdeployed.csv, tbscollected.csv, and stats_collected.properties
# from the tbcd1235.zip file
function extractTbLoaderArtifacts() {
    local directory=$1&&shift
    local f
    (
        cd ${directory}
        for f in tbsdeployed.csv tbscollected.csv stats_collected.properties; do
            if [ ! -e $f ]; then
                echo no existing $f
                rm -f tmp
                if unzip -p tbcd*.zip "*$f">tmp ; then
                    echo extracted $f from zip
                    mv -v tmp $f
                else
                    echo could not extract $f from zip: $?
                fi
            else
                echo found existing $f
            fi
        done
    )
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
            if [ $processed -ge $limit ]; then break; fi
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

    getCss
    echo "\n\n\n\n*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+"
    echo "*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+"
    echo "*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+"
    echo "\n\n\nRe-importing Deployment deployments to database."
    echo "<h2>Re-importing Deployment deployments to database.</h2>">>${report}
    rm "${report}.tmp"

    echo "get tb-loader artifacts"
    # iterate the timestamp directories and extract TB-Loader artifacts.
    for statdir in $(cd ${dailyDir}; ls); do
        if [ -d "${dailyDir}/${statdir}" ]; then
            # Extract some files from the zipped collected data: tbsdeployed.csv, tbscollected.csv, stats_collected.properties
            ${verbose} && echo "extractTbLoaderArtifacts ${dailyDir}/${statdir}">>"${report}.tmp"
            ${execute} && extractTbLoaderArtifacts "${dailyDir}/${statdir}">>"${report}.tmp"
        fi
    done

    ${verbose} && echo "report: ${report}"

    # Insert from *Z directories into tbsdeployed and tbscollected. Translate coordinates->latitude/longitude (--c2ll). On conflict
    # with primary key (alreay inserted) update non-pkey columns (--upsert).
    ${verbose} && echo "csvInsert.py --table tbscollected --files *Z/tbscollected.csv --verbose --c2ll --upsert"
    ${execute} &&      (csvInsert.py --table tbscollected --files *Z/tbscollected.csv --verbose --c2ll --upsert 2>&1 | tee -a "${report}.tmp")
    ${verbose} && echo "csvInsert.py --table tbsdeployed  --files *Z/tbsdeployed.csv  --verbose --c2ll --upsert"
    ${execute} &&      (csvInsert.py --table tbsdeployed  --files *Z/tbsdeployed.csv  --verbose --c2ll --upsert 2>&1 | tee -a "${report}.tmp")

    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"

    echo "*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+"
    echo "*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+"
    echo "*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+\n\n\n\n"
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
    echo "    -a         re-import from archived-data, not processed-data"
    echo "               OVERWRITES processed-data. Be sure."
    echo "    -c         Do NOT update s3://acm-stats/processed-data/yyyy/mm/dd/..."
    echo ""
    echo "    -u         reimport User feedback"
    echo "    -p pr        when importing UF, limit to project pr."
    echo "    -s         reimport Statistics. If both user feedback and statistics, user feedback is first."
    echo "    -z           when importing Statistics, do not perform database writes."
    echo "    -i         reimport Deployment Deployments. Runs after UF or Stats."
    echo ""
    echo "    -e         no email"
    echo "    -n         dry run, No import"
    echo "    -v         Verbose output"
    echo "    -l n       Limit to n directories imported"
    echo "    -k m       sKip first m directories"
    echo "                 Note that -l and -k apply to BOTH statistics and user feedback combined."
    echo "    -x         setopt -x in script"
}

declare -a remainingArgs=()
function readArguments() {
    local readopt='getopts $opts opt;rc=$?;[ $rc$opt == 0? ]&&exit 1;[ $rc == 0 ]||{ shift $[OPTIND-1];false; }'
    fromArchive=false
    uploadToS3=true
    day=""
    limit=99999
    dryrun=false
    month=""
    skip=0
    sendemail=true
    statistics=false
    userfeedback=false
    deployments=false
    verbose=false
    execute=false
    year=$(date -u +%Y)
    sqloption=''
    ufProject=''
    if [ $# == 0 ]; then help=true; else help=false; fi

    # from Archive, no Cloud, Day:, no Email, Help, (i)Deployments, sKip:, Limit:, Month:, dry ruN, Stats, Userfeedback, Verbose, -set X, Year:, (z) no sql insert
    opts=aced:hik:l:m:np:suvxy:z

    # Enumerating options
    foundone=false
    while eval $readopt
    do
        #echo OPT:$opt ${OPTARG+OPTARG:$OPTARG}
        case "${opt}" in
        a) fromArchive=true;;
        c) uploadToS3=false;;
        e) sendemail=false;;
        d) day=${OPTARG};;
        h) help=true;;
        i) deployments=true;;
        l) limit=${OPTARG};;
        m) month=${OPTARG};;
        n) dryrun=true;;
        p) ufProject=${OPTARG};;
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

   $verbose && echo "Limiting UF ufProject to ${ufProject}"

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
