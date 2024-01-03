#!/usr/bin/env bash
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"
goodIFS="$IFS"
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
    needcss=true
    verbose=true
    execute=true
}

function configure() {

    # This date format is used all over the LB software; keep it for compatability. 
    timestamp=$(date -u +%Yy%mm%dd%Hh%Mm%Ss)

    curYear=$(date -u +%Y)
    curMonth=$(date -u +%m)
    curDay=$(date -u +%d)

    s3DailyDir=${s3bucket}/processed-data/${curYear}/${curMonth}/${curDay}

    dailyDir=${processed_data}/${curYear}/${curMonth}/${curDay}
    mkdir -p ${dailyDir}

    timestampedDir=${dailyDir}/${timestamp}
    mkdir -p ${timestampedDir}

    s3import="${s3bucket}/collected-data"
    s3archive="${s3bucket}/archived-data/${curYear}/${curMonth}/${curDay}"
    s3uf="s3://amplio-uf/collected"

    recipientsfile="${dailyDir}/recipients.csv"
    recipientsmapfile="${dailyDir}/recipients_map.csv"

    report=${dailyDir}/importStats.html
    rm ${report}
    #touch ${report}
    gatheredAny=false

    verbose=true
    execute=true

    echo "dbcxn is ${dbcxn}"
    echo "Stats root is ${stats_root}"
    echo "bin in ${bin}"
    echo "core is ${core}"
    echo "acm is ${acm}"
    echo "email is ${email}"
    echo "processed_data in ${processed_data}"
    echo "s3import in ${s3import}"
    echo "s3archive in ${s3archive}"
    echo "s3uf in ${s3uf}"

}

function main() {
    setDefaults
    configure

    gatherFiles
    importUserFeedback ${dailyDir}

    echo "Gathered? ${gatheredAny}"
    if ${gatheredAny} ; then
        getRecipientMap ${dailyDir}

        importStatistics ${dailyDir}
        importDeployments ${dailyDir}
        sendMail
    fi

    # Adds and updates files, but won't remove anything.
    echo "aws s3 sync ${dailyDir} ${s3DailyDir}"
    set -x
    aws s3 sync ${dailyDir} ${s3DailyDir}

    # If the timestampedDir is empty, we don't want it. Same for the dailyDir. If can't remove, ignore error.
    if [ -z ${timestampedDir}/tmp ]; then
        rm ${timestampedDir}/tmp
    fi
    rmdir -vp ${timestampedDir}
}


# Gathers the new files, from s3.
function gatherFiles() {

    set -x
    # gather from s3
    echo "-------- gatherFiles: Gathering the collected data from s3 --------"

    echo "Gather from s3"
    tmpdir=$(mktemp -d)
    echo "temp:${tmpdir}"

    # pull files from s3
    aws s3 sync ${s3import} ${tmpdir}>reports3.raw

    # save a list of the zip file names. They'll be deleted locally, so get the list now. We'll use
    # the list later, to move the files in s3 to an archival location.
    statslist="$(cd ${tmpdir}; findZips)"

    # process into collected-data
    echo "Process into collected-data"
    time java -cp ${acm}/acm.jar:${acm}/lib/* org.literacybridge.acm.utils.MoveStats -b blacklist.txt ${tmpdir} ${dailyDir} ${timestamp}
    if [ $? -eq 0 ]; then
        gatheredAny=true
        if [ -s acm.log ]; then
            # Log file from MoveStats above.
            mv acm.log ${dailyDir}/moves3.log
        fi
    fi

    # move s3 files from import to archive "folder".
    echo "Archive s3 objects"
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
    set +x
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
    local recordingsDir=${dailyDir}/userrecordings

    echo "-------- importUserFeedback: Importing user feedback audio to s3, metadata to database. --------"

    set -x
    echo "Checking for user feedback recordings"
    if [ -d "${recordingsDir}" ]; then
        echo "Export user feedback from ${recordingsDir} and upload to ${s3uf}"
        # using "mktemp -d" only for a unique name.
        tmpname=$(mktemp -d)
        rm -rf ${tmpname}
        tmpdir=~/importUserFeedback${tmpname}
        mkdir -p ${tmpdir}
        echo "uf temp:${tmpdir}"

        python3.8 ${ufexporter} -vv extract_uf ${recordingsDir} --out ${tmpdir}
        aws s3 mv --recursive ${tmpdir} ${s3uf}

        find ${tmpdir}
        rm -rf ${tmpdir}/*
        rmdir -p --ignore-fail-on-non-empty ${tmpdir}
    else
        echo "No directory ${recordingsDir}"
    fi

    set +x
    true
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
        echo "-------- extractTbLoaderArtifacts: in directory ${directory} --------"
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
    echo "Import user statistics to database."

    cat importStats.css >>"${report}"

    echo "-------- importStatistics: Importing 'playstatistics' to database. --------"
    echo "<h2>Importing TB Statistics to database.</h2>">>${report}

    # These -D settings are needed to turn down the otherwise overwhelming hibernate logging.
    local quiet1=-Dorg.jboss.logging.provider=slf4j
    local quiet2=-Djava.util.logging.config.file=simplelogger.properties
    # iterate the timestamp directories.
    for statdir in $(cd ${dailyDir}; ls); do
        if [ -d "${dailyDir}/${statdir}" ]; then
            # -f: force;  -z: process-zips-from-this-directory; -d put-logs-here; -r: append-report-here
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

    getCss
    echo "-------- importAltStatistics: Importing playstatistics to database. --------"
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

}

function importDeployments() {
    local dailyDir=$1&&shift
    echo "-------- importDeployments: Importing Deployment installations to database. --------"
    echo "<h2>Importing Deployment installations to database.</h2>">>${report}
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

    # Import into db, and update tbsdeployed
    # Insert from *Z directories into tbsdeployed and tbscollected. Translate coordinates->latitude/longitude (--c2ll). On conflict
    # with primary key (alreay inserted) do nothing (no --upsert option).
    csvInsert.py --table tbscollected --files *Z/tbscollected.csv --verbose --c2ll 2>&1 | tee -a "${report}.tmp"
    csvInsert.py --table tbsdeployed  --files *Z/tbsdeployed.csv  --verbose --c2ll 2>&1 | tee -a "${report}.tmp"

    echo '<div class="reportline">'>>"${report}"
    awk '{print "<p>"$0"</p>"}' "${report}.tmp" >>"${report}"
    echo '</div>'>>"${report}"
}

function getRecipientMap() {
    local dailyDir=$1&&shift
    local recipientsmapfile="${dailyDir}/recipients_map.csv"

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
}


function sendMail() {
    ${email} --subject 'Statistics & User Feedback imported' --body ${report}
}


main "$@"
