#!/usr/bin/env bash
set -u

if [ -z ${dropbox-} ]; then
    dropbox=~/Dropbox
fi

if [ -z ${acm-} ]; then
    acm=${dropbox}/LB-software/ACM-install/ACM/software/
fi

report="$(pwd)/report.txt"
projectsList=projects.txt
partitionsList=partitions.txt

acmExporter="java -cp ${acm}acm.jar:${acm}lib/* org.literacybridge.acm.tools.CSVDatabaseExporter"
 acmExtract="java -cp ${acm}acm.jar:${acm}lib/* org.literacybridge.acm.utils.MessageExtractor"
  acmImport="java -cp ${acm}acm.jar:${acm}lib/* org.literacybridge.acm.utils.CmdLineImporter"
  acmCloner="java -cp ${acm}acm.jar:${acm}lib/* org.literacybridge.acm.utils.CloneACM"
 summarizer="$(pwd)/uncategorized.py"

numericre='^[0-9]+$'

# Main function (called from the end)
function main() {
    printf "\n\nUser Feedback reports created at $(date).\n\n">${report}

    cp "${projectsList}" uf/
    for acm in $(cat "${projectsList}"); do
        mkdir -p "uf/${acm}"
        (cd "uf/${acm}"; getUserFeedbackStats "${acm}")
    done

    deployUpdated
}

# Gets the latest DB update 
# Parameters: acm  (full acm name, ACM-FOO)
#             suffix   (suffix to name, like ".p1-4", made from partition)
# echos largest 123 from db123.zip in acm directory.
function getLatest() {
    local acm="$1"&&shift
    local suffix="${1-}"&&shift
    local latest=0
    for dbix in ${dropbox}/${acm}${suffix}/db*.zip; do
        fn=${dbix##*/db}
        fnum=${fn%.zip}
        if [[ ${fnum} =~ ${numericre} ]] ; then
            if [ "${fnum}" -gt "${latest}" ]; then
                latest="${fnum}"
            fi
        fi
    done
    echo ${latest}
}

# Most recent DB update that we've processed
# Parameters: prefix  (prefix to '123.latest' file: empty, or like "p1-4/", made from partition)
function getRecent() {
    local prefix="${1-}"&&shift
    local recent=0
    for curix in $(ls ${prefix}*.recent 2>/dev/null); do
        fn=${curix##*/}
        fnum=${fn%.recent}
        if [ "${fnum}" -gt "${recent}" ]; then
            if [[ ${fnum} =~ ${numericre} ]] ; then
                recent="${fnum}"
            fi
        fi
    done
    echo ${recent}
}

###############################################################################
###############################################################################
#
# To partition user feedback:
#
# Add the base ACM to the list in projects.txt. Create the corresponding
# directory in uf. In the ACM's directory inside uf, create a file named
# partitions.txt, with a series of lines, one for each partition.
#
# Example partition lines:
# p1-4,--max 0.25 --category 9-0
#
# This means "create a partition named 'p1-4'. Extract .25 of the messages
# of category 9-0 (uncategorized user feedback) to that partition.
#
# p-dga,--language dga --category 9-0
#
# This means "create a partition named 'p-dga'. Extract messages in the 
# dga language of category 9-0.
#
# Use any filters that MessageExtractor supports, in the partitions.txt
#
# To partition by 1/3's use lines like:
# p1-3,--max 0.33 --category 9-0
# p2-3,--max 0.5 --category 9-0
# p3-3,--category 9-0
#
# This puts 1/3 in p1-3, half the remaining in p2-3, and the rest in p3-3.
#
# Categories may also be excluded, by prefixing them with '!':
p-cated,--lagnuage dga --category !9-0

# When this script runs, the partition ACMs will be created if they don't 
# already exist. They are given a name like "${acm}.${partition}"
#
###############################################################################
###############################################################################

# Create new, empty partitions, if they don't already exist
# Parameters: acm  (full acm name, ACM-FOO)
function createEmptyPartitions() {
    set acm="$1"&&shift
    # Check for any partitions
    if [ -e ${partitionsList} ]; then
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            mkdir -p ${partition}/msgs
            if [ ! -e "${dropbox}/${acm}.${partition}" ]; then
                echo "Creating new ACM ${acm}.${partition}">>${report}
                ${acmCloner} --from "${acm}" --as "${acm}.${partition}" --verbose>>${report}
            fi
        done
    fi
}

# Check if there has been update activity in a partition
# Parameters: acm  (full acm name, ACM-FOO)
function haveUpdatesOccurred() {
    set acm="$1"&&shift
    # Check for any partitions
    if [ -e ${partitionsList} ]; then
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            local latest=$(getLatest "${acm}" ".${partition}")
            local recent=$(getRecent "${partition}/")
            if [[ "${recent}" -ne "${latest}" ]]; then
                echo "1"
                exit 0
            fi
        done
    fi
    # Check the master
    local latest=$(getLatest "${acm}")
    local recent=$(getRecent)
    if [[ "${recent}" -ne "${latest}" ]]; then
        echo "1"
        exit 0
    fi
    echo "0"
}

# Partition messages per ${partitionsList} file
# Parameters: acm  (full acm name, ACM-FOO)
function partitionMessages() {
    local acm="$1"&&shift
    if [ -e ${partitionsList} ]; then
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            # Clean up from last success; not an error if there are not such files.
            rm -rf "${partition}/msgs/success/*a18" || true
        done
         traditionalIFS=${IFS}
        echo "Partitioning messages for ${acm}">>${report}
        (
        IFS="`printf '\n\t'`"
        for cmd in $(awk -F ',' '{print $2 " --destination " $1 "/msgs"}' ${partitionsList}); do
            IFS=$traditionalIFS
            ${acmExtract} --acm ${acm} ${cmd} --verbose>>${report}
        done
        )
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            ${acmImport} --acm "${acm}.${partition}" "${partition}/msgs">>${report}
            mkdir -p "${partition}/msgs/errors"
            mv "${partition}/msgs/*a18*" "${partition}/msgs/errors/"
            # Clean up the "success" files. Remove all the directories, if they're empty.
            rm "${partition}/msgs/success/*" 2>/dev/null
            rmdir "${partition}/msgs/success" 2>/dev/null
            rmdir "${partition}/msgs/errors" 2>/dev/null
            rmdir "${partition}/msgs" 2>/dev/null
        done
    fi
}

# Gather the categorization statistics back to a central messages.csv file
# Parameters: acm  (full acm name, ACM-FOO)
function gatherCategoryCounts() {
    local acm="$1"&&shift
    # first the master. This will provide the CSV headers
    ${acmExporter} ${acm} -c messages.raw

    if [ -e ${partitionsList} ]; then
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            ${acmExporter} ${acm}.${partition} -c --noheader ${partition}/messages.raw
            cat ${partition}/messages.raw>>messages.raw
        done
    fi

    # current messages, with categories as codes. Some messages have embedded nulls, so
    # remove them for downstream processing. (That's probably corruption from somewhere.)
    tr -d '\000' <messages.raw >messages.csv

}

# Mark the partitions as having been updated
# Parameters: acm  (full acm name, ACM-FOO)
function recordRecentlyProcessed() {
    local acm="$1"&&shift
    if [ -e ${partitionsList} ]; then
        for partition in $(awk -F ',' '{print $1}' ${partitionsList}); do
            (cd ${partition}; rm -f *.recent || true)
            local latest=$(getLatest "${acm}" ".${partition}")
            touch "${partition}/${latest}.recent"
        done
    fi
    # And the master. Should have already been deleted.
    rm -f "*.recent" || true
    local latest=$(getLatest "${acm}")
    touch "${latest}.recent"
}

# Export from a single acm
# Parameters: acm  (full acm name, ACM-FOO)
function getUserFeedbackStats() {
    local acm="$1"&&shift
    printf "ACM ${acm}:\n">>${report}
    mkdir -p logs

    # Creates the ACMs and subdirectories used by the partitions
    createEmptyPartitions ${acm}

    local haveUpdates=$(haveUpdatesOccurred ${acm})

    # any changes?
    if [[ "${haveUpdates}" -eq "0" && -e categories.csv && -e messages.csv && -e summary.csv ]]; then
        # no, done with this acm
        printf "${acm} is up-to-date at version $(getRecent)\n">>${report}
        return
    fi
    echo "Updating ${acm} from $(getRecent) to $(getLatest ${acm})\n">>${report}
    
    # Delete this now, so that if the process dies, we'll run it again next time.
    rm -f *.recent || true

    # updated list of categories
    ${acmExporter} ${acm} -f categories.csv

    # Distribute messages to working ACMs, if the acm is so configured. 
    partitionMessages "${acm}"

    # Get the categorization counts for all of the projects.
    gatherCategoryCounts "${acm}"

    # extract counts in each category. This is what actually drives the dashboard.
    local cmd="python ${summarizer} messages.csv --categories categories.csv --summary summary.csv"
    printf "%s\n\n" "$(${cmd})">>${report}

    # easily identify the data
    echo ${acm}>project.txt

    # remember latest
    recordRecentlyProcessed "${acm}"
}

# Copy all exports to S3
function deployUpdated() {
    local staging=~/ufreports
    if [ -d ${staging} ]; then
        local s3dest="s3://dashboard-lb-stats/uf"

        # copy from dropbox those files whose contents have changed (--update -c)
        printf "\n\nReports distributed at $(date)\n\nrsync:\n" >>${report}
        rsync --update -cmvr --delete --delete-excluded  "uf/" "${staging}" >>${report}

        # then sync changes to s3
        printf "\n\naws s3 sync:\n" >>${report}
        (cd ${staging}; aws s3 sync . ${s3dest} --delete --cache-control "public, max-age=3600") >>${report}

        # clean up the s3 debugging spew, and send email
        cat ${report} | tr '\r' '\n' | sed '/^Completed.*remaining/d'>"${report%%.*}.filtered"
        # never mind the email... Uncomment to send
        #${dropbox}/AWS-LB/bin/sendses.py --subject 'Usesr Feedback Reports updates to s3' --body report.filtered
    else
        echo "No ${staging}, not copying reports"
    fi
}

main "$@"
