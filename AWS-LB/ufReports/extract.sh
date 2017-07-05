#!/usr/bin/env bash

if [ -z ${dropbox} ]; then
    dropbox=~/Dropbox
fi

report="$(pwd)/report.txt"
listfile=projects.txt

swpath=${dropbox}/LB-software/ACM-install/ACM/software/
acmExporter="java -cp ${swpath}acm.jar:${swpath}lib/* org.literacybridge.acm.tools.CSVDatabaseExporter"
summarizer="$(pwd)/uncategorized.py"

# Main function (called from the end)
function main() {
    printf "\n\nUser Feedback reports created at $(date).\n\n">${report}

    cp "${listfile}" uf/
    for proj in $(cat "${listfile}"); do
        mkdir -p "uf/${proj}"
        (cd "uf/${proj}"; exportProject "${proj}")
    done

    deployUpdated
}

# Export from a single project
function exportProject() {
    local proj="$1"&&shift
    printf "Project ${proj}:\n">>${report}

    # find latest '123' from db101.zip, db102.zip, db123.zip, in ACM-{project} directory.
    local latest=0
    for dbix in ${dropbox}/${proj}/db*.zip; do
        fn=${dbix##*/db}
        fnum=${fn%.zip}
        if [ "${fnum}" -gt "${latest}" ]; then
            latest="${fnum}"
        fi
    done
    # recent we've processed
    local recent=0
    for curix in $(ls *.recent 2>/dev/null); do
        fnum=${curix%.recent}
        if [ "${fnum}" -gt "${recent}" ]; then
            recent="${fnum}"
        fi
    done
    # any changes?
    if [[ "${recent}" -eq "${latest}" && -e categories.csv && -e messages.csv && -e summary.csv ]]; then
        # no, done with this project
        printf "${proj} is up-to-date at version ${latest}\n">>${report}
        return
    fi
    echo "Updating ${proj} from ${recent} to ${latest}\n">>${report}
    rm -f *.recent

    # updated list of categories
    ${acmExporter} ${proj} -f categories.csv

    # current messages, with categories as codes. Some messages have embedded nulls, so
    # remove them for downstream processing. (That's probably corruption from somewhere.)
    ${acmExporter} ${proj} -c messages.raw
    tr -d '\000' <messages.raw >messages.csv

    # extract counts in each category
    local cmd="python ${summarizer} messages.csv --categories categories.csv --summary summary.csv"
    printf "%s\n\n" "$(${cmd})">>${report}

    # easily identify the data
    echo ${proj}>project.txt

    # remember latest
    touch ${latest}.recent
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
        (cd ${staging}; aws s3 sync . ${s3dest} --delete) >>${report}

        # clean up the s3 debugging spew, and send email
        cat ${report} | tr '\r' '\n' | sed '/^Completed.*remaining/d'>"${report%%.*}.filtered"
        # never mind the email... Uncomment to send
        #${dropbox}/AWS-LB/bin/sendses.py --subject 'Usesr Feedback Reports updates to s3' --body report.filtered
    else
        echo "No ${staging}, not copying reports"
    fi
}

main "$@"
