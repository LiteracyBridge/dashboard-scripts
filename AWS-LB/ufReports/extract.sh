#!/usr/bin/env bash

if [ -z ${dropbox} ]; then
    dropbox=~/Dropbox
fi

listfile=projects.txt

swpath=${dropbox}/LB-software/ACM-install/ACM/software/
acmExporter="java -cp ${swpath}acm.jar:${swpath}lib/* org.literacybridge.acm.tools.CSVDatabaseExporter"
summarizer="$(pwd)/uncategorized.py"

# Main function (called from the end)
function main() {
    for proj in $(cat "${listfile}"); do
        mkdir -p "uf/${proj}"
        (cd "uf/${proj}"; exportProject "${proj}")
    done

    deployUpdated
}

# Export from a single project
function exportProject() {
    local proj="$1"&&shift

    # updated list of categories
    ${acmExporter} ${proj} -f categories.csv

    # current messages, with categories as codes
    ${acmExporter} ${proj} -c messages.csv

    # extract counts in each category
    python ${summarizer} messages.csv --categories categories.csv --summary summary.csv

    # easily identify the data
    echo ${proj}>project.txt
}

# Copy all exports to S3
function deployUpdated() {
    local staging=~/ufreports
    if [ -d ${staging} ]; then
        set -x
        local s3dest="s3://dashboard-lb-stats/uf/data"

        # copy from dropbox those files whose contents have changed (--update -c)
        printf "\n\nReports distributed at $(date)\n\nrsync:\n" >report.txt
        rsync --update -cmvr --delete --delete-excluded  "uf/" "${staging}" >>report.txt

        # then sync changes to s3
        printf "\n\naws s3 sync:\n" >>report.txt
        (cd ${staging}; aws s3 sync . ${s3dest} --delete) >>report.txt

        # clean up the s3 debugging spew, and send email
        cat report.txt | tr '\r' '\n' | sed '/^Completed.*remaining/d'>report.filtered
        # never mind the email... Uncomment to send
        #${dropbox}/AWS-LB/bin/sendses.py --subject 'Usesr Feedback Reports updates to s3' --body report.filtered
    else
        echo "No ${staging}, not copying reports"
    fi
}

main "$@"
