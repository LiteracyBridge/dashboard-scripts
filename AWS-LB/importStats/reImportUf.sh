#!/usr/bin/env bash
#
# Usage:
#   cd into a 'daily directory', like 2021/06/01
#   run this script 

if [ -z "${dropbox-}" ]; then
    dropbox=~/Dropbox
fi

if [ -z "${ufexporter-}" ]; then
    ufexporter=${dropbox}/AWS-LB/bin/ufUtility/ufUtility.py
fi
s3uf="s3://amplio-uf/collected"


# Import user feedback to ACM-{project}-FB-{update}
function importUserFeedback() {
    local dailyDir=$1&&shift
    local recordingsDir=${dailyDir}/userrecordings

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
}

set -x
importUserFeedback $(pwd)
