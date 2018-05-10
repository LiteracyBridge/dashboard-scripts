#!/bin/sh
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"
goodIFS="$IFS"
set -u
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

}

function fixTable() {
    local table=$1&&shift
    IFS=${traditionalIFS}
    ${psql} ${dbcxn} -c "update ${table} set languagecode='sil' where languagecode='ssl1';"
    IFS=${goodIFS}
}
function fixTables() {
    fixTable communities
    fixTable contentmetadata2
    fixTable languages
    fixTable packagesindeployment
    IFS=${traditionalIFS}
    ${psql} ${dbcxn} -c "update recipients set language='sil' where language='ssl1';"
    #${psql} ${dbcxn} 'alter table recipients rename language to languagecode;'
    IFS=${goodIFS}
    #fixTable recipients
}

function fixCommunities() {
    local acmdir=$1&&shift
    # iterate over TB-Loaders/communities
    local greetings=0
    local groups=0
    for community in $(ls "${acmdir}/TB-Loaders/communities"); do
        local communitydir="${acmdir}/TB-Loaders/communities/${community}"
        # If there is a languages/ssl1 directory, rename it to languages/sil
        if [ -d "${communitydir}/languages/ssl1" ]; then
            mv "${communitydir}/languages/ssl1" "${communitydir}/languages/sil"
            greetings=$[greetings+1]
        fi
        # If there is a system/ssl1.grp file, rename it to system/sil.grp
        if [ -e "${communitydir}/system/ssl1.grp" ]; then
            mv "${communitydir}/system/ssl1.grp" "${communitydir}/system/sil.grp"
            groups=$[groups+1]
        fi
    done
    echo "$greetings greetings and $groups groups in $acmdir"
}

function fixMetadata() {
    local acmdir=$1&&shift
    # iterate over TB-Loaders/published
    for pub in $(ls "${acmdir}/TB-Loaders/published"); do
        local metadir="${acmdir}/TB-Loaders/published/${pub}/metadata"
        if [ -d ${metadir} ]; then
            (   cd ${metadir}
                # in directory metadata
                # Of files containing "ssl1", edit in place, backup with ~, remove backups on success
                files=$(grep -l ssl1 *.csv); 
                if [ $? -eq 0 ]; then 
                    echo "  ${pub}: $(echo ${files}|tr '\n' ' ')"
                    sed -e s/ssl1/sil/g -i~ $files && rm *.csv~;
                fi
            )
        fi
    done
}

function fixACMs() {
    # iterate over ACMs
    for acm in $(cd ${dropbox}; ls -d ACM-*); do
        acmdir="${dropbox}/${acm}"
        if [ -d ${acmdir}/TB-Loaders/communities ]; then
            echo "Fixing communities for ${acm}:"
            fixCommunities ${acmdir}
        fi
        if [ -d ${acmdir}/TB-Loaders/published ]; then
            echo "Fixing deployments for ${acm}:"
            fixMetadata ${acmdir}
        fi
    done
}

function main() {
    #export psql=psql
    #export dbcxn="--host=localhost --port=5432 --username=lb_data_uploader --dbname=dashboard"
    #export dropbox=~/tmp

    setDefaults

    fixTables
    fixACMs
}

main "$@"

