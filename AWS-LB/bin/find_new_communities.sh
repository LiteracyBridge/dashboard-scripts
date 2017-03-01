#!/bin/bash
# e=errexit, abort on first error (except in loops, if test, lists); u=nounset, attempt to use undefined variable fails.
# use: if [ -z ${variable-} ]; then variable_isnt_set; fi 
set -u
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"

# This is a script to find communities that have been added to an ACM- project, but 
# haven't been added to the communities table. Any such communities are added.
# Steps:
# 1 Query the database for known communities, into communities.txt
# 2 Iterate over the projects, and communities in the projects
# 3 If the community from the project isn't in communities.txt, add a line 
#   to a new_communities.csv file
# 4 If a new_communities.csv file has been created, insert it into the communities table

# find psql command
if [ -z ${psql-} ]; then
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
# if dbcxn isn't specified
if [ -z ${dbcxn-} ]; then
    dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi
# if dropbox isn't specified
if [ -z ${dropbox-} ]; then
    dropbox=~/Dropbox
fi

# names of working files
communitiesFile=communities.txt
foundCommunitiesFile=found_communities.csv

# list of projects to process. Several choices to generate the list.
#projects=$'CARE\nCBCC\nMEDA\nTUDRIDEP\nUWR'
#projects=$'CBCC'
# Get list of projects (ACM DBs) from database projects table
cmd="${psql} ${dbcxn} -c 'SELECT projectcode FROM projects WHERE id >= 0' -t"
projects=$(bash -c "$cmd")

# If we ever want a clean-up function, put it here
function finish {
    echo Done
}
trap finish EXIT

# Get list of community names from the communities table in the database 
function queryCommunities() {
    cmd="${psql} ${dbcxn} -c '\\COPY (select communityname from communities) TO ${communitiesFile} WITH CSV FORCE QUOTE communityname'"
    # bash because IFS messes with argument expansion. And we need IFS because the filenames contain
    # spaces. (Sigh...)
    bash -c "$cmd"
}

# If there is a new_communities.csv file, import it into the database.
function importFoundCommunities() {
    if [ ! -e ${foundCommunitiesFile} ]; then echo "No data"; return; fi
    # 
    # NOTE: fields and order. Must write same fields, in same order, to .csv file!
    #
    cmd="${psql} ${dbcxn} -c '\\COPY communities (communityname, year, languagecode, project)	FROM ${foundCommunitiesFile} WITH CSV'"
    # bash because IFS messes with argument expansion. And we need IFS because the filenames contain
    # spaces. (Sigh...)
    bash -c "$cmd"

    # Send notification email
    ${dropbox}/AWS-LB/bin/sendses.py --subject 'Communities added to RDS' --body ${foundCommunitiesFile} 
}

# For a community in a project, determine the language, and generate the .csv line.
function addCommunity() {
    local proj=$1
    local projdir="${dropbox}/ACM-${proj}"
    local commdir="${projdir}/TB-Loaders/communities"
    local community=$2
    local communitydir=${commdir}/${community}
    # ${community^^} would be nice, but requires bash 4; macOS is stuck on 3.2
    community=$(echo ${community} | awk '{print toupper($0)}')
    # The community is new to the DB. Assume it was added this year.
    local year=$(date "+%Y")

    # determine the language, by examining $communitydir
    # Looks like /Users/bill/Dropbox/ACM-CBCC/TB-Loaders/communities/demo-Turkana
    # First look for a 10.a18 file. Ensure there's nothing left over.
    unset lang
    for f in $(find $communitydir -type f -iname 10.a18); do
        # f looks like /Users/bill/Dropbox/ACM-CARE/TB-Loaders/communities/ATAMIDABODI/languages/kus/10.a18
        # Drop trailing /10.a18 and everything up to and including the last /
        lang=${f%/10.a18}
        lang=${lang##*/}
    done
    # If not found, look for 'languages/XYZ'
    if [ -z ${lang-} ]; then
        for f in $(cd ${communitydir}; ls languages); do
            # If more than one, (randomly) take the last.
            lang=$f
        done
    fi
    
    #
    # NOTE: fields and order must match the import, above!
    #
    echo \"${community}\",${year},${lang},${proj}>>${foundCommunitiesFile}
}

# For a given project, iterate over all the communities listed in TB-Loaders/communities. If a
# community isn't in the communities.txt file, create an entry to be added to the database.
function examineOneProject() {
    local proj=$1
    local projdir="${dropbox}/ACM-${proj}"
    local commdir="${projdir}/TB-Loaders/communities"

    echo "Examining project ${proj}"

    for community in $(cd ${commdir}; ls); do
        # If community not found in communities.txt... (ie, if grep doesn't return 0)
        if ! grep -iq "\"${community}\"" ${communitiesFile} ; then
            addCommunity ${proj} ${community}
        fi
     done
}

# Iterate over all projects
function examineAllProjects() {
    for proj in ${projects}; do
        # The database command gives us leading spaces. Trim them. (Sigh...)
        proj=$(echo ${proj} | xargs)
        examineOneProject $proj
    done
}

# main
if [ -e ${foundCommunitiesFile} ]; then rm ${foundCommunitiesFile} ; fi
queryCommunities
examineAllProjects
importFoundCommunities


exit 0

