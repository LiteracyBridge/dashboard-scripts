#!/bin/sh
# uncomment next line for script debugging
#set -x

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
    if [ -d ~/Dropbox\ \(Literacy\ Bridge\) ]; then
        dropbox=~/Dropbox\ \(Literacy\ Bridge\)
    else
        dropbox=~/Dropbox
    fi
    echo "dropbox in ${dropbox}"
fi

codebasedir="${dropbox}/AWS-LB/reports"
outputdir="${dropbox}/DashboardReports"
sqldir="${codebasedir}/sql"
helpers="${codebasedir}/reportHelpers/*.sql"

PREFIX=''

#
# Main function (call is at end of file)
function main() {
    makeGlobalReports
    makeReportsWithHelpers reportsGlobal ${outputdir}

    local projects=($(${psql} ${dbcxn} -c "SELECT projectcode from projects WHERE id >= 0" -t))
    echo WILL NOW ITERATE THROUGH PROJECTS: ${projects[@]}
    for project in "${projects[@]}"; do
        printf "___________________________________\n\nPROJECT:${project}\n"
        projectdir=${outputdir}"/${project}"
        mkdir -p ${projectdir}

        makeProjectReports ${project}
        makeDeploymentReports ${project}
        makePackageReports ${project}

        (PREFIX="${project}-"; makeReportsWithHelpers reportsByPrj ${projectdir} -v prj=${project})
    done

    # final "report" is current timestamp.
    echo $(date)>${outputdir}/reports_date.txt
    distributeReports
}

# Copy reports to the ACM- directory. This makes them accessible to partners like CBCC.
function distributeReports() {
    local projects="CBCC"

    for proj in ${projects} ; do
        echo "Distributing reports for ${proj}"
        local reportdir=$(getProjectDir ${proj})
        local acmdir=${dropbox}/ACM-${proj}/DashboardReports
        mkdir -p ${acmdir}
        cp -r ${reportdir}/* ${acmdir}
    done

    # If there exists a ~/dashboardreports directory, copy any deltas to it, then 
    # sync newly created reports to s3. Ignore the one directory, and anything hidden.
    local staging=~/dashboardreports
    if [ -d ${staging} ]; then
        local s3dest="s3://dashboard-lb-stats/data"
        local excludes='--exclude="NEW-2016-02-22/***" --exclude=".*" --exclude="*.sh"'

        # copy from dropbox those files whose contents have changed (--update -c)
        printf "\n\nReports distributed at $(date)\n\nrsync:\n" >report.txt
        rsync --update -cmvr ${excludes} "${outputdir}" "${staging}" >>report.txt
        
        # then sync changes to s3
        printf "\n\naws s3 sync:\n" >>report.txt
        (cd ${staging}; aws s3 sync . ${s3dest} --delete) >>report.txt

        # clean up the s3 debugging spew, and send email
        cat report.txt | tr '\r' '\n' | sed '/^Completed.*remaining/d'>report.filtered
        ${dropbox}/AWS-LB/bin/sendses.py --subject 'Reports updates to s3' --body report.filtered
    else
        echo "No ~/dashboardreports, not copying reports"
    fi
}

function getProjectDir() {
    echo "${outputdir}/$1"
}

#
# Global reports, across all projects
function makeGlobalReports() {
    echo "CROSS-PROJECT REPORTS"

    projectdir=$(getProjectDir ALL_PROJECTS)
    mkdir -p ${projectdir}
    for report in $(cat ${codebasedir}/reportsByAll.txt); do 
        echo "  REPORT:${report}"

        exportdir=${projectdir}
        ${psql} ${dbcxn} -A -f $sqldir/${report}.sql > "${exportdir}/${report}.csv" 
    done
}

#
# Reports by project. Queries (xyz.sql) are listed in "reportsByPrj.txt" file.
function makeProjectReports() {
    echo "" 
    echo "  PROJECT REPORTS"
    
    local project=$1
    local projectdir=$(getProjectDir ${project})
    exportdir="${projectdir}/ALL_DEPLOYMENTS"
    mkdir -p ${exportdir}
    
    for report in `cat ${codebasedir}/reportsByPrj.txt`; do 
        echo "    REPORT:${report}"
        ${psql} ${dbcxn} -A -f $sqldir/${report}.sql -v prj=${project} > "${exportdir}/${project}-${report}.csv" 
    done
}

#
# Reports by project, by deployment. Queries are listed in "reportsByDepl.txt" file.
function makeDeploymentReports() {
    echo ""
    echo "  DEPLOYMENT REPORTS"

    local project=$1
    local deployments=($(${psql} ${dbcxn} -c "SELECT deployment from (SELECT distinct deployment, startdate from packagesindeployment WHERE project ='${project}' ORDER BY startdate DESC, deployment) foo" -t))
    
    makeReportsWithItems ${project}  "${codebasedir}/reportsByDepl.txt" depl "${deployments[@]}"
}

#
# Reports by project, by package. Queries are listed in "reportsByPkg.txt" file.
function makePackageReports() {
    echo "  PACKAGE REPORTS"
    
    local project=$1
    local packages=($(${psql} ${dbcxn} -c "SELECT contentpackage from packagesindeployment WHERE project ='${project}' ORDER BY startdate DESC,contentpackage" -t))

    makeReportsWithItems ${project} "${codebasedir}/reportsByPkg.txt" pkg "${packages[@]}"
}
#
# Iterates over report names from a file, then iterates over a list. 
# Calls psql to generate the given report, for each item
#
# param: project name
# param: filename containing list of reports
# param: name of the parameter to be passed to psql ('depl' or 'pkg')
# rest: list of values to be passed to psql, like -v name=value
function makeReportsWithItems() {
    local project="${1}"&& shift
    local reportlistfile="${1}"&&shift
    local itemname="${1}"&& shift
    local items=("${@}")
    local projectdir=$(getProjectDir ${project})

    for report in $(cat ${reportlistfile}); do
        echo "    REPORT:${report}"

        local exportdir=${projectdir}"/${report}"
        mkdir -p ${exportdir}
        if [ -f "${exportdir}/${project}-${report}-all.csv" ]; then
            rm "${exportdir}/${project}-${report}-all.csv"
        fi
        # We want to accumulate the header line, the first line, from the first .csv, so start the first one at line 1 
        firstline="+1"
        for item in "${items[@]}"; do 
            echo "      ${itemname} item:$item"
            ${psql} ${dbcxn} -A -f $sqldir/${report}.sql -v prj=${project} -v ${itemname}=${item} > "${exportdir}/${project}-${report}-$item.csv" 

            # Accumulate the .csv into the 'all' .csv file
            tail -n ${firstline} "${exportdir}/${project}-${report}-${item}.csv" >> "${exportdir}/${project}-${report}-all.csv"
            # We do not want to accumulate the header line for subsequent .csv files, so start at line 2
            firstline="+2"
        done
    done
}

#
# Iterates *.sql in a directory, invokes psql on those files.
# Injects global helpers before the sql
# Writes output to a given directory
#
# param sql directory - queries are here
# param output directory - write output here, named like query.csv
# rest - additional arguments to psql
function makeReportsWithHelpers() {
    local sqldir=$1&&shift
    local csvdir=$1&&shift
    local values="${@}"

    for sql in $(ls ${sqldir}/*.sql); do
        # bash re, drops ".sql" or ".SQL", etc.
        fn=${sql%.*}
        fn=${fn##*/}
        echo "    REPORT:${fn}"
        makeReportWithHelpers ${sql} ${csvdir}/${PREFIX}${fn}.csv ${values}
    done
}

#
# Runs one report, injecting helper sql files
#
# param sql - query to be run
# param csv - csv file to be written with results
# rest - additional args to psql
#
# helpers - global variable pointing to helper sql to be injected. These
# may contain ", sub_query AS (SELECT ...)" entries.
function makeReportWithHelpers() {
    local sql=$1&&shift
    local csv=$1&&shift
    local values="${@}"
    $psql $dbcxn -A ${values}  <<EndOfQuery >${csv}
        COPY (
        WITH dummy_view AS (SELECT 0)
        $(cat ${helpers})
        $(cat ${sql})
        ) TO STDOUT(FORMAT csv, HEADER true);
EndOfQuery
}


# Now that everything has been declared, run it.
time main "$@"

