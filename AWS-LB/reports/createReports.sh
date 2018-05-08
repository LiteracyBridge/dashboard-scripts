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
    dropbox=~/Dropbox
    echo "dropbox in ${dropbox}"
fi

codebasedir="${dropbox}/AWS-LB/reports"
outputdir="${dropbox}/DashboardReports"
sqldir="${codebasedir}/sql"
helpers="${codebasedir}/reportHelpers/*.sql"
projectQueries="${codebasedir}/projectQueries.list"
globalQueries="${codebasedir}/globalQueries.list"

#
# Main function (call is at end of file)
function main() {
#    makeGlobalReports

    printf "project,path\n" > "${outputdir}/project_list.csv"
    projects=($(${psql} ${dbcxn} -c "SELECT projectcode from projects WHERE id >= 0" -t))
    echo WILL NOW ITERATE THROUGH PROJECTS: ${projects[@]}
    for project in "${projects[@]}"; do
        printf "___________________________________\n\nPROJECT:${project}\n"
        printf "%s,%s/\n" ${project} ${project} >>"${outputdir}/project_list.csv"
        projectdir=${outputdir}"/${project}"
        mkdir -p ${projectdir}
        rm ${projectdir}/${project}-*.csv        

 #       makeProjectReports ${project}
 #       makeDeploymentReports ${project}
 #       makePackageReports ${project}

        extractMetadata ${project}
    done

    runBatchedQueries

    # final "report" is current timestamp.
    echo $(date)>${outputdir}/reports_date.txt
    distributeReports
}

# Copy reports to the ACM- directory. This makes them accessible to partners like CBCC.
function distributeReports() {
    echo 'DISTRIBUTING REPORTS'
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

        # copy from dropbox those files whose contents have changed (--update -c)
        printf "\n\nReports distributed at $(date)\n\nrsync:\n" >report.txt
        rsync --update -cmvr --delete --delete-excluded  --filter=". stagingFilter" "${outputdir}/" "${staging}" >>report.txt
        
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
# Extracts metadata from database into project database directory (ACM-XYZ)
function extractMetadata() {
    local project="${1}"&&shift
    local metadatadir="${dropbox}/ACM-${project}/TB-Loaders/metadata"
    if [ -d "${dropbox}/ACM-${project}/TB-Loaders" ]; then
        echo "Extract metadata for ${project} to ${metadatadir}"
        mkdir -p "${metadatadir}"
        # Extract data from recipientstable.
        ${psql} ${dbcxn}  <<EndOfQuery 
        \\timing
        \\set ECHO queries
        \COPY (SELECT * FROM recipients WHERE recipientid IN (SELECT recipientid FROM recipients_map WHERE project='${project}') ) TO '${metadatadir}/recipients.csv' WITH CSV HEADER;
        \COPY (SELECT * FROM deployments WHERE project='${project}') TO '${metadatadir}/deployments.csv' WITH CSV HEADER;
EndOfQuery
    fi
}

#
# This function actually runs psql to process the query. All of the tricky stuff (like
# building the queries and helpers) is done elsewhere.
#
function runBatchedQueries() {
echo 'BATCHED PSQL REPORTS'
$psql $dbcxn <<EndOfQuery >log.txt
\\timing
\\set ECHO queries
$(cat ${helpers})

$(makePerProjectQueries)

$(makeGlobalQueries)

EndOfQuery
}

# Make the report name for query and optionsl project
function rptName() {
    local query=${1}&&shift
    local project=${1}&&shift

    printf "${outputdir}/"
    if [ "${project}" != "" ]; then
        printf "${project}/${project}-"
    fi
    printf "${query}.csv"
}

#
# Make the queries for the per-project reports.
#
# Return the queries as the output, suitable to insert into a psql script.
#
makePerProjectQueries() {
    local queries=($(cat ${projectQueries}))
    for project in "${projects[@]}"; do
        mkdir -p ${outputdir}/${project}
        printf " --  PROJECT: ${project}\n"
        for query in "${queries[@]}"; do
            printf " \\COPY (select * from %s where project='%s')" ${query} ${project}
            printf "  TO '$(rptName ${query} ${project})' (FORMAT csv, HEADER true);\n"
        done
    done
}

#
# Make the queries for the non per-project (ie, global) reports.
#
# Return the queries as the output, suitable to insert into a psql script.
#
function makeGlobalQueries() {
    local queries=($(cat ${globalQueries}))
    printf " --  GLOBAL\n"
    for query in "${queries[@]}"; do
        printf " \\COPY (select * from %s)" ${query}
        printf "  TO '$(rptName ${query})' (FORMAT csv, HEADER true);\n"
    done
}



# Now that everything has been declared, run it.
time main "$@"

