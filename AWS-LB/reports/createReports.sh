#!/usr/bin/env bash
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
# outputdir="${dropbox}/DashboardReports"
sqldir="${codebasedir}/sql"
helpers="${codebasedir}/reportHelpers/*.sql"
# projectQueries="${codebasedir}/projectQueries.list"
# globalQueries="${codebasedir}/globalQueries.list"

#
# Main function (call is at end of file)
function main() {

#    printf "project,path\n" > "${outputdir}/project_list.csv"
#     projects=($(${psql} ${dbcxn} -c "SELECT projectcode from projects WHERE id >= 0" -t))
#     echo WILL NOW ITERATE THROUGH PROJECTS: ${projects[@]}
#     for project in "${projects[@]}"; do
#         printf "___________________________________\n\nPROJECT:${project}\n"
# #        printf "%s,%s/\n" ${project} ${project} >>"${outputdir}/project_list.csv"
#         projectdir=${outputdir}"/${project}"
#         mkdir -p ${projectdir}
#         #rm ${projectdir}/${project}-*.csv        
#     done

    runBatchedQueries

}

# function getProjectDir() {
#     echo "${outputdir}/$1"
# }


#
# This function actually runs psql to process the query. All of the tricky stuff (like
# building the queries and helpers) is done elsewhere.
#
function runBatchedQueries() {
echo 'BATCHED PSQL REPORTS'
$psql $dbcxn --pset pager=off<<EndOfQuery >log.txt
\\timing
\\set ECHO all
$(cat ${helpers})

EndOfQuery
# Extracted from above
#
#$(makePerProjectQueries)
#
#$(makeGlobalQueries)
}

# Make the report name for query and optionsl project
# function rptName() {
#     local query=${1}&&shift
#     local project=${1}&&shift
#     local prefix=${1}&&shift
# 
#     printf "${outputdir}/"
#     if [ "${project}" != "" ]; then
#         printf "${project}/"
#     fi
#     if [ "${prefix}" != "" ]; then
#         printf "${prefix}-"
#     fi
#     printf "${query}.csv"
# }
# 
#
# Make the queries for the per-project reports.
#
# Return the queries as the output, suitable to insert into a psql script.
#
# makePerProjectQueries() {
#     local queries=($(cat ${projectQueries}))
#     for project in "${projects[@]}"; do
#         mkdir -p ${outputdir}/${project}
#         printf " --  PROJECT: ${project}\n"
#         for query in "${queries[@]}"; do
#             printf " \\COPY (select * from %s where project='%s')" ${query} ${project}
#             printf "  TO '$(rptName ${query} ${project})' (FORMAT csv, HEADER true);\n"
#         done
#     done
# }
# 
#
# Make the queries for the non per-project (ie, global) reports.
#
# Return the queries as the output, suitable to insert into a psql script.
#
# function makeGlobalQueries() {
#     local queries=($(cat ${globalQueries}))
#     printf " --  GLOBAL\n"
#     for query in "${queries[@]}"; do
#         printf " \\COPY (select * from %s)" ${query}
#         printf "  TO '$(rptName ${query})' (FORMAT csv, HEADER true);\n"
#     done
# }



# Now that everything has been declared, run it.
time main "$@"

