#!/usr/bin/env bash
traditionalIFS="$IFS"
#IFS="`printf '\n\t'`"
set +x

# Script to create and initially populate the tbsdeployed table.
# Extracts from tbdataoperations, and gathers deploymentsAll.log files from collected-data-processed.
# Creates and populates tbsdeployed.

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

function configure() {
    extractfile="tbdataoperations.csv"
    extractfilter="tbsdeployed.py"
    deploymentsfile="tbsdeployed.csv"
    recipientsmapfile="recipients_map.csv"

    echo $(date)>log.txt
    verbose=true
    execute=true
}

function main() {
    configure

    dropbox=~/Dropbox

    # We need this to map the semi-random 'community name' back to a recipient id.
    extractRecipientsMap

    # Extract the deployments from before there was a deployments.log file...
    extractTbOperations
    # ...and after...
    collectAllDeployments

    set -x
    
    createTable
    importTable
}

function createTable() {
    # Create the tbsdeployed table if needed
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO queries
    DROP TABLE IF EXISTS public.tbsdeployed;
    CREATE TABLE IF NOT EXISTS public.tbsdeployed
    (
      talkingbookid CHARACTER VARYING(255) NOT NULL,
      recipientid CHARACTER VARYING REFERENCES recipients(recipientid),
      deployedtimestamp timestamp NOT NULL,
      project CHARACTER VARYING(255) NOT NULL,
      deployment CHARACTER VARYING(255) NOT NULL,
      contentpackage CHARACTER VARYING(255) NOT NULL,
      firmware CHARACTER VARYING(255) NOT NULL,
      location CHARACTER VARYING(255) NOT NULL,
      coordinates POINT,
      username CHARACTER VARYING(255) NOT NULL,
      tbcdid CHARACTER VARYING(255) NOT NULL,
      action CHARACTER VARYING(255),
      newsn BOOLEAN NOT NULL,
      testing BOOLEAN NOT NULL,
      CONSTRAINT tbdeployments_pkey PRIMARY KEY (talkingbookid, deployedtimestamp)
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public.tbsdeployed
      OWNER TO lb_data_uploader;
EndOfQuery
}

function collectAllDeployments() {
    # Gather the deploymentsAll.log files from 2017-October
    deploymentsLogs=$(find ${dropbox}/collected-data-processed/2017 -iname 'deploymentsAll.log')
    #
    local extract=(python ${extractfilter} --tbdata ${extractfile} --map ${recipientsmapfile}  --output ${deploymentsfile} ${deploymentsLogs})
    ${verbose} && echo "${extract[@]}"
    ${execute} && "${extract[@]}"
}

function extractTbOperations() {
    # Extract data from tbdataoperations
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO queries
    \COPY (SELECT outsn, updatedatetime, project, outdeployment, outimage, outcommunity, outfwrev, outsyncdir, location, action FROM tbdataoperations WHERE updatedatetime < '2017-10-04' AND action ilike 'update%') TO '${extractfile}' WITH CSV HEADER;
EndOfQuery
}

function extractRecipientsMap() {
    # Extract data from recipients_map
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO queries
    \COPY (SELECT project, directory, recipientid FROM recipients_map) TO '${recipientsmapfile}' WITH CSV HEADER;
EndOfQuery
}

function importTable() {
    # Import into db, and update tbsdeployed
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO queries
    delete from tbsdeployed where true;

    create temporary table tbtemp as select * from tbsdeployed where false;

    \copy tbtemp from '${deploymentsfile}' with delimiter ',' csv header;

    delete from tbsdeployed d using tbtemp t where d.talkingbookid=t.talkingbookid and d.deployedtimestamp=t.deployedtimestamp;

    insert into tbsdeployed select * from tbtemp on conflict do nothing;

EndOfQuery
}


main "$@"

# ta-da