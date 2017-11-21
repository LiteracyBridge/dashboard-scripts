#!/usr/bin/env bash
traditionalIFS="$IFS"
#IFS="`printf '\n\t'`"

# Script to create and initially populate the recipients table.
# Extracts from UNICEF-2-recipients.csv (from UNICEF2 project spec), and scans ACM directories for "communities"
# Creates and populates recipients.

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
    fromspecification="UNICEF-2-recipients.csv"
    application="createrecipients.py"
    recipientsfile="recipients.csv"
    recipmapfile="recipients_map.csv"
    communitiesfile="communities.csv"

    echo $(date)>log.txt
    verbose=true
    execute=true
}

function main() {
    configure

    set -x
    
    # Extract the recipients from directory names, and filter from UNICEF-2 specification.
    extractRecipients

    createTable
    importTable
}

function createTable() {


    # Create the recipients table if needed
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO all
    DROP TABLE IF EXISTS public.recipients CASCADE;
    
    DROP TABLE IF EXISTS public.recipients_map;

    CREATE TABLE IF NOT EXISTS public.recipients
    (
      recipientid CHARACTER VARYING PRIMARY KEY,
      project CHARACTER VARYING NOT NULL,
      partner CHARACTER VARYING NOT NULL,
      communityname CHARACTER VARYING NOT NULL,
      groupname CHARACTER VARYING NOT NULL,
      affiliate CHARACTER VARYING NOT NULL,
      component CHARACTER VARYING NOT NULL,
      country CHARACTER VARYING NOT NULL,
      region CHARACTER VARYING NOT NULL,
      district CHARACTER VARYING NOT NULL,
      numhouseholds INTEGER NOT NULL,
      numtbs INTEGER  NOT NULL,
      supportentity CHARACTER VARYING NOT NULL,
      model CHARACTER VARYING NOT NULL,
      language CHARACTER VARYING NOT NULL,
      coordinates POINT,
      UNIQUE (partner, communityname, groupname),
      CONSTRAINT lowercase_recipientid_check CHECK (recipientid = LOWER(recipientid))
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public.recipients
      OWNER TO lb_data_uploader;

    CREATE TABLE IF NOT EXISTS public.recipients_map
    (
      project CHARACTER VARYING NOT NULL,
      directory CHARACTER VARYING NOT NULL,
      recipientid CHARACTER VARYING NOT NULL,
      PRIMARY KEY (project, directory)
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public.recipients_map
      OWNER TO lb_data_uploader;
EndOfQuery
}

function extractRecipients() {
    # Extract data from recipients_map
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO all
    \COPY (SELECT * FROM communities) TO '${communitiesfile}' WITH CSV HEADER;
EndOfQuery
    python ${application} UNICEF-2-recipients.csv --dropbox ${dropbox} --projects @projects.list --communities ${communitiesfile}
}

function importTable() {
    # Import into db, and update recipients
    ${psql} ${dbcxn} -a <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO all
    delete from recipients where true;
    create temporary table temp_recip as select * from recipients where false;
    \copy temp_recip from '${recipientsfile}' with delimiter ',' csv header;
    insert into recipients select * from temp_recip  on conflict do nothing;
EndOfQuery

    # Import into db, and update recipients_map
    ${psql} ${dbcxn}  <<EndOfQuery >>log.txt
    \\timing
    \\set ECHO all
    delete from recipients_map where true;
    create temporary table temp_recip_map as select * from recipients_map where false;
    \copy temp_recip_map from '${recipmapfile}' with delimiter ',' csv header;
    insert into recipients_map select * from temp_recip_map  on conflict do nothing;
EndOfQuery
}


main "$@"

# ta-da
