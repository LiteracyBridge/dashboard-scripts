#!/usr/bin/env zsh

function makeReport() {
# 2021-06-01 12:11:38      77553 collected/CARE-ETH-BOYS/1/011b089a-fec8-5c84-aa8e-d6247ebf411a.mp3
# 2021-06-01 12:11:38       1629 collected/CARE-ETH-BOYS/1/011b089a-fec8-5c84-aa8e-d6247ebf411a.properties

echo "Report at $(date '+%Y-%m-%d %H:%M:%S')"

psql=$(which psql)
# dbcxn=(--host=localhost --port=5432 --username=lb_data_uploader --dbname=dashboard)
dbcxn=(--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port=5432 --username=lb_data_uploader --dbname=dashboard)

if [ ! -e collected-uuids.txt ] ; then
    echo collected-uuids.txt does not exist
    # Build a .csv of message_uuid,s3://url of the .mp3 files in amplio-uf/collected/
    aws s3 ls --recursive s3://amplio-uf/collected/ | \
        grep '.mp3' | \
        awk '{print $NF}' | awk -F '.' '{print $1}' | \
        awk -F / '{printf("%s,%s\n", $NF, $0)}' >collected-uuids.txt
else
    echo collected-uuids.txt exists
fi

if [ ! -e published-uuids.txt ] ; then
    echo published-uuids.txt does not exist
    # Build a .csv of message_uuid,s3://url of the .mp3 files published in downloads.amplio.org/${programid}
    # To assign each line to a variable, as an array (in zsh): output=(${(f)"$(mycmd)"})
    projects=(${(f)"$(${psql} ${dbcxn} -c "select projectcode from projects where not projectcode  ilike any (array['unknown', '%test%']);" -t)"})
    rm -f published.txt
    set -x
    for p in ${projects}; do
        p=${p# }
        aws s3 ls --recursive s3://downloads.amplio.org/${p}/ >>published.txt
    done
    cat published.txt | \
        awk '{print $NF}' | awk -F '.' '{print $1}' | \
        awk -F / '{printf("%s,%s\n", $NF, $0)}' >published-uuids.txt
else
    echo published-uuids.txt exists
fi


# Run the queries to see what's missing from where
${psql} ${dbcxn} << 'EOQ'

\echo Create temp table collected_uuids
CREATE TEMP TABLE collected_uuids (uuid text, url text);
ALTER TABLE collected_uuids ADD PRIMARY KEY (uuid);

\copy collected_uuids(uuid, url) from 'collected-uuids.txt' with csv;

\echo Create temp table published_uuids
CREATE TEMP TABLE published_uuids (uuid text, url text);
ALTER TABLE published_uuids ADD PRIMARY KEY (uuid);

\copy published_uuids(uuid, url) from 'published-uuids.txt' with csv;


\echo Published (s3://downloads.amplio.org/${programid}) uuids not in the "uf_messages" table (will not show up in the UF forms)
\echo (0 rows is good)
select url from published_uuids where uuid not in (select message_uuid from uf_messages);


\echo Collected uuids not in the "uf_messages" table (will not show up in the UF forms)
\echo (0 rows is good)
select url from collected_uuids where uuid not in (select message_uuid from uf_messages);


\echo Rows in "uf_messages" with no corresponding published s3 object (will show up but will not play)
\echo (0 rows is good)
\echo Collected into s3://amplio-uf/collected/${programid}/${deploymentnumber}/${message_uuid}.*
\echo Published in s3://downloads.amplio.org/${programid}/deployment-${deploymentnumber}/${language}/${message_uuid}.mp3
select programid, deploymentnumber, language, message_uuid from uf_messages where message_uuid not in (select uuid from collected_uuids);

EOQ

true
}

echo $(pwd)

# From whence we are run
SCRIPT_SOURCE=${0%/*}

rm -f collected-uuids.txt
rm -f published-uuids.txt

makeReport >missing_uf.log 2>&1

${SCRIPT_SOURCE}/sendses.py --subject 'Missing UF report' --to 'ictnotifications@amplio.org' --body missing_uf.log

# Clean up after self
rm -f collected-uuids.txt published-uuids.txt missing_uf.log published.txt

