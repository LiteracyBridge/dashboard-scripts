#!/bin/bash
# Look for postgresql utilities.
if [ -z "$psql" ]; then
  if [ -e /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
  elif [ -e /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
  elif [ -z $(which psql) ]; then
    psql=$(which psql)
  else
    echo "Can't find psql!"
    exit 100
  fi
  export psql=$psql
fi
# Verify that we found them.
pgrestore="${psql%psql}pg_restore"
if [ ! -e $psql ]; then
    echo "Can't find 'psql'."
    exit 100
fi
if [ ! -e $pgrestore ]; then
    echo "Can't find 'pg_restore'."
    exit 100
fi

# For local test machine
# dbcxn="--host=localhost --port 5432 --username=lb_data_uploader --dbname=dashboard"

# For production machine
dbcxn="--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard"

echo "Recreating views..."
$psql ${dbcxn} < newviews.sql

