#!/bin/bash

if [ -z "$psql" ]; then
  if [ -x /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
  elif [ -x /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
    psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
  elif [ -x $(which psql) ]; then
    psql=$(which psql)
  else
    echo "Can't find psql!"
    exit 100
  fi
  export psql=$psql
fi

dbcxn="--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader"

# Assume that pg_dump is next to psql
pgdump="${psql%psql}pg_dump"
if [ ! -x $pgdump ]; then
    echo "Can't find pg_dump; aborting."
    exit 100
fi

echo "pg_dump: $pgdump"

time $pgdump $dbcxn -v --create --clean --schema-only -f schema.sql dashboard
time $pgdump $dbcxn -Fc --data-only -f database.data dashboard
