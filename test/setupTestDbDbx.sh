#!/bin/sh

# Source this file to set shell variables for connecting to a local machine test database

if [ -z "$psql" ]; then
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
export dbcxn="--host=localhost --port 5432 --username=lb_data_uploader --dbname=dashboard"
export dropbox=~/tmp
