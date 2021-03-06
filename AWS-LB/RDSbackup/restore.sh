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

# Verify user wants to drop and recreate the database.
echo "This will drop and recreate the database 'dashboard' in the local database,"
echo "and will kill any running instances of pgAdmin3."
read -r -p "Do you want to continue? [y/N] " response
if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "."; # go on
else
    exit 99 
fi

looking=true
while $looking; do
  looking=false
  killall datagrip 2>/dev/null
  if [ $? -eq 0 ]; then echo killed datagrip; looking=true; fi
  killall pgAdmin3 2>/dev/null
  if [ $? -eq 0 ]; then echo killed ppgAdmin3; looking=true; fi 
  killall psql 2>/dev/null
  if [ $? -eq 0 ]; then echo killed psql; looking=true; fi
  if $looking ; then sleep 2; fi
done

dbcxn="--host=localhost --port 5432 --username=lb_data_uploader --dbname=dashboard"

echo "Recreating database..."
time $psql ${dbcxn%--user*} < schema.sql
echo "."
echo "Restoring data. This will take a few minutes..."
time $pgrestore ${dbcxn} -Fc -j 4 database.data
#echo "Rematerializing contentstatistics"
#time $psql ${dbcxn} -c 'REFRESH MATERIALIZED VIEW contentstatistics;'

