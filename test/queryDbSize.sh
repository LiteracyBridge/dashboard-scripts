#!/bin/bash
set -x
echo "psql:$psql, dbcxn:$dbcxn"
$psql $dbcxn -c "SELECT current_database(), pg_size_pretty( pg_database_size( current_database() ) ) As size, pg_database_size(current_database() ) as raw_size;"
