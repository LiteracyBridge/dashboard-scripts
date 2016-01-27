#!/bin/sh

# Source this file to set shell variables for connecting to a local machine test database

export psql="/Applications/Postgres.app/Contents/Versions/9.5/bin/psql"
export dbcxn="--host=localhost --port 5432 --username=lb_data_uploader --dbname=dashboard"
export dropbox=~/tmp
