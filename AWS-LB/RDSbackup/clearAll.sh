#!/usr/bin/env bash

#psql $dbcxn -tc "select tablename from pg_tables where tableowner='lb_data_uploader';"


psql $dbcxn -t <<EndOfData | psql $dbcxn
select 'drop table if exists "' || tablename || '" cascade;' from pg_tables where tableowner='lb_data_uploader';

EndOfData

