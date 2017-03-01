# Helpers to copy the stats database to another machine

On the ec2 stats machine, create a directory RDSBackup (the backup runs much
faster from the ec2 machine). Copy these files to that directory, and run
backup.sh.

On the machine where you want to load the database, run pull_backup.sh (note
that you'll need lb-stats-machine.pem, acquired separately). This will
copy the schema and data files created on the ec2 machine.

Next, be sure that postgresql is running. Then run restore.sh; you may need
to quit any applications that are accessing the database. The script will
kill psql, so you don't need to.

