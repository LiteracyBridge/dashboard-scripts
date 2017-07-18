Files in this directory.

createReports.sh  The shell script that runs the queries to create the reports. Reads from PostgreSQL
    and writes to Dropbox and to S3.
    PostgreSQL connection set by ${dbcxn}, default " --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com
                                                     --port 5432 --username=lb_data_uploader --dbname=dashboard "
    PostgreSQL utility, psql, set by ${psql}, or discovered on ${path}
    Dropbox directory set by ${dropbox}, default "~/Dropbox"

    Reports are written to ${dropbox}/DashboardReports, then sync'd to s3://dashboard-lb-stats/data


reportHelpers  A directory with helper SQL scripts. These scripts create temporary tables and views,
    and are loaded into a psql session. Other files contain the names of queries to run against these
    temporary tables and views, parameterized with project name.


sql  A directory with individual SQL scripts. These are run, parameterized with project, package, and/or deployment.


globalQueries.list  A list of tables or views to be queried. (The tables and/or views were created by the
    SQL in the reportHelpers directory.) The createReports.sh shell script iterates through the lines in this
    file, each line containing a table or view name, and generates "COPY (SELECT * from ${tableorview})
    TO ${reportname} (FORMAT csv, HEADER true);" statements. The report name is build from the table or view name.


projectQueries.list  A list of tables or views to be queried, like globalQueries.list. The shell script iterates
    over the list of all the projects, and generates "COPY (SELECT * from ${tableorview} where project=${project}) 
    TO ${reportname} (FORMAT csv, HEADER true);" statements. Again, the report name is build from the table or 
    view name, with the project name prepended.


reportsByAll.txt  A list of files containing SQL queries. These queries have no parameters. Each query is run, and
    creates a .csv file named for the query.


reportsByDepl.txt  A list of files containing SQL queries, parameterized by project and deployment. Reports are 
    named for the project, query, and deployment. The per-deployment reports are also concatentated into a whole
    project report.


reportsByPkg.txt  A list of files containing SQL queries, parameterized by project and package. Reports are named
    for the project, query, and package. Per-package reports are concatenated into a whole project report.


reportsByPrj.txt  A list of files containing SQL queries, parameterized by project. Reports are named by project
    and query.


stagingFilter  A file of rsync copy filters, used to exclude some files from deployment to s3.
