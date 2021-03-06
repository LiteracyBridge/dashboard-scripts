Helper files for statistics reports.

These files are injected into a psql script. The NN- prefixes control the ordering of them, in case 
that's useful. (It is not needed as of this writing.)

By the time these scripts have all been included, the psql script should be ready to produce the
reports. These helpers can define views, create temporary tables, whatever they need to do for
the setup.

The contents of the projectQueries.list and globalQueries.list files are assumed to be the 
names of tables or views, one name per line.

The globalQueries.list file is read, and each line is used to create a psql line like
  \COPY (SELECT * from view_name) TO 'REPORTSDIR/view_name.csv' OPTIONS (FORMAT csv, HEADERS true);

Similarly, the projectQueries.list file is read and processed, with the addition of:
  WHERE project='${project}'
to the SELECT statement, and a change to the filename like
  'REPORTSDIR/${project}/${project}_view_name.csv'
The '${project}' variable is iterated over the projects, emitting a line to generate the report
for every project.
