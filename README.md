# dashboard-scripts

Version control for the script files that process Talking Book statistics.

Contents:
* AWS-LB The files that are distributed via Dropbox to the EC2 machine where the statistics run.
* querygen A tool used to rapidly generate SQL queries, used in exploring which reports are useful.
* test Some files to set up for testing.
* install.sh A script to copy files to Dropbox.
* diff.sh A script to run a diff on this directory vs Dropbox. Used to find changes made directly on the server.
