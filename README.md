# dashboard-scripts

Version control for the script files that process Talking Book statistics.

Contents:
* **AWS-LB** The code for stats collection and processing. The files are distributed via S3 to the EC2 machine where the statistics run.
* **deploy.sh** A shell script to deploy changes to s3.
* **check_deploy.sh** A shell script to see what would be deployed.
* **test** A directory with files to set up database connection strings, some miscelaneous files. 
* **helpers** A directory with sources to helper utilities.
* **diff.sh** A script to run `bcompare` on this `AWS-LB` directory vs `s3://acm-stats/AWS-LB`.
* **README.md** This file.
