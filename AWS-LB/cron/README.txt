These are files to run stats processing from cron.

cronjob.sh          The script that is directly run by cron. 
crontab.txt         Configuration file to run cronjob.sh at 0400.
dropboxwrapper.sh   Run by cronjob.sh, this runs DropboxChangeProcessor to move files outbox->inbox.
statswrapper.sh     Run by cronjob.sh, this runs the AWS-LB/.runAll.sh script.

The cron script (cronjob.sh) runs ${foo}wrapper.sh scripts, and captures their outputs in timestamped files.

To add another task, simply create a foowrapper.sh, and add a line to cronjob.sh to invoke it. Use existing
tasks as a guide.

