Periodic batch jobs.

Every 5 minutes:
	Sync content from Dropbox to S3 with: DeploymentsToS3Sync/cronjob.py

Every day at 0400 UTC:
	Import stats and process with cronjob.sh, dropboxwrapper.sh, statswrapper.sh


crontab.txt		pre-defined crontab to run the batch files
cronON.sh		turn cron on
cronOFF.sh		turn cron off