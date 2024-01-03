Periodic batch jobs.

Every 6 hours at :00, import v1 stats:
	Import stats and process with cronjob.sh, statswrapper.sh

Every 4 hours at :55, import v2 stats:
    Import v2 stats and process with tbv2cron.sh

Reboot daily at 00:45.

Run missing UF report daily at 01:00.

Re-start dropbox after boot.


crontab.txt		pre-defined crontab to run the batch files
cronON.sh		turn cron on
cronOFF.sh		turn cron off
