#!/bin/sh
LANHOST=s95345.gridserver.com
USER=literacybridge.org
PASSWD=T@lkingMT1
ProjectSubdir=$1
ReportSubdir=$2
SRCDIR=~/Dropbox/AWS-LB/reports/$ProjectSubdir/$ReportSubdir
DESTDIR=/domains/literacybridge.org/html/data
FILENAME=*
ftp -inv ${LANHOST} <<END
user ${USER} ${PASSWD}
cd  ${DESTDIR}
lcd ${SRCDIR}
mkdir $ProjectSubdir
cd $ProjectSubdir
mkdir $ReportSubdir
cd $ReportSubdir
mput ${FILENAME} 
bye
END
