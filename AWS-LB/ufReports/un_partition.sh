#!/usr/bin/env bash
set -u

#
# Script to un-partition user feedback. Will  iterate all the ACM-*FB-*.P* directories, 
# extract the feedback from them, and import it into the ACM-*FB-* ACM.
#
# The .P* ACM directories can then be deleted.
#


if [ -z ${dropbox-} ]; then
    dropbox=~/Dropbox
fi

if [ -z ${acm-} ]; then
    acmsw=${dropbox}/LB-software/ACM-install/ACM/software/
fi

 acmCleaner="java -cp ${acmsw}acm.jar:${acmsw}lib/* org.literacybridge.acm.tools.AcmCleaner"
 acmExtract="java -cp ${acmsw}acm.jar:${acmsw}lib/* org.literacybridge.acm.utils.MessageExtractor"
  acmImport="java -cp ${acmsw}acm.jar:${acmsw}lib/* org.literacybridge.acm.utils.CmdLineImporter"

# acm=ACM-UNICEF-2-FB-2-2017-1
# acm=ACM-UNICEF-2-FB-2-2018-2
# acm=ACM-UNICEF-2-FB-2-2018-3
# acm=ACM-CARE-FB-17-6-UC2-1
# acm=ACM-MEDA-FB-UC2-17-12
# acm=ACM-MEDA-FB-UC2-18-13
acm=ACM-UNICEF-2-FB-2-2018-4

tmpDir=~/work/uf_dir
report=${tmpDir}/report.txt
date>>${report}

read -r -p "Press enter to continue on to 'rm -rf ${tmpDir}/success'. " response

rm -rf ${tmpDir}/success
mkdir -p ${tmpDir}

read -r -p "Press enter to continue. " response

cd ${dropbox}
for acm_p in ${acm}.P*; do
    echo $acm_p
    printf "\n\n${acm_p}\n\n">>${report}
    ${acmExtract} --acm ${acm_p} --keep --dest ${tmpDir} --verbose >>${report} 2>&1
    #${acmCleaner} ${acm_p}
done

#${acmCleaner} ${acm}

#echo "To import the content into the ACM, use:"
#echo ${acmImport} --acm ${acm} ${tmpDir}
${acmImport} --acm ${acm} ${tmpDir} >>${report} 2>&1