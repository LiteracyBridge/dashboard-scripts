#!/usr/bin/env bash
set -u
IFS="`printf '\n\t'`"

# This script syncs content updates from dropbox to s3.
#
# It looks in the Dropbox directory for ACM-* project directories, and in those
# directories for a TB-Loaders/published directory. (This will exclude any
# projects that aren't published, like feedback projects.) When such projects
# are found, the current content- and software- .zip files are copied to the
# staging directory, and a .current marker file is created; old .current files
# are also deleted. The staging directory is then sync'd to s3.
#
# Creates a directory structure like this:
# └── projects
#     ├── CARE
#     │   ├── 2016-4-d.current
#     │   ├── content-2016-4-d.zip
#     │   └── software-2016-4-d.zip
#     ├── CBCC
#    . . .
#
# BUGS:
# - the .current file is deleted and re-created, instead of renaming. Thus,
# there can be a window of time in which there are no .current files, or two
# .current files. This is considered to be acceptable, because this will only
# happen as new content is being updated, and anyone who will be using the new
# content *should* know that the content is being updated, and that they
# therefore may need to sync their TB-Loader to get the new content.

# Find dropbox.
if [ -z ${dropbox-} ]; then
    if [ -e ~/Dropbox\ \(Literacy\ Bridge\) ]; then
        dropbox=~/Dropbox\ \(Literacy\ Bridge\)
    elif [ -e ~/Dropbox ]; then
        dropbox=~/Dropbox
    else
        echo "Can't find Dropbox."
        exit 100
    fi
    export dropbox=$dropbox
fi
echo "Dropbox is in $dropbox"

# Checks that there exists a latest ".rev" file.
# Returns 1 on error, 0 on success. (ie, bash)
function checkLatestUpdate() {
    local project=$1
    # Revision file, like ~/Dropbox/ACM-DEMO/TB-Loaders/published/DEMO-2016-1-a.rev
    if [ -z ${2-} ]; then
        # missing *.rev argument
	return 1
    fi
    local rev=$2
    # the .rev file without extension; is also the directory name containing the update files
    latestContentDir=${rev%.rev}
    # the content update name
    update=${latestContentDir##*/}

    #echo "             rev: $rev"
    #echo "latestContentDir: ${latestContentDir}"
    #echo "          update: ${update}"

    if [[ ! -e ${rev} ]]; then
        echo "Missing (or multiple) rev file(s) for project ${project}"
        return 1
    fi
    if [[ ! -d ${latestContentDir} ]]; then
        echo "Missing content directory for project ${project}"
        return 1
    fi
    if [[ ! -e ${latestContentDir}/software-${update}.zip ]]; then
        echo "Missing software .zip file for project ${project}"
        return 1
    fi
    if [[ ! -e ${latestContentDir}/content-${update}.zip ]]; then
        echo "Missing content .zip file for project ${project}"
        return 1
    fi
    return 0
}


if [[ -e report.txt ]]; then rm report.txt; fi

anyupdated=0

# Update the files in the staging directory.
echo "Checking projects for updated content..."
for projdir in $(pushd>/dev/null ${dropbox};ls -d ACM-*); do
    project=${projdir#*-}
    publishedDir=${dropbox}/${projdir}/TB-Loaders/published
    
    s3proj=projects/${project}

    #echo "     project: ${project}"
    #echo "publishedDir: ${publishedDir}"
    #echo "      s3proj: ${s3proj}"


    if [[ -d $publishedDir ]] ; then
        echo $project 
        cur=$(ls ${publishedDir}/*.rev)
        checkLatestUpdate ${project} ${cur}
        if [[ $? -eq 0 ]]; then
            #echo "             rev: $cur"
            #echo "latestContentDir: ${latestContentDir}"
            #echo "          update: ${update}"
		    
            needUpdate=
            # Software different?
            cmp ${latestContentDir}/software-${update}.zip ${s3proj}/software-${update}.zip 1>/dev/null 2>&1
            if [[ $? -ne 0 ]]; then needUpdate=1; fi
            #if [ ${needUpdate} ]; then echo "Need update after compare software"; fi

            # Content different?
            cmp ${latestContentDir}/content-${update}.zip ${s3proj}/content-${update}.zip 1>/dev/null 2>&1
            if [[ $? -ne 0 ]]; then needUpdate=1; fi
            #if [ ${needUpdate} ]; then echo "Need update after compare content"; fi
            
            # .current file missing?
            if [[ ! -e ${s3proj}/${update}.current ]]; then needUpdate=1; fi
            #if [ ${needUpdate} ]; then echo "Need update after compare .current"; fi

            # Something needs updating. Update everything.
            if [[ $needUpdate ]]; then
                echo "Updating ${s3proj}"
                echo "Updating ${s3proj}" >> report.txt
                # Delete and recreate target directory, to clean it.
                if [[ -d ${s3proj} ]]; then rm ${s3proj}/*; fi
		        if [[ ! -d ${s3proj} ]]; then mkdir -p ${s3proj}; fi
                cp ${latestContentDir}/*.zip ${s3proj}
                echo ${update}>${s3proj}/${update}.current
                anyupdated=1
            fi
        fi
    fi
done

if [ $anyupdated -eq 1 ]; then
    # Update the files in S3, in this order:
    # - upload new .zip files
    # - upload new .current files and remove old .current files
    # - remove old .zip files
    echo "s3 sync, please wait..."
    aws s3 sync ./projects s3://acm-content-updates/projects --exclude "*" --include "*.zip" >> report.txt
    aws s3 sync ./projects s3://acm-content-updates/projects --delete --exclude "*" --include "*.current" >> report.txt
    aws s3 sync ./projects s3://acm-content-updates/projects --delete >> report.txt
    echo "...done"
    # Send notification email. Delete the noisy "Completed 1MiB/10MiB..." lines.
    cat report.txt | tr '\r' '\n' | sed '/^Completed.*remaining *$/d'>report.filtered
    ${dropbox}/AWS-LB/bin/sendses.py --subject 'Content updates to s3' --body report.filtered
else
    echo No updates.
    if [[ -e report.txt ]]; then rm report.txt; fi
fi


