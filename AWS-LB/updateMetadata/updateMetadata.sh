#!/bin/sh

# This script extracts metadata from project specific ACM directories and updates RDS.
# From ACM, it updates contentmetadata2, categories, and languages.
# From TB-Loaders, it updates packagesindeployment, categoriesinpackages, and contentsinpackages.

#CONFIGURATION
# uncomment next line for script debugging
#set -x

if [ -z "$psql" ]; then
    if [ -e /Applications/Postgres.app/Contents/Versions/9.5/bin/psql ]; then
        psql=/Applications/Postgres.app/Contents/Versions/9.5/bin/psql
    elif [ -e /Applications/Postgres.app/Contents/Versions/9.4/bin/psql ]; then
        psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
    elif [ ! -z $(which psql) ]; then
        psql=$(which psql)
    else
        echo "Can't find psql!"
        exit 100
    fi
fi
if [ -z "$dbcxn" ]; then
    dbcxn=" --host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com --port 5432 --username=lb_data_uploader --dbname=dashboard "
fi
if [ -z "$dropbox" ]; then
    dropbox=~/Dropbox
fi
if [ -z "$acm" ]; then
    acm=acm.jar
fi
echo "Processing stats with dropbox:$dropbox, psql:$psql, dbcxn:$dbcxn"
echo "acm: $acm"

exportdir=$dropbox/AWS-LB/updateMetadata/ACMexports/
exportdir=${exportdir%/}

# Get list of projects (ACM DBs) from database projects table
projects=($($psql $dbcxn -c "SELECT projectcode from projects WHERE id >= 0" -t))

#create a single line from list of projects to pass as parameter to jar
for i in "${projects[@]}";  do
    project_spaced_list=" $project_spaced_list ACM-$i"
done

# Move into Java directory with lib & resources subdirectory
cd $dropbox/LB-software/ACM-install/ACM/software
echo "Exporting all content metadata and ACMs languages & categories to $exportdir from these ACMs: $project_spaced_list"
rm $exportdir/*
mkdir -p $exportdir
java -Djava.awt.headless=true -cp $acm:lib/* org.literacybridge.acm.tools.DBExporter $exportdir $project_spaced_list

# For each project, import the following data (just exported above):
#     content metadata
#     list of languages
#     list of categories
# And for each of the most recently published deployments in each project, 
# import the following data (exported during the latest TB-Builder PUBLISH):
#     packages in deployment
#     categories in each package
#     content in each package
# First delete the same data from the database to avoid duplicate or primary key conflict.
for i in "${projects[@]}"; do
    # Only delete data if import file exists.
    if [ -f $exportdir/$i-metadata.csv ]; then
        echo DELETE FROM contentmetadata2 WHERE project =$i
        $psql $dbcxn -c "DELETE FROM contentmetadata2 WHERE project ='$i'"
        echo importing metadata for $i into AWS
        $psql $dbcxn -c "COPY contentmetadata2 FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < $exportdir/$i-metadata.csv
    fi

    # Only delete data if import file exists.
    if [ -f $exportdir/$i-categories.csv ]; then
        echo DELETE FROM categories WHERE projectcode =$i
        $psql $dbcxn -c "DELETE FROM categories WHERE projectcode ='$i'"
        echo importing categories for $i into AWS
        $psql $dbcxn -c "COPY categories FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < $exportdir/$i-categories.csv
    fi

    # Only delete data if import file exists.
    if [ -f $exportdir/$i-languages.csv ]; then
        echo DELETE FROM languages WHERE projectcode =$i
        $psql $dbcxn -c "DELETE FROM languages WHERE projectcode ='$i'"
        echo importing languages for $i into AWS
        $psql $dbcxn -c "COPY languages FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < $exportdir/$i-languages.csv
    fi

    # get latest distribution for each project and then CSV files
    f=$(ls $dropbox/ACM-$i/TB-Loaders/published/*.rev)
    distribution_w_version=$(echo $f | awk -F"/" '{print $NF}' | sed '/\.rev/s/\.rev//g')
    #distribution_wo_version=$(echo $distribution_w_version | sed 's/-[a-z]//g')
    csvDir=$dropbox/ACM-$i/TB-Loaders/published/$distribution_w_version/metadata
    echo metadatadir:$csvDir
    categoriesInPackagesFile=$(echo $csvDir/categoriesinpackages.csv)
    contentInPackagesFile=$(echo $csvDir/contentinpackages.csv)

    # packagesindeployment.csv is "project","deployment","contentpackage","packagename","startdate","enddate","languagecode","groups","distribution"
    packagesInDeploymentFile=$(echo $csvDir/packagesindeployment.csv)
    # skip header, split on comma, take second field (deployment), sort and eliminate duplicates
    # same as: deployments=$(cat $packagesInDeploymentFile | awk -F , 'NR>1{print $2}' | sort -u)
    deployments=$(cat $packagesInDeploymentFile | sed -n '1!p' | cut -d ',' -f 2 | sort -u)
    # third field (contentpackage)
    packages=$(cat $packagesInDeploymentFile | sed -n '1!p' | cut -d ',' -f 3 | sort -u)
    
    # turn "2015-3" "2015-4" -> '2015-3','2015-4'
    deployments=$(echo $deployments | awk '{print toupper($0)}' | sed "s/\"/'/g" | sed "s/' '/','/g")
    # turn "2015-3-bim" "2015-3-kus" -> '2015-3-BIM','2015-3-KUS'
    packages=$(echo $packages | awk '{print toupper($0)}' | sed 's/\" \"/\",\"/g' | sed "s/\"/'/g")
    echo deployments:$deployments
    echo packages:$packages

    # get rid of empty quotes ("")
    cat "$packagesInDeploymentFile" | sed s'/\"\"//'g > "$csvDir/packagesindeployment-nonullquote.csv" 
    mv "$csvDir/packagesindeployment-nonullquote.csv" "$packagesInDeploymentFile"

    if [ -f $packagesInDeploymentFile ]; then
        echo "DELETE FROM packagesindeployment WHERE project='$i' AND UPPER(deployment) IN ($deployments)"
        $psql $dbcxn -c "DELETE FROM packagesindeployment WHERE project='$i' AND UPPER(deployment) IN ($deployments)"
        echo importing packagesindeployment for $i into AWS
        $psql $dbcxn -c "COPY packagesindeployment FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < $packagesInDeploymentFile
        echo updating packagesindeployment with distribution label $distribution_w_version
        $psql $dbcxn -c "UPDATE packagesindeployment SET distribution='$distribution_w_version' WHERE project='$i' AND UPPER(deployment) IN ($deployments)"
    fi

    if [ -f $categoriesInPackagesFile ]; then
        echo "DELETE FROM categoriesinpackage WHERE project ='$i' AND UPPER(contentpackage) IN ($packages)"
        $psql $dbcxn -c "DELETE FROM categoriesinpackage WHERE project ='$i' AND UPPER(contentpackage) IN ($packages)"
        echo importing categoriesinpackage for $i into AWS
        $psql $dbcxn -c "COPY categoriesinpackage FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < "$categoriesInPackagesFile" 
    fi

    if [ -f $contentInPackagesFile ]; then
        echo "DELETE FROM contentinpackage WHERE project ='$i' AND UPPER(contentpackage) IN ($packages)"
        $psql $dbcxn -c "DELETE FROM contentinpackage WHERE project ='$i' AND UPPER(contentpackage) IN ($packages)"
        echo importing contentinpackage for $i into AWS
        $psql $dbcxn -c "COPY contentinpackage FROM STDIN WITH (delimiter ',',FORMAT csv, HEADER true, ENCODING 'SQL_ASCII');" < $contentInPackagesFile
    fi

    echo .
done
