#!/bin/sh
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

codebasedir="$dropbox/AWS-LB/reports"
outputbasedir="$dropbox/DashboardReports"
sqldir=$codebasedir"/sql"

mkdir -p $outputbasedir

#### NOTE: A BETTER WAY TO DO THIS WOULD PROBABLY JUST BE TO STICK EACH TYPE OF REPORT QUERY
#### (E.G. PROJECT, DEPLOYMENT, PACKAGE) INTO A FOLDER AND THEN THIS SCRIPT WOULD ITERATE
#### THROUGH WHATEVER .SQL FILES ARE IN THERE, RATHER THAN TO HAVE TO ADD THEIR FILENAME
#### TO THE TEXT FILES AS WE DO NOW.

#### RUN CROSS-PROJECT REPORTS
echo "CROSS-PROJECT REPORTS"
projectdir=$outputbasedir"/ALL_PROJECTS"
mkdir -p $projectdir
for report in `cat $codebasedir/reportsByAll.txt`; do 
    echo "  REPORT:$report"

    exportdir=$projectdir
    $psql $dbcxn -A -f $sqldir/$report.sql > "$exportdir/$report.csv" 

done



#### ITERATE THROUGH PROJECTS
projects=($($psql $dbcxn -c "SELECT projectcode from projects WHERE id >= 0" -t))
echo ""
echo WILL NOW ITERATE THROUGH PROJECTS: ${projects[@]}
for project in "${projects[@]}"; do
    echo ___________________________________
    echo ""
    echo PROJECT:$project
    projectdir=$outputbasedir"/$project"
    mkdir -p $projectdir

    #### RUN PROJECT REPORTS
    echo ""
    echo "  PROJECT REPORTS"
    for report in `cat $codebasedir/reportsByPrj.txt`; do 
        echo "    REPORT:$report"

        exportdir=$projectdir"/ALL_DEPLOYMENTS"
        mkdir -p $exportdir
        $psql $dbcxn -A -f $sqldir/$report.sql -v prj=$project > "$exportdir/$project-$report.csv" 

    done

    #### ITERATE THROUGH DEPLOYMENTS AND RUN REPORTS
    echo ""
    echo "  DEPLOYMENT REPORTS"
    deployments=($($psql $dbcxn -c "SELECT deployment from (SELECT distinct deployment, startdate from packagesindeployment WHERE project ='$project' ORDER BY startdate DESC, deployment) foo" -t))
    for report in `cat $codebasedir/reportsByDepl.txt`; do 
        echo "    REPORT:$report"

        exportdir=$projectdir"/$report"
        mkdir -p $exportdir
        if [ -f "$exportdir/$project-$report-all.csv" ]; then
            rm "$exportdir/$project-$report-all.csv"
        fi
        # We want to accumulate the header line, the first line, from the first .csv 
        firstline="+1"
        for deployment in "${deployments[@]}"; do 
            echo "      DEPLOYMENT:$deployment"
            $psql $dbcxn -A -f $sqldir/$report.sql -v prj=$project -v depl=$deployment > "$exportdir/$project-$report-$deployment.csv" 

            # Accumulate the .csv into the 'all' .csv file
            tail -n $firstline "$exportdir/$project-$report-$deployment.csv" >> "$exportdir/$project-$report-all.csv"
            # We do not want to accumulate the header line for subsequent .csv files
            firstline="+2"
        done
    done

    #### ITERATE THROUGH PACKAGES AND RUN REPORTS
    echo "  PACKAGE REPORTS"
    packages=($($psql $dbcxn -c "SELECT contentpackage from packagesindeployment WHERE project ='$project' ORDER BY startdate DESC,contentpackage" -t))
    for report in `cat $codebasedir/reportsByPkg.txt`; do 
        echo "    REPORT:$report"

        exportdir=$projectdir"/$report"
        mkdir -p $exportdir
        if [ -f "$exportdir/$project-$report-all.csv" ]; then
            rm "$exportdir/$project-$report-all.csv"
        fi
        # We want to accumulate the header line, the first line, from the first .csv 
        firstline="+1"
        for package in "${packages[@]}"; do 
            echo "      PACKAGE:$package"
            $psql $dbcxn -A -f $sqldir/$report.sql -v prj=$project -v pkg=$package > "$exportdir/$project-$report-$package.csv" 

            # Accumulate the .csv into the 'all' .csv file
            tail -n $firstline "$exportdir/$project-$report-$package.csv" >> "$exportdir/$project-$report-all.csv"
            # We do not want to accumulate the header line for subsequent .csv files
            firstline="+2"
        done
    done
done
