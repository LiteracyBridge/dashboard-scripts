#!/bin/sh
# uncomment next line for script debugging
#set -x

if [ -z "$psql" ]; then
  psql=/Applications/Postgres.app/Contents/Versions/9.4/bin/psql
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
if [ ! -d "$projectdir" ]; then
   mkdir $projectdir
fi   
for report in `cat $codebasedir/reportsByAll.txt`
 do 
   echo "  REPORT:$report"
   #COMMENTING OUT SINCE NOT USING THESE REPORTS (SEE COMMENT BELOW)
   #if [ ! -d "$sqldir/$report" ]; then
   #  mkdir $sqldir/$report
   #fi   

   exportdir=$projectdir
   $psql $dbcxn -A -F "^" -f $sqldir/$report.sql | sed 's/\^/\",\"/g' | sed 's/^/\"/' | sed 's/$/\"/' > "$exportdir/$report.csv" 

   #COMMENTING OUT LINE BELOW SINCE IT DOUBLES EXECUTION TIME JUST TO GET THE EXACT QUERY RUN, WHICH CAN BE DERIVED FROM THE VARIABLES AND .SQL FILE
   #$psql $dbcxn -e -f $sqldir/$report.sql > "$sqldir/$report/$report.txt" 
done
#COMMENTING OUT THE CALL TO FTP REPORTS -- JUST LEAVE CSV FILES IN DROPBOX FOLDER FOR NOW
#$codebasedir/ftpReports.sh $project $report 
   



#### ITERATE THROUGH PROJECTS
projects=($($psql $dbcxn -c "SELECT projectcode from projects WHERE id >= 0" -t))
echo ""
echo WILL NOW ITERATE THROUGH PROJECTS: ${projects[@]}
for project in "${projects[@]}"
  do
   echo ___________________________________
   echo ""
   echo PROJECT:$project
   projectdir=$outputbasedir"/$project"
   if [ ! -d "$projectdir" ]; then
	  mkdir $projectdir
   fi   

   #### RUN PROJECT REPORTS
   echo ""
   echo "  PROJECT REPORTS"
   for report in `cat $codebasedir/reportsByPrj.txt`
     do 
       echo "    REPORT:$report"
 
       #COMMENTING OUT SINCE NOT USING THESE REPORTS (SEE COMMENT BELOW)
       #if [ ! -d "$sqldir/$report" ]; then
       #  mkdir $sqldir/$report
       #fi   
 
       exportdir=$projectdir"/ALL_DEPLOYMENTS"
       if [ ! -d "$exportdir" ]; then
    	  mkdir $exportdir
       fi   
       $psql $dbcxn -A -F "^" -f $sqldir/$report.sql -v prj=$project | sed 's/\^/\",\"/g' | sed 's/^/\"/' | sed 's/$/\"/' > "$exportdir/$project-$report.csv" 
 
       #COMMENTING OUT LINE BELOW SINCE IT DOUBLES EXECUTION TIME JUST TO GET THE EXACT QUERY RUN, WHICH CAN BE DERIVED FROM THE VARIABLES AND .SQL FILE
       #$psql $dbcxn -e -f $sqldir/$report.sql -v prj=$project > "$sqldir/$report/$project-$report.txt" 
   done
   #COMMENTING OUT THE CALL TO FTP REPORTS -- JUST LEAVE CSV FILES IN DROPBOX FOLDER FOR NOW
   #$codebasedir/ftpReports.sh $project $report 

   #### ITERATE THROUGH DEPLOYMENTS AND RUN REPORTS
   echo ""
   echo "  DEPLOYMENT REPORTS"
   deployments=($($psql $dbcxn -c "SELECT deployment from (SELECT distinct deployment, \"startDate\" from packagesindeployment WHERE project ='$project' ORDER BY \"startDate\" DESC, deployment) foo" -t))
   for report in `cat $codebasedir/reportsByDepl.txt`
     do 
       echo "    REPORT:$report"
       #COMMENTING OUT SINCE NOT USING THESE REPORTS (SEE COMMENT BELOW)
       #if [ ! -d "$sqldir/$report" ]; then
       #  mkdir $sqldir/$report
       #fi   
 
       exportdir=$projectdir"/$report"
       if [ ! -d "$exportdir" ]; then
    	  mkdir $exportdir
       fi   
       if [ -f "$exportdir/$project-$report-all.csv" ]; then
	       rm "$exportdir/$project-$report-all.csv"
	   fi
       for deployment in "${deployments[@]}"
        do 
         echo "      DEPLOYMENT:$deployment"
         $psql $dbcxn -A -F "^" -f $sqldir/$report.sql -v prj=$project -v depl=$deployment | sed 's/\^/\",\"/g' | sed 's/^/\"/' | sed 's/$/\"/' > "$exportdir/$project-$report-$deployment.csv" 

         #COMMENTING OUT LINE BELOW SINCE IT DOUBLES EXECUTION TIME JUST TO GET THE EXACT QUERY RUN, WHICH CAN BE DERIVED FROM THE VARIABLES AND .SQL FILE
         #$psql $dbcxn -e -f $sqldir/$report.sql -v prj=$project -v depl=$deployment > "$sqldir/$report/$project-$report-$deployment.txt" 

         cat "$exportdir/$project-$report-$deployment.csv" >> "$exportdir/$project-$report-all.csv"
        done
        #COMMENTING OUT THE CALL TO FTP REPORTS -- JUST LEAVE CSV FILES IN DROPBOX FOLDER FOR NOW
        #$codebasedir/ftpReports.sh $project $report 
   done

   #### ITERATE THROUGH PACKAGES AND RUN REPORTS
   echo "  PACKAGE REPORTS"
   packages=($($psql $dbcxn -c "SELECT contentpackage from packagesindeployment WHERE project ='$project' ORDER BY \"startDate\" DESC,contentpackage" -t))
   for report in `cat $codebasedir/reportsByPkg.txt`
     do 
       echo "    REPORT:$report"
       #COMMENTING OUT SINCE NOT USING THESE REPORTS (SEE COMMENT BELOW)
       #if [ ! -d "$sqldir/$report" ]; then
       #  mkdir $sqldir/$report
       #fi   
 
       exportdir=$projectdir"/$report"
       if [ ! -d "$exportdir" ]; then
    	  mkdir $exportdir
       fi   
       if [ -f "$exportdir/$project-$report-all.csv" ]; then
          rm "$exportdir/$project-$report-all.csv"
	   fi
       for package in "${packages[@]}"
        do 
         echo "      PACKAGE:$package"
         $psql $dbcxn -A -F "^" -f $sqldir/$report.sql -v prj=$project -v pkg=$package | sed 's/\^/\",\"/g' | sed 's/^/\"/' | sed 's/$/\"/' > "$exportdir/$project-$report-$package.csv" 

         #COMMENTING OUT LINE BELOW SINCE IT DOUBLES EXECUTION TIME JUST TO GET THE EXACT QUERY RUN, WHICH CAN BE DERIVED FROM THE VARIABLES AND .SQL FILE
         #$psql $dbcxn -e -f $sqldir/$report.sql -v prj=$project -v pkg=$package > "$sqldir/$report/$project-$report-$package.txt" 
 
         cat "$exportdir/$project-$report-$package.csv" >> "$exportdir/$project-$report-all.csv"
        done
        #COMMENTING OUT THE CALL TO FTP REPORTS -- JUST LEAVE CSV FILES IN DROPBOX FOLDER FOR NOW
        #$codebasedir/ftpReports.sh $project $report 
     done
done
