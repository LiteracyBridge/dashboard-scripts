Note for testing: to connect to a database other than the production RDS database, edit
dashboard.properties to point to the correct database instance (or provide a test version
of the .properties file). IMPORTANT: you must move or rename any global file, located in
/opt/literacybridge/dashboard.properies; that file takes precedence, if it exists.


Initialization
- Set environment variables if not already set:
  psql      - postgresql app, 9.5, 9.4, $(which psql)
  dbcxn     - postgresql connection, 
              "--host=lb-device-usage.ccekjtcevhb7.us-west-2.rds.amazonaws.com 
               --port 5432 --username=lb_data_uploader --dbname=dashboard"
  dropbox   - ~/Dropbox (works because server has a symbolic link from Dropbox)
  bin       - location of Amplio software, ${dropbox}/AWS-LB/bin 
  core      - location of stats importer, ${dropbox}/AWS-LB/bin/core-with-deps.jar
  acm       - location of acm.jar, ${dropbox}/LB-software/ACM-install/ACM/software
  email     - location of SES email sender, ${dropbox}/AWS-LB/bin/sendses.py
  s3bucket  - bucket name into which stats have been placed, s3://acm-stats

- Get the current year, month, and day. 
- dailyDir = ${dropbox}/collected-data-processed/${year}/${month}/${day}
  There can be multiple files and directories of data under that directory.


- Gather files
  All statistics are now sent through S3. 
  - Statistics from a laptop or phone are zipped and uploaded to
    ${s3bucket}/collected-data/${tbloader-id}/${timestamped-zip-file}
      where tbloader-id is an id that is unique to a single user (a user may have several,
         over time)
        timestamped-zip-file is a zip file named by the timestamp of its creation, to 1ms.
          If a single user runs two devices, which create a .zip file within the same ms, 
          we could have a collision. Considered to be an acceptable risk.

  - The individual files are downloaded and unzipped. The contents of the zip are structured
     ${program}
         userrecordings / ${package} / ${tbloader-id} / ${directory}
             ${tb-srn}_${random}.a18
             ...
         TalkingBookData / ${package} / ${tbloader-id} / ${directory} / ${tb-srn}
             ${funky-timestamp}-${tbloader-id}.zip
     where program is the program that is the soruce of these recordings and statistics
         package is a possibly-not-unique name of the package that is the source
             of these user recordings and statistics.
         directory is a stand-in for recipientid. (recipientid_map allows mapping back
             to the recipient)
         funky-timestamp is like 2020y10m02d15h32m47s for 2020-10-02T15:32:47