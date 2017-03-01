#!/bin/bash
# ec2-user@ec2-54-191-197-187.us-west-2.compute.amazonaws.com
time rsync -ve 'ssh -i ~/.ssh/lb-stats-machine.pem.txt' ec2-user@ec2-54-149-230-198.us-west-2.compute.amazonaws.com:/home/ec2-user/RDSBackup/schema.sql .

time rsync -ve 'ssh -i ~/.ssh/lb-stats-machine.pem.txt' ec2-user@ec2-54-149-230-198.us-west-2.compute.amazonaws.com:/home/ec2-user/RDSBackup/database.data .
