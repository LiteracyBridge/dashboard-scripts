#!/bin/bash
# ec2-user@ec2-54-191-197-187.us-west-2.compute.amazonaws.com
user="ec2-user"
host=$(aws ec2 describe-instances --instance-ids "i-4374b784" --query 'Reservations[].Instances[].PublicDnsName' --output text)
path="/home/ec2-user/RDSBackup"

time rsync -ve 'ssh -i /Users/bill/.ssh/lb-stats-machine.pem.txt' ${user}@${host}:${path}/schema.sql .

time rsync -ve 'ssh -i /Users/bill/.ssh/lb-stats-machine.pem.txt' ${user}@${host}:${path}/database.data .
