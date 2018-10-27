#!/bin/bash
# ec2-user@ec2-54-191-197-187.us-west-2.compute.amazonaws.com
user="ubuntu"
instanceid="i-08645deb18a69cd9e"
host=$(aws ec2 describe-instances --instance-ids "${instanceid}" --query 'Reservations[].Instances[].PublicDnsName' --output text)
path="work/RDSBackup"
key="~/.ssh/lb-stats-machine.pem.txt"

set -x

time rsync -ve 'ssh -i /Users/bill/.ssh/lb-stats-machine.pem.txt' ${user}@${host}:${path}/schema.sql .

time rsync -ve 'ssh -i /Users/bill/.ssh/lb-stats-machine.pem.txt' ${user}@${host}:${path}/database.data .
