#!/bin/bash

echo "Dropbox Mover started at $(date)"
echo "path:$PATH"
cd ~/Dropbox/AWS-LB/importStats
./dropboxmover.sh -v
echo "Dropbox Mover finished at $(date)"

#echo "Dropbox Change Processor started at $(date)"
#echo "path:$PATH"
#cd ~/DropboxChangeProcessor/bin
#./DropboxChangeProcessor dropbox.key ec2-config.properties
#echo "Dropbox Change Processor finished at $(date)"

