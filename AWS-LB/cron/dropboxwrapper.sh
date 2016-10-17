#!/bin/bash

echo "Dropbox Change Processor started at $(date)"
echo "path:$PATH"
cd ~/DropboxChangeProcessor/bin
./DropboxChangeProcessor dropbox.key ec2-config.properties
echo "Dropbox Change Processor finished at $(date)"

