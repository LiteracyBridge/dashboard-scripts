#!/bin/bash

echo "Stats import and processing started at $(date)"
echo "path:$PATH"
cd ~/Dropbox/AWS-LB
./runAll.sh
echo "Stats import and processing finished at $(date)"

