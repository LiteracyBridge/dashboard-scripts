#!/bin/bash
set -x

# This will put back the recently imported stats, so they can be imported again. ONLY FOR TESTING.
# Assumes that there's only one batch of stats to be restored. USE WITH CAUTION, AND NOT IN PRODUCTION!

# Move files (presumably .zip files) from collected-data-processed back to the stats directory.
for f in $(find ../collected-data-processed/2* -type f)
do
    mv $f ../stats/
done
# Remove any .DS_Store files from the collected-data-processed directory
for f in ../collected-data-processed/2*
do
    if [ -e $f/.DS_Store]; then rm $f/.DS_Store; fi
done
# With the .DS_Store files gone, remove collected-data-processed's presumably only subdirectory
rmdir ../collected-data-processed/2*

# Go to the stats directory, and unzip all of the .zip files, then delete them.
cd ../stats
for f in *.zip
do
    unzip $f
done
rm *.zip

