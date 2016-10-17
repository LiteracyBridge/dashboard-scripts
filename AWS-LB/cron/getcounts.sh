#!/bin/bash
# handling for spaces in filenames; with this they're not a separator
IFS="`printf '\n\t'`"

for f in *dropbox.log; do 
    n=$(grep Moving $f|wc -l)
    if [ $n -ne 0 ]; then  
        echo $f: $n; 
    fi; 
done
