#!/bin/bash
set -eu
IFS="`printf '\n\t'`"
rm $(find . -maxdepth 1 -ctime +14 \( -iname '*.err' -o -iname '*.log' \))
