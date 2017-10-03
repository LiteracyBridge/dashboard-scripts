#!/usr/bin/env bash
dropbox=~/dropbox
cp=${dropbox}/LB-software/ACM-install/ACM/software/

function main() {
    # Clean up
    rm  *-out.csv
    rm grouped*

    # Export individual ACMs.
    for f in $(cd ${dropbox}; ls -d "$1"*); do
        exportMessagsOneAcm $f
    done

    # Save list of csv files
    csvs=$(ls *.csv)
    # Count categories in each file individually
    for f in ${csvs}; do
        countCategories "$f"
    done
    countCategories ${csvs} --output grouped-out.csv
}

# Export metadata from one ACM.
function exportMessagsOneAcm() {
    acmname=$(echo $1|tr /a-z/ /A-Z/)
    shift
    # if doesn't start with ACM-, make it
    prefix=${acmname:0:4}
    if [ "${prefix}" != "ACM-" ]; then
        acmname="ACM-${acmname}"
    fi

    csvname="$(echo ${acmname}|tr [:upper:] [:lower:]).csv"
    rawname="$(echo ${acmname}|tr [:upper:] [:lower:]).raw"
    echo $acmname $csvname
    
    java -cp ${cp}/acm.jar:${cp}/lib/*:${cp}/resources/ org.literacybridge.acm.tools.CSVDatabaseExporter $acmname $rawname -n
    # some have embedded nulls; clean them
    tr -d '\000' <${rawname} >${csvname}
    rm ${rawname}
}

function countCategories() {
    echo $@
    python categoryCounter.py $@
}

main "$@"