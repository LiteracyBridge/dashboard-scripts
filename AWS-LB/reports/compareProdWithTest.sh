#!/usr/bin/env bash
#
# Shell script to sort the contents of production and test DashboardReports directories.
#
# Allow comparing the results of test stats with prod stats, despite differences in the report output.
# (Other than headers, the order of the .csv files is unimportant.
#

function main() {

    rm -rf tmpSortedNew
    rm -rf tmpSortedOld

    sortDir ~/tmp/DashboardReports tmpSortedNew
    sortDir ~/Dropbox/DashboardReports tmpSortedOld

}


function sortDir() {
    local fromdir=$1
    local todir=$2

    echo "Sorting ${fromdir}"

    for f in $(find ${fromdir}); do
        local newfn=${f#"${fromdir}/"}
        #echo $newfn
        if [ -d $f ]; then
            mkdir -p ${todir}/${newfn}
        else
            local ext=${newfn##*.}
            if [ $ext = csv ]; then
                #echo "sorting $f"
                sort $f > ${todir}/${newfn}
            fi
        fi
    done

}

main "$@"

