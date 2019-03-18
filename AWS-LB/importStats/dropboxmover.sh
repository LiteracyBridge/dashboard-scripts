#!/usr/bin/env bash
traditionalIFS="$IFS"
IFS="`printf '\n\t'`"



# Set default values for any settings that aren't externally set.
function setDefaults() {
    if [ -z "${dropbox-}" ]; then
      dropbox=~/Dropbox
      echo "Use default dropbox: ${dropbox}"
    else
      echo "Use environment dropbox: ${dropbox}"
    fi
    
    if [ -z "${email-}" ]; then
        email=${dropbox}/AWS-LB/bin/sendses.py
        echo "Use default email: ${email}"
    else
        echo "Use environment email: ${email}"
    fi
}

function configure() {
    # From and to locations 
    inboxdir=${indir:=${dropbox}/inbox/stats}
    outboxdir=${outdir:=${dropbox}/outbox/stats}
    
    # This function needs the inbox and outbox directories. Client workstations aren't configured that way.
    if [[ ! -d ${inboxdir} || ! -d ${outboxdir} ]]; then
        echo "Error: one of inbox or outbox is missing"
        exit 1;
    fi

    dr_arg=""
    $dryrun && dr_arg="--dryrun"

    report=report.txt
    echo "Moving files @ $(date)">${report}
}

function main() {
    readArguments "$@"
    setDefaults
    configure || exit 1

    ./dropboxmover.py --dropbox "${dropbox}" ${dr_arg} >>${report}

    $execute && ${email} --subject 'Dropbox files moved' --body ${report}

}


# Moves files from one tree to another. Recreates the source tree in the destination tree,
# leaves the source tree intact.
function mover() {
    local srcdir="$1"&&shift
    local dstdir="$1"&&shift

    local lastdir=

    $dryrun && printf "Dry run, not moving files.\n">>${report}
    $summary && (
        printf "Moving files from source: ${srcdir}\n">>${report}
        printf "          to destination: ${dstdir}\n\n">>${report}
    )

    filesmoved=0
    for frompath in $(find "${srcdir}" -type f); do

        local filename="${frompath##*/}"
        local firstch="${filename:0:1}"
        
        # Ignore hidden files
        if [ ${firstch} != "." ]; then
            # Compute the relevant paths and names
            local relpath="${frompath#${srcdir}}"
            local reldir="${relpath%/*}"
            local todir="${dstdir}${reldir}"
            local topath="${todir}/${filename}"
            
            $trace && (
                printf "filename: ${filename}\n"
                printf "    path: ${frompath}\n"
                printf " relpath: ${relpath}\n"
                printf "  reldir: ${reldir}\n"
                printf "   todir: ${todir}\n"
                printf "  topath: ${topath}\n"
                printf "\n"
            )
            if [[ $summary && "${lastdir}" != "${reldir}" ]]; then
                printf "Processing directory ${reldir}\n">>${report}
                lastdir="${reldir}"
            fi

            # Make the commands, so that they can be displayed and/or executed
            mkdircmd=(mkdir -p "${todir}")
            mvcmd=(mv "${frompath}" "${topath}")

            $verbose && echo "${mkdircmd[@]}">>${report}
            $execute && "${mkdircmd[@]}"
            
            $verbose && echo "${mvcmd[@]}">>${report}
            $execute && "${mvcmd[@]}"
            
            filesmoved=$[filesmoved+1]
        fi
        
        if [ $filesmoved -gt $limit ]; then exit 1; fi

    done


}

declare -a remainingArgs=()
function readArguments() {
    local readopt='getopts $opts opt;rc=$?;[ $rc$opt == 0? ]&&exit 1;[ $rc == 0 ]||{ shift $[OPTIND-1];false; }'
    indir=
    outdir=
    verbose=false
	dryrun=false
    summary=false
    trace=false
	limit=99999
	
    # limit:, no-execute, summary, trace, verbose, input-dir:, output-dir:
    opts=l:nstvi:o:

    # Enumerating options
    while eval $readopt
    do
        #echo OPT:$opt ${OPTARG+OPTARG:$OPTARG}
        case "${opt}" in
		l) limit=${OPTARG};;
	    n) dryrun=true;;
        s) summary=true;;
        t) trace=true;;
        v) verbose=true;;
        o) outdir=${OPTARG};;
        i) indir=${OPTARG};;
        *) printf "OPT:$opt ${OPTARG+OPTARG:$OPTARG}" >&2;;
        esac
   done
   
    # Enumerating arguments, collect into an array accessible outside the function
	remainingArgs=()
    for arg
    do
	    remainingArgs+=("$arg")
    done
    # When the function returns, the following sets the unprocessed arguments back into $1 .. $9
    # set -- "${remainingArgs[@]}"

    # execute is the opposite of dryrun
    execute=true
    $dryrun && execute=false

    $verbose && summary=true
}

main "$@"

