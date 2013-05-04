#!/usr/bin/env bash

# resolve symlinks and canonicalize the path (make it absolute)
pushd . > /dev/null
this=$0
cd `dirname $this`
this=`basename $this`
while [ -L "$this" ]
do
    this=`readlink $this`
    cd `dirname $this`
    this=`basename $this`
done
this="$(pwd -P)"
popd > /dev/null

POSITION_URL="http://developer.mbta.com/lib/gtrtfs/Vehicles.pb"
UPDATE_URL="http://developer.mbta.com/lib/gtrtfs/Passages.pb"

function help() {
    echo "Usage: ./fetcher.sh output_dir [interval [hostname:port]]"
    echo "    Poll the data every interval seconds, this runs until ctrl-c is pressed."
}

# call ./run.sh loadrt filename to load the data into database
function load() {
    if [ $# -eq 0 ]; then help && exit; fi

    $this/run.sh loadrt "$1" $2
}

# download the current feed to file and load it into database
# $1 is URL
# $2 is filename prefix
# $3 is output_dir
# $4 is interval
# $5 is hostname:port
function fetch() {
    URL=$1
    PREFIX=$2
    OUTPUT_DIR=$3
    INTERVAL=$4
    SERVER=$5

    while true
    do
        NOW=`date +"%Y%m%d-%H%M%S"`
        FILENAME="$PREFIX-$NOW.pb"
        FILEPATH="$OUTPUT_DIR/$FILENAME"
        if [ ! -f $FILEPATH ]
        then
            wget -O "$FILEPATH" $URL

            load "$FILEPATH" $SERVER
        fi

        sleep $INTERVAL
    done
}

# download both position updates and trip updates and load them into database
# $1 is output_dir
# $2 is interval
# $3 is hostname:port
function fetch_all() {
    INTERVAL=10                 # default to every 10 seconds
    if [ $# -ge 2 ]; then INTERVAL=$2; fi

    SERVER="localhost"
    if [ $# -ge 3 ]; then SERVER=$3; fi

    fetch $POSITION_URL "mbta-vehicles" $1 $INTERVAL $SERVER
    fetch $UPDATE_URL "mbta-updates" $1 $INTERVAL $SERVER
}

if [ $# -ge 1 ]; then fetch_all "$@"; else help; fi
