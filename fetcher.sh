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

URL="http://developer.mbta.com/lib/gtrtfs/Vehicles.pb"

function help() {
    echo "Usage: ./fetcher.sh output_dir [interval]"
    echo "    Poll the data every interval seconds, this runs until ctrl-c is pressed."
}

# call ./run.sh loadrt filename to load the data into database
function load() {
    if [ $# -eq 0 ]; then help && exit; fi

    $this/run.sh loadrt $1
}

# download the current vehicle positions to file
function fetch() {
    INTERVAL=10                 # default to every 10 seconds
    if [ $# -eq 0 ]; then help && exit; fi
    if [ $# -eq 2 ]; then INTERVAL=$2; fi

    while true
    do
        NOW=`date +"%Y%m%d-%H%M%S"`
        FILENAME="mbta-vehicles-$NOW.pb"
        FILEPATH="$1/$FILENAME"
        if [ ! -f $FILEPATH ]
        then
            echo wget -O $FILEPATH $URL

            # load $FILEPATH
        fi

        sleep $INTERVAL
    done
}

if [ $# -ge 1 ]; then fetch "$@"; else help; fi
