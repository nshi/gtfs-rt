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
NOW=`date +"%Y%m%d-%H%M%S"`
FILENAME="mbta-vehicles-$NOW.pb"

function help() {
    echo "Usage: ./fetcher.sh output_dir"
}

# call ./run.sh loadrt filename to load the data into database
function load() {
    if [ $# -eq 0 ]; then help && exit; fi

    $this/run.sh loadrt $1
}

# download the current vehicle positions to file
function fetch() {
    if [ $# -eq 0 ]; then help && exit; fi

    FILEPATH="$1/$FILENAME"
    if [ -f $FILEPATH ]; then exit; fi
    wget -O $FILEPATH $URL

    # load $FILEPATH
}

if [ $# -ge 1 ]; then fetch "$@"; else help; fi
