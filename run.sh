#!/usr/bin/env bash

APPNAME="gtfs-rt"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_LIB="`pwd`/../../lib"
    VOLTDB_VOLTDB="`pwd`/../../voltdb"
fi

export CLASSPATH="obj"
APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
CSVLOADER="$VOLTDB_BIN/csvloader"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar voltdbroot voltdbroot
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -target 1.6 -source 1.6 -classpath $APPCLASSPATH -d obj \
        src/java/com/google/transit/realtime/*.java \
        src/java/voltdb/*.java \
        src/java/voltdb/gtfs/procedures/*.java \
        src/java/voltdb/realtime/procedures/*.java \
        src/java/voltdb/realtime/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar src/ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment deployment.xml \
        license $LICENSE host $HOST
}

function loadgtfs() {
    SERVER="localhost"
    if [ $# -eq 1 ]; then SERVER=$1; fi

    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/routes.txt routes
    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/trips.txt trips
    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/calendar.txt -p InsertCalendar
    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/calendar_dates.txt -p InsertCalendarDates
    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/stops.txt -p InsertStops
    $CSVLOADER --skip 1 -s $SERVER -f data/mbta/gtfs/stop_times.txt -p InsertStopTimes
}

# load realtime feeds
function loadrt() {
    if [ $# -eq 0 ]; then echo "loadrt FILENAME [SERVER:PORT]" && exit; fi
    SERVER="localhost"
    if [ $# -eq 2 ]; then SERVER=$2; fi
    echo Loading "$1" into $SERVER
    # run the loader
    java -classpath obj:$APPCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voltdb.realtime.Loader "$1" $SERVER
}

# Start the web server, requires Java 7
function webserver() {
    export VERTX_MODS=`pwd`/src/webserver
    cd src/webserver
    vertx run webserver.groovy
}

function help() {
    echo "Usage: ./run.sh {clean|catalog|server|loadgtfs|loadrt|webserver}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -ge 1 ]; then "$@"; else server; fi
