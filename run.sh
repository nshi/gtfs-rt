#!/usr/bin/env bash

APPNAME="gtfs"

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

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
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
        src/java/org/voltdb/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
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

function dump() {
    srccompile
    java -classpath obj:$APPCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        org.voltdb.GtfsExample \
        $1
}

function help() {
    echo "Usage: ./run.sh {clean|dump}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -ge 1 ]; then $@; else server; fi
