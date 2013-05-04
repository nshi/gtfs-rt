A GTFS-realtime application that tracks realtime public transportation
information using a VoltDB database.

How to use
=====

Download the latest VoltDB 3.x and unpack it. Clone this project into the
examples/ sub-directory in the unpacked VoltDB distribution.

To start a server on localhost:
```bash
./run.sh server
```

To populate the database with static GTFS information:
```bash
./run.sh loadgtfs
```

To start loading GTFS-realtime data, replace GTFS_REALTIME_FILENAME with a
realtime protobuf feed:
```bash
./run.sh loadrt GTFS_REALTIME_FILENAME
```

If you have a list of realtime feeds downloaded in a directory like the ones we
provide in the data/mbta/ directory, you can use the following command to load
them all:
```bash
./run.sh loadrt data/mbta
```

The realtime feed can be either vehicle positions or trip updates. So far, I
have only tested with feeds from
[MBTA](http://www.mbta.com/rider_tools/developers/default.asp?id=22393)

If the server is running on a different machine or on a non-standard VoltDB
port, you can specify the hostname:port of the server machine after the feed
path like this
```bash
./run.sh loadrt data/mbta localhost:31313
```

How to fetch more data
=====

The fetcher.sh script can be used to download more realtime feeds from MBTA. It
polls vehicle position feeds from the MBTA website with the given interval.

For example, to download vehicle position feeds into the data/mbta/
sub-directory every 30 seconds,
```bash
./fetcher.sh data/mbta 30
```

How to download pre-collected data
=====

We have collected some data since April 14 2013 in case you want to play with
them. The data is stored in a S3 bucket. The total size is a little bit less
than 2GB compressed. To download all of them, use the following command.
```bash
wget --recursive --no-clobber http://gtfs-rt-data.s3-website-us-east-1.amazonaws.com/
```

This command will download all the files into a directory called
"gtfs-rt-data.s3-website-us-east-1.amazonaws.com" under your current working
directory.

The content of this directory should look like this
```bash
index.html
positions/
updates/
```

You can ignore the index.html file. The positions/ directory contains tarballs
of vehicle positions updates. The updates/ directory contains tarballs contains
trip updates.

To unpack all updates, do the following
```bash
tar xzf positions/*
tar xzf updates/*
```

The unpacked version is quite large, around 8GB. So make sure you have enough
free space before unpacking them.

Once unpacked, the directories are named by dates. You can load them in order by
passing each dated directory to the run.sh script. For example,
```bash
./run.sh positions/20130416
```
