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

How to fetch more data
=====

The fetcher.sh script can be used to download more realtime feeds from MBTA. It
polls vehicle position feeds from the MBTA website with the given interval.

For example, to download vehicle position feeds into the data/mbta/
sub-directory every 30 seconds,
```bash
./fetcher.sh data/mbta 30
```
