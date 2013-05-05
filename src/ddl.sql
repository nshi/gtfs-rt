-- Note that all timestamp type columns in all tables are stored in GMT

CREATE TABLE vehicle_positions
(
  trip_id       varchar(40) NOT NULL,
  start_date    timestamp   NOT NULL,
  timestamp     timestamp   NOT NULL,
  stop_sequence integer     NOT NULL,
  relationship  tinyint     NOT NULL,
  latitude      float       NOT NULL,
  longitude     float       NOT NULL,

  PRIMARY KEY
  (
    trip_id, timestamp
  )
);

PARTITION TABLE vehicle_positions ON COLUMN trip_id;

CREATE TABLE trip_updates
(
  trip_id       varchar(40) NOT NULL,
  start_date    timestamp   NOT NULL,
  timestamp     timestamp   NOT NULL,
  relationship  tinyint     NOT NULL,
  stop_sequence integer     NOT NULL,
  delay         bigint      NOT NULL,

  PRIMARY KEY
  (
    trip_id, stop_sequence, timestamp
  )
);

PARTITION TABLE trip_updates ON COLUMN trip_id;

-- Sum up delays for a given stop of a trip, used to calculate average delays
CREATE VIEW v_trip_updates_delay_by_stop
(
  trip_id,
  stop_sequence,
  num_delays,
  total_delay
)
AS
SELECT trip_id,
       stop_sequence,
       COUNT(*),
       SUM(delay)
FROM trip_updates
GROUP BY trip_id, stop_sequence;

-- The following are GTFS tables

CREATE TABLE routes
(
  route_id         varchar(32) NOT NULL,
  agency_id        varchar(1)  DEFAULT NULL,
  route_short_name varchar(16) NOT NULL,
  route_long_name  varchar(50) DEFAULT NULL,
  route_desc       varchar(32) DEFAULT NULL,
  route_type       tinyint     NOT NULL,
  route_url        varchar(32) DEFAULT NULL,
  route_color      varchar(6)  DEFAULT NULL,
  route_text_color varchar(6)  DEFAULT NULL,

  PRIMARY KEY
  (
    route_id
  )
);

CREATE TABLE trips
(
  route_id     varchar(32)  NOT NULL,
  service_id   varchar(32)  NOT NULL,
  trip_id      varchar(40)  NOT NULL,
  headsign     varchar(128) DEFAULT NULL,
  direction_id tinyint      DEFAULT -1,
  block_id     varchar(16)  DEFAULT NULL,
  shape_id     varchar(16)  DEFAULT NULL,

  PRIMARY KEY
  (
    trip_id
  )
);

PARTITION TABLE trips ON COLUMN trip_id;

CREATE TABLE calendar
(
  service_id varchar(32) NOT NULL,
  weekdays   tinyint     NOT NULL, --compact field
  start_date timestamp   NOT NULL,
  end_date   timestamp   NOT NULL,

  PRIMARY KEY
  (
    service_id
  )
);

CREATE TABLE calendar_dates
(
  service_id     varchar(32) NOT NULL,
  date           timestamp   NOT NULL,
  exception_type tinyint     NOT NULL,

  PRIMARY KEY
  (
    service_id, date
  )
);

CREATE TABLE stops
(
  stop_id        varchar(32)  NOT NULL,
  stop_code      varchar(16)  DEFAULT NULL,
  stop_name      varchar(50)  NOT NULL,
  stop_desc      varchar(255) DEFAULT NULL,
  stop_lat       float        NOT NULL,
  stop_lon       float        NOT NULL,
  zone_id        varchar(16)  DEFAULT NULL,
  stop_url       varchar(16)  DEFAULT NULL,
  location_type  tinyint      DEFAULT 0,
  parent_station varchar(16)  DEFAULT NULL,

  PRIMARY KEY
  (
    stop_id
  )
);

CREATE TABLE stop_times
(
  trip_id        varchar(40) NOT NULL,
  arrival_time   timestamp   NOT NULL,
  departure_time timestamp   NOT NULL,
  stop_id        varchar(32) NOT NULL,
  stop_sequence  integer     NOT NULL,
  stop_headsign  varchar(16) DEFAULT NULL,
  pickup_type    tinyint     DEFAULT 0,
  drop_off_type  tinyint     DEFAULT 0,

  PRIMARY KEY
  (
    trip_id, stop_sequence
  )
);

PARTITION TABLE stop_times ON COLUMN trip_id;

-- Stored procedures
CREATE PROCEDURE FROM CLASS voltdb.gtfs.procedures.InsertCalendar;
CREATE PROCEDURE FROM CLASS voltdb.gtfs.procedures.InsertCalendarDates;
CREATE PROCEDURE FROM CLASS voltdb.gtfs.procedures.InsertStops;
CREATE PROCEDURE FROM CLASS voltdb.gtfs.procedures.InsertStopTimes;

CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.InsertPosition;
CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.InsertUpdate;

CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.GetLatestSchedule;
