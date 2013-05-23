-- Note that all timestamp type columns in all tables are stored in GMT

CREATE TABLE vehicle_positions
(
  trip_id       varchar(40) NOT NULL,
  start_date    varchar(8)  NOT NULL,
  start_usec    bigint      NOT NULL, -- pseudo-date
  timestamp     timestamp   NOT NULL,
  time_usec     bigint      NOT NULL, -- pseudo-timestamp
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
  start_date    varchar(8)  NOT NULL,
  start_usec    bigint      NOT NULL, -- pseudo-date
  timestamp     timestamp   NOT NULL,
  time_usec     bigint      NOT NULL, -- pseudo-timestamp
  relationship  tinyint     NOT NULL,

  PRIMARY KEY
  (
    trip_id, start_date
  )
);

PARTITION TABLE trip_updates ON COLUMN trip_id;

CREATE TABLE stop_time_updates
(
  trip_id        varchar(40) NOT NULL,
  start_date     varchar(8)  NOT NULL,
  start_usec     bigint      NOT NULL, -- pseudo-date
  time_usec      bigint      NOT NULL, -- pseudo-timestamp (batch update id)
  stop_sequence  integer     NOT NULL,
  delay          bigint      NOT NULL,

  PRIMARY KEY
  (
    trip_id, start_date, stop_sequence
  )
);

PARTITION TABLE stop_time_updates ON COLUMN trip_id;

CREATE TABLE effective_stop_times
(
  trip_id        varchar(40) NOT NULL,
  start_date     varchar(8)  NOT NULL, -- a delay will be specific to the trip on a particular date
  start_usec     bigint      NOT NULL, -- pseudo-timestamp
  time_usec      bigint      NOT NULL, -- pseudo-timestamp (diagnostic source batch update id)
  arrival_usec   bigint      NOT NULL, -- pseudo-time-of-day
  departure_usec bigint      NOT NULL, -- pseudo-time-of-day
  stop_sequence  integer     NOT NULL,

  PRIMARY KEY
  (
    trip_id, start_date, stop_sequence
  )
);

PARTITION TABLE effective_stop_times ON COLUMN trip_id;

-- The following are (static) GTFS tables

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
  start_date varchar(8)  NOT NULL,
  start_usec bigint      NOT NULL, -- pseudo-timestamp
  end_date   varchar(8)  NOT NULL,
  end_usec   bigint      NOT NULL, -- pseudo-timestamp

  PRIMARY KEY
  (
    service_id
  )
);

CREATE TABLE calendar_dates
(
  service_id     varchar(32) NOT NULL,
  date           varchar(8)  NOT NULL,
  date_usec      bigint      NOT NULL, -- pseudo-timestamp
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
  arrival_time   varchar(8)  NOT NULL,
  arrival_usec   bigint      NOT NULL, -- pseudo-time-of-day
  departure_time varchar(8)  NOT NULL,
  departure_usec bigint      NOT NULL, -- pseudo-time-of-day
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
CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.InsertStopTimeUpdates;
CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.EffectStopTimeUpdates;

CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.GetLatestSchedule;
CREATE PROCEDURE FROM CLASS voltdb.realtime.procedures.FindBetterTime;

CREATE PROCEDURE FindTrips AS
    SELECT st.trip_id trip_id, st.stop_sequence stop_sequence
    FROM stops s, stop_times st, trips t -- add? , routes r
    WHERE st.stop_id = s.stop_id
    AND t.trip_id = st.trip_id
    -- add? AND t.route_id = st.trip_id
    AND s.stop_name = ?
    AND t.route_id = ? -- switch to? r.route_short_name = ?
    ORDER BY 1, 2
    ;

-- Get the scheduled stop time with a possible overriding effective (rescheduled) time.
CREATE PROCEDURE CheckArrival AS
    SELECT est.arrival_usec effective_arrival, st.arrival_usec fallback_arrival
    FROM stop_times st LEFT JOIN effective_stop_times est
    ON st.trip_id = est.trip_id AND st.stop_sequence = est.stop_sequence AND est.start_usec = ?
    WHERE st.trip_id = ?
    AND st.stop_sequence = ?
    ORDER BY 2, 1 -- Not required since record should be unique, but this smells more deterministic.
    LIMIT 1
    ;
PARTITION PROCEDURE CheckArrival ON TABLE stop_times COLUMN trip_id PARAMETER 1;
    
