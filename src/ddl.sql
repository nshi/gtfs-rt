CREATE TABLE vehicle_positions
(
  trip_id       varchar(40) NOT NULL,
  start_date    timestamp   NOT NULL,
  timestamp     timestamp   NOT NULL,
  stop_sequence integer     DEFAULT 0,
  status        tinyint     NOT NULL,
  latitude      float       NOT NULL,
  longitude     float       NOT NULL
);

PARTITION TABLE vehicle_positions ON COLUMN trip_id;

-- The following GTFS tables are all replicated

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
  route_text_color varchar(6)  DEFAULT NULL
);

CREATE TABLE trips
(
  route_id     varchar(32)  NOT NULL,
  service_id   varchar(32)  NOT NULL,
  trip_id      varchar(40)  NOT NULL,
  headsign     varchar(128) DEFAULT NULL,
  direction_id tinyint      DEFAULT -1,
  block_id     varchar(16)  DEFAULT NULL,
  shape_id     varchar(16)  DEFAULT NULL
);

CREATE TABLE calendar
(
  service_id varchar(32) NOT NULL,
  weekdays   tinyint     NOT NULL, --compact field
  start_date timestamp   NOT NULL,
  end_date   timestamp   NOT NULL,
);

CREATE TABLE calendar_dates
(
  service_id     varchar(32) NOT NULL,
  date           timestamp   NOT NULL,
  exception_type tinyint     NOT NULL
);

-- Stored procedures
CREATE PROCEDURE FROM CLASS voltdb.gtfs.procedures.InsertCalendar;
