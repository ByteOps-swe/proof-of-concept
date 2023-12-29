CREATE TABLE IF NOT EXISTS temperature_table
(
    `sensor_id` String,
    `sensor_city` String,
    `sensor_cell` String,
    `type` String,
    `temperature` Decimal(4, 2),
    `season` String,
    `latitude` Float64,
    `longitude` Float64,
    `timestamp` DateTime
)
ENGINE = MergeTree
ORDER BY timestamp;


CREATE TABLE IF NOT EXISTS humidity_table
(
    `sensor_id` String,
    `sensor_city` String,
    `sensor_cell` String,
    `type` String,
    `humidity` Decimal(5, 2),
    `latitude` Float64,
    `longitude` Float64,
    `timestamp` DateTime   
)
ENGINE = MergeTree
ORDER BY timestamp;


CREATE TABLE IF NOT EXISTS charging_station_table
(
    `sensor_id` String,
    `sensor_city` String,
    `sensor_cell` String,
    `type` String,
    `state` Bool,
    `latitude` Float64,
    `longitude` Float64,
    `timestamp` DateTime   
)
ENGINE = MergeTree
ORDER BY timestamp;

