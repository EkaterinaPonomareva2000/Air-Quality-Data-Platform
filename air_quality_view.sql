-- `default`.air_quality_view source

CREATE VIEW default.air_quality_view
(

    `location` String,

    `city` String,

    `country` String,

    `latitude` String,

    `longtitude` String,

    `PARAMETER` String,

    `value` Float64,

    `lastUpdated` String,

    `unit` String
) AS
SELECT
    location,

    city,

    country,

    latitude,

    longtitude,

    PARAMETER,

    CAST(value,
 'Float64') AS value,

    lastUpdated,

    unit
FROM default.air_quality_data AS aqd
WHERE unit != 'µg/m³';