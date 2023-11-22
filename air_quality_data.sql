-- `default`.air_quality_data definition

CREATE TABLE default.air_quality_data
(

    `location` String,

    `city` String,

    `country` String,

    `latitude` String,

    `longtitude` String,

    `PARAMETER` String,

    `value` String,

    `lastUpdated` String,

    `unit` String
)
ENGINE = S3('https://storage.yandexcloud.net/data-for-analytics-dev-project/air_quality_data/*/*.csv',
 'CSV');