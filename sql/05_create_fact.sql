DROP TABLE IF EXISTS taxi_dw.fact_taxi_trips;

CREATE TABLE taxi_dw.fact_taxi_trips (
    trip_key BIGINT IDENTITY(1,1) PRIMARY KEY,

    datetime_key     BIGINT,
    vendor_key       BIGINT,
    ratecode_key     BIGINT,
    payment_type_key BIGINT,

    passenger_count        DOUBLE PRECISION,
    trip_distance          DOUBLE PRECISION,
    fare_amount            DOUBLE PRECISION,
    extra                  DOUBLE PRECISION,
    mta_tax                DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    tolls_amount           DOUBLE PRECISION,
    improvement_surcharge  DOUBLE PRECISION,
    congestion_surcharge   DOUBLE PRECISION,
    airport_fee            DOUBLE PRECISION,
    trip_duration_minutes  BIGINT,
    total_surcharges       DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION
);
