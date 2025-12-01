-- Create DW schema
CREATE SCHEMA IF NOT EXISTS taxi_dw;

-- Drop and recreate staging table for raw CSV
DROP TABLE IF EXISTS taxi_dw.stg_yellow_tripdata;

CREATE TABLE taxi_dw.stg_yellow_tripdata (
    vendorid               DOUBLE PRECISION,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        DOUBLE PRECISION,
    trip_distance          DOUBLE PRECISION,
    ratecodeid             DOUBLE PRECISION,
    store_and_fwd_flag     VARCHAR(10),
    pulocationid           DOUBLE PRECISION,
    dolocationid           DOUBLE PRECISION,
    payment_type           DOUBLE PRECISION,
    fare_amount            DOUBLE PRECISION,
    extra                  DOUBLE PRECISION,
    mta_tax                DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    tolls_amount           DOUBLE PRECISION,
    improvement_surcharge  DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION,
    congestion_surcharge   DOUBLE PRECISION,
    airport_fee            DOUBLE PRECISION
);

-- Load from S3 into staging (this is the copy you already ran)
-- Adjust bucket / role if you changed them.
COPY taxi_dw.stg_yellow_tripdata
FROM 's3://taxi-tlc-bucket-abbas/raw/yellow_tripdata_2023-01.csv'
IAM_ROLE 'arn:aws:iam::411189231571:role/RedshiftSSAccessRole'
FORMAT AS CSV
IGNOREHEADER 1
DELIMITER ','
EMPTYASNULL
BLANKSASNULL;
