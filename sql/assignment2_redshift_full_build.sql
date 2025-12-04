-- =========================================================
-- ASSIGNMENT 2 - NYC TLC Yellow Taxi DW in Redshift
-- Author: Abbas Syed
-- This script:
--   1) Creates schema + staging table
--   2) Loads CSV from S3 into staging
--   3) Creates cleaned trip table
--   4) Creates dimensions with surrogate keys
--   5) Creates fact table and loads it
--   6) Creates star and mapping views
--   7) Provides sanity-check queries
-- =========================================================

-- 0. SCHEMA ------------------------------------------------
CREATE SCHEMA IF NOT EXISTS taxi_dw;

-- =========================================================
-- 1. STAGING TABLE (1:1 copy of CSV)
-- =========================================================

DROP TABLE IF EXISTS taxi_dw.stg_yellow_tripdata;

CREATE TABLE taxi_dw.stg_yellow_tripdata (
    vendorid                INT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         INT,
    trip_distance           DECIMAL(10,3),
    RatecodeID              INT,
    store_and_fwd_flag      VARCHAR(1),
    PULocationID            INT,
    DOLocationID            INT,
    payment_type            INT,
    fare_amount             DECIMAL(10,2),
    extra                   DECIMAL(10,2),
    mta_tax                 DECIMAL(10,2),
    tip_amount              DECIMAL(10,2),
    tolls_amount            DECIMAL(10,2),
    improvement_surcharge   DECIMAL(10,2),
    total_amount            DECIMAL(10,2),
    congestion_surcharge    DECIMAL(10,2),
    airport_fee             DECIMAL(10,2)
);

-- NOTE: run this COPY once you edit the IAM role ARN
-- (You can also keep using the console version – this is for documentation)
-- COPY taxi_dw.stg_yellow_tripdata
-- FROM 's3://taxi-tlc-bucket-abbas/raw/yellow_tripdata_2023-01.csv'
-- IAM_ROLE 'arn:aws:iam::XXXXXXXXXXXX:role/YourRedshiftRole'
-- FORMAT AS CSV
-- IGNOREHEADER 1
-- DELIMITER ','
-- EMPTYASNULL
-- BLANKSASNULL;

-- =========================================================
-- 2. CLEANED TRIP TABLE
--    - unified date
--    - split into Year, Month, Day, Hour, DOW
--    - derived trip_duration_minutes
--    - basic quality filters
-- =========================================================

DROP TABLE IF EXISTS taxi_dw.taxi_trips_clean;

CREATE TABLE taxi_dw.taxi_trips_clean AS
SELECT DISTINCT
    -- original keys
    vendorid,
    RatecodeID           AS ratecodeid,
    payment_type,
    PULocationID         AS pulocationid,
    DOLocationID         AS dolocationid,

    -- original timestamps
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    -- unified date + parts
    CAST(tpep_pickup_datetime AS DATE)                  AS trip_date,
    EXTRACT(YEAR   FROM tpep_pickup_datetime)           AS trip_year,
    EXTRACT(QUARTER FROM tpep_pickup_datetime)          AS trip_quarter,
    EXTRACT(MONTH  FROM tpep_pickup_datetime)           AS trip_month,
    EXTRACT(DAY    FROM tpep_pickup_datetime)           AS trip_day,
    EXTRACT(HOUR   FROM tpep_pickup_datetime)           AS trip_hour,
    EXTRACT(DOW    FROM tpep_pickup_datetime)           AS trip_dow,

    -- measures
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,

    -- derived duration in minutes
    DATEDIFF(
        minute,
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    )                                                   AS trip_duration_minutes,

    -- optional derived surcharge total
    (COALESCE(extra,0)
     + COALESCE(mta_tax,0)
     + COALESCE(improvement_surcharge,0)
     + COALESCE(congestion_surcharge,0)
     + COALESCE(airport_fee,0))                         AS total_surcharges
FROM taxi_dw.stg_yellow_tripdata
WHERE
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND trip_distance >= 0
    AND total_amount   >= 0;

-- =========================================================
-- 3. DIMENSIONS
--    All with surrogate keys
-- =========================================================

-- 3.1 Vendor dimension
DROP TABLE IF EXISTS taxi_dw.dim_vendor;

CREATE TABLE taxi_dw.dim_vendor (
    vendor_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    vendorid     INT NOT NULL,
    vendor_name  VARCHAR(100)
);

TRUNCATE TABLE taxi_dw.dim_vendor;

INSERT INTO taxi_dw.dim_vendor (vendorid, vendor_name)
SELECT DISTINCT
    vendorid,
    CASE vendorid
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'Verifone'
        ELSE 'Other / Unknown'
    END AS vendor_name
FROM taxi_dw.taxi_trips_clean
WHERE vendorid IS NOT NULL;

-- 3.2 Rate code dimension
DROP TABLE IF EXISTS taxi_dw.dim_rate_code;

CREATE TABLE taxi_dw.dim_rate_code (
    ratecode_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    ratecodeid   INT NOT NULL,
    ratecode_desc VARCHAR(100)
);

TRUNCATE TABLE taxi_dw.dim_rate_code;

INSERT INTO taxi_dw.dim_rate_code (ratecodeid, ratecode_desc)
SELECT DISTINCT
    ratecodeid,
    CASE ratecodeid
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Other / Unknown'
    END AS ratecode_desc
FROM taxi_dw.taxi_trips_clean
WHERE ratecodeid IS NOT NULL;

-- 3.3 Payment type dimension
DROP TABLE IF EXISTS taxi_dw.dim_payment_type;

CREATE TABLE taxi_dw.dim_payment_type (
    payment_type_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    payment_type     INT NOT NULL,
    payment_desc     VARCHAR(100)
);

TRUNCATE TABLE taxi_dw.dim_payment_type;

INSERT INTO taxi_dw.dim_payment_type (payment_type, payment_desc)
SELECT DISTINCT
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END AS payment_desc
FROM taxi_dw.taxi_trips_clean
WHERE payment_type IS NOT NULL;

-- 3.4 Datetime dimension (pickup)
DROP TABLE IF EXISTS taxi_dw.dim_datetime;

CREATE TABLE taxi_dw.dim_datetime (
    datetime_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    pickup_date  DATE NOT NULL,
    pickup_year  INT,
    pickup_quarter INT,
    pickup_month INT,
    pickup_day   INT,
    pickup_hour  INT,
    pickup_dow   INT
);

TRUNCATE TABLE taxi_dw.dim_datetime;

INSERT INTO taxi_dw.dim_datetime (
    pickup_date,
    pickup_year,
    pickup_quarter,
    pickup_month,
    pickup_day,
    pickup_hour,
    pickup_dow
)
SELECT DISTINCT
    trip_date,
    trip_year,
    trip_quarter,
    trip_month,
    trip_day,
    trip_hour,
    trip_dow
FROM taxi_dw.taxi_trips_clean;

-- =========================================================
-- 4. FACT TABLE
-- =========================================================

DROP TABLE IF EXISTS taxi_dw.fact_taxi_trips;

CREATE TABLE taxi_dw.fact_taxi_trips (
    trip_key              BIGINT IDENTITY(1,1) PRIMARY KEY,
    datetime_key          BIGINT NOT NULL,
    vendor_key            BIGINT NOT NULL,
    ratecode_key          BIGINT NOT NULL,
    payment_type_key      BIGINT NOT NULL,

    passenger_count       INT,
    trip_distance         DECIMAL(10,3),
    fare_amount           DECIMAL(10,2),
    extra                 DECIMAL(10,2),
    mta_tax               DECIMAL(10,2),
    tip_amount            DECIMAL(10,2),
    tolls_amount          DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge  DECIMAL(10,2),
    airport_fee           DECIMAL(10,2),
    total_surcharges      DECIMAL(10,2),
    total_amount          DECIMAL(10,2),
    trip_duration_minutes INT,

    pulocationid          INT,
    dolocationid          INT
)
-- Redshift treats FKs as informational, but add for documentation
;

TRUNCATE TABLE taxi_dw.fact_taxi_trips;

INSERT INTO taxi_dw.fact_taxi_trips (
    datetime_key,
    vendor_key,
    ratecode_key,
    payment_type_key,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_surcharges,
    total_amount,
    trip_duration_minutes,
    pulocationid,
    dolocationid
)
SELECT
    d.datetime_key,
    v.vendor_key,
    r.ratecode_key,
    p.payment_type_key,
    c.passenger_count,
    c.trip_distance,
    c.fare_amount,
    c.extra,
    c.mta_tax,
    c.tip_amount,
    c.tolls_amount,
    c.improvement_surcharge,
    c.congestion_surcharge,
    c.airport_fee,
    c.total_surcharges,
    c.total_amount,
    c.trip_duration_minutes,
    c.pulocationid,
    c.dolocationid
FROM taxi_dw.taxi_trips_clean c
JOIN taxi_dw.dim_vendor        v ON c.vendorid     = v.vendorid
JOIN taxi_dw.dim_rate_code     r ON c.ratecodeid   = r.ratecodeid
JOIN taxi_dw.dim_payment_type  p ON c.payment_type = p.payment_type
JOIN taxi_dw.dim_datetime      d ON c.trip_date    = d.pickup_date
                                 AND c.trip_year   = d.pickup_year
                                 AND c.trip_month  = d.pickup_month
                                 AND c.trip_day    = d.pickup_day
                                 AND c.trip_hour   = d.pickup_hour;

-- =========================================================
-- 5. VIEWS FOR SERVING LAYER
-- =========================================================

-- 5.1 Star view for Tableau (fact + dims)
DROP VIEW IF EXISTS taxi_dw.v_taxi_trips_star;

CREATE VIEW taxi_dw.v_taxi_trips_star AS
SELECT
    f.trip_key,

    -- datetime grain
    d.pickup_date,
    d.pickup_year,
    d.pickup_quarter,
    d.pickup_month,
    d.pickup_day,
    d.pickup_hour,
    d.pickup_dow,

    -- dimensions
    v.vendorid,
    v.vendor_name,
    r.ratecodeid,
    r.ratecode_desc,
    p.payment_type,
    p.payment_desc,

    -- measures
    f.passenger_count,
    f.trip_distance,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.congestion_surcharge,
    f.airport_fee,
    f.total_surcharges,
    f.total_amount,
    f.trip_duration_minutes,

    -- locations (for optional use)
    f.pulocationid,
    f.dolocationid
FROM taxi_dw.fact_taxi_trips   f
JOIN taxi_dw.dim_datetime      d ON f.datetime_key     = d.datetime_key
JOIN taxi_dw.dim_vendor        v ON f.vendor_key       = v.vendor_key
JOIN taxi_dw.dim_rate_code     r ON f.ratecode_key     = r.ratecode_key
JOIN taxi_dw.dim_payment_type  p ON f.payment_type_key = p.payment_type_key;

-- 5.2 Simple mapping view for geo map (PULocationID)
DROP VIEW IF EXISTS taxi_dw.v_taxi_trips_for_map;

CREATE VIEW taxi_dw.v_taxi_trips_for_map AS
SELECT
    CAST(c.pulocationid AS INT)        AS locationid,
    c.tpep_pickup_datetime             AS pickup_datetime,
    c.tpep_dropoff_datetime            AS dropoff_datetime,
    c.passenger_count,
    c.trip_distance,
    c.total_amount
FROM taxi_dw.taxi_trips_clean c;

-- =========================================================
-- 6. SANITY CHECKS
-- =========================================================

-- Row counts
SELECT COUNT(*) AS stg_rows   FROM taxi_dw.stg_yellow_tripdata;
SELECT COUNT(*) AS clean_rows FROM taxi_dw.taxi_trips_clean;
SELECT COUNT(*) AS fact_rows  FROM taxi_dw.fact_taxi_trips;

-- Null foreign-key diagnostics (should all be 0)
SELECT
    SUM(CASE WHEN datetime_key    IS NULL THEN 1 ELSE 0 END) AS null_datetime_key,
    SUM(CASE WHEN vendor_key      IS NULL THEN 1 ELSE 0 END) AS null_vendor_key,
    SUM(CASE WHEN ratecode_key    IS NULL THEN 1 ELSE 0 END) AS null_ratecode_key,
    SUM(CASE WHEN payment_type_key IS NULL THEN 1 ELSE 0 END) AS null_payment_type_key
FROM taxi_dw.fact_taxi_trips;

-- Basic measure sanity
SELECT
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount,
    AVG(total_amount) AS avg_total_amount
FROM taxi_dw.fact_taxi_trips;

