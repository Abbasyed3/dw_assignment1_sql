-- create_taxi_dw.sql
-- Data warehouse star schema for TLC Yellow Taxi (Jan 2023)

CREATE SCHEMA IF NOT EXISTS taxi_dw;

-- Vendor dimension
CREATE TABLE IF NOT EXISTS taxi_dw.dim_vendor (
    vendor_key      SERIAL PRIMARY KEY,
    vendorid        INTEGER NOT NULL,
    vendor_name     TEXT,
    UNIQUE (vendorid)
);

-- Rate code dimension
CREATE TABLE IF NOT EXISTS taxi_dw.dim_rate_code (
    ratecode_key    SERIAL PRIMARY KEY,
    ratecodeid      INTEGER NOT NULL,
    ratecode_desc   TEXT,
    UNIQUE (ratecodeid)
);

-- Payment type dimension
CREATE TABLE IF NOT EXISTS taxi_dw.dim_payment_type (
    payment_type_key    SERIAL PRIMARY KEY,
    payment_type        INTEGER NOT NULL,
    payment_desc        TEXT,
    UNIQUE (payment_type)
);

-- Datetime dimension
CREATE TABLE IF NOT EXISTS taxi_dw.dim_datetime (
    datetime_key    SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    pickup_date     DATE,
    pickup_hour     INTEGER,
    pickup_dow      INTEGER,
    UNIQUE (pickup_datetime)
);

-- Fact table (REMOVED airport_fee)
CREATE TABLE IF NOT EXISTS taxi_dw.fact_taxi_trips (
    trip_key                BIGSERIAL PRIMARY KEY,
    vendor_key              INTEGER NOT NULL REFERENCES taxi_dw.dim_vendor(vendor_key),
    ratecode_key            INTEGER NOT NULL REFERENCES taxi_dw.dim_rate_code(ratecode_key),
    payment_type_key        INTEGER NOT NULL REFERENCES taxi_dw.dim_payment_type(payment_type_key),
    pickup_datetime_key     INTEGER NOT NULL REFERENCES taxi_dw.dim_datetime(datetime_key),

    passenger_count         INTEGER,
    trip_distance           NUMERIC(10,2),
    fare_amount             NUMERIC(10,2),
    extra                   NUMERIC(10,2),
    mta_tax                 NUMERIC(10,2),
    tip_amount              NUMERIC(10,2),
    tolls_amount            NUMERIC(10,2),
    improvement_surcharge   NUMERIC(10,2),
    total_amount            NUMERIC(10,2),
    congestion_surcharge    NUMERIC(10,2)
);

-- Load Vendor
INSERT INTO taxi_dw.dim_vendor (vendorid)
SELECT DISTINCT vendorid
FROM public.raw_yellow_tripdata
WHERE vendorid IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE taxi_dw.dim_vendor
SET vendor_name = CASE vendorid
    WHEN 1 THEN 'Creative Mobile Technologies (CMT)'
    WHEN 2 THEN 'VeriFone (VTS)'
    ELSE vendor_name
END
WHERE vendor_name IS NULL;

-- Load Rate
INSERT INTO taxi_dw.dim_rate_code (ratecodeid)
SELECT DISTINCT ratecodeid
FROM public.raw_yellow_tripdata
WHERE ratecodeid IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE taxi_dw.dim_rate_code
SET ratecode_desc = CASE ratecodeid
    WHEN 1 THEN 'Standard rate'
    WHEN 2 THEN 'JFK'
    WHEN 3 THEN 'Newark'
    WHEN 4 THEN 'Nassau/Westchester'
    WHEN 5 THEN 'Negotiated fare'
    WHEN 6 THEN 'Group ride'
    ELSE 'Unknown'
END
WHERE ratecode_desc IS NULL;

-- Load Payment type
INSERT INTO taxi_dw.dim_payment_type (payment_type)
SELECT DISTINCT payment_type
FROM public.raw_yellow_tripdata
WHERE payment_type IS NOT NULL
ON CONFLICT DO NOTHING;

UPDATE taxi_dw.dim_payment_type
SET payment_desc = CASE payment_type
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
END
WHERE payment_desc IS NULL;

-- Load Datetime
INSERT INTO taxi_dw.dim_datetime (
    pickup_datetime,
    pickup_date,
    pickup_hour,
    pickup_dow
)
SELECT DISTINCT
    tpep_pickup_datetime,
    DATE(tpep_pickup_datetime),
    EXTRACT(HOUR FROM tpep_pickup_datetime)::INT,
    EXTRACT(DOW FROM tpep_pickup_datetime)::INT
FROM public.raw_yellow_tripdata
WHERE tpep_pickup_datetime IS NOT NULL
ON CONFLICT DO NOTHING;

-- Load Fact (REMOVED airport_fee)
INSERT INTO taxi_dw.fact_taxi_trips (
    vendor_key,
    ratecode_key,
    payment_type_key,
    pickup_datetime_key,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge
)
SELECT
    v.vendor_key,
    r.ratecode_key,
    p.payment_type_key,
    d.datetime_key,
    r_raw.passenger_count,
    r_raw.trip_distance,
    r_raw.fare_amount,
    r_raw.extra,
    r_raw.mta_tax,
    r_raw.tip_amount,
    r_raw.tolls_amount,
    r_raw.improvement_surcharge,
    r_raw.total_amount,
    r_raw.congestion_surcharge
FROM public.raw_yellow_tripdata r_raw
JOIN taxi_dw.dim_vendor        v ON v.vendorid = r_raw.vendorid
JOIN taxi_dw.dim_rate_code    r ON r.ratecodeid = r_raw.ratecodeid
JOIN taxi_dw.dim_payment_type p ON p.payment_type = r_raw.payment_type
JOIN taxi_dw.dim_datetime     d ON d.pickup_datetime = r_raw.tpep_pickup_datetime;

