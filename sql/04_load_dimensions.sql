-- 1) DATETIME DIMENSION
TRUNCATE TABLE taxi_dw.dim_datetime;

INSERT INTO taxi_dw.dim_datetime (
    datetime_key,
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    pickup_dow
)
SELECT DISTINCT
    (trip_year  * 1000000) +
    (trip_month * 10000)   +
    (trip_day   * 100)     +
    trip_hour                 AS datetime_key,
    trip_date                 AS pickup_date,
    trip_year,
    trip_month,
    trip_day,
    trip_hour,
    trip_dow
FROM taxi_dw.taxi_trips_clean;


-- 2) VENDOR DIMENSION
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


-- 3) RATE CODE DIMENSION
TRUNCATE TABLE taxi_dw.dim_rate_code;

INSERT INTO taxi_dw.dim_rate_code (ratecodeid, ratecode_desc)
SELECT DISTINCT
    CAST(ratecodeid AS INT) AS ratecodeid,
    CASE CAST(ratecodeid AS INT)
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


-- 4) PAYMENT TYPE DIMENSION
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
