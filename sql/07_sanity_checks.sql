-- Compare counts between clean table and fact
SELECT COUNT(*) AS clean_rows
FROM taxi_dw.taxi_trips_clean;

SELECT COUNT(*) AS fact_rows
FROM taxi_dw.fact_taxi_trips;

-- Null foreign keys (should all be 0)
SELECT
    SUM(CASE WHEN datetime_key     IS NULL THEN 1 ELSE 0 END) AS null_datetime_key,
    SUM(CASE WHEN vendor_key       IS NULL THEN 1 ELSE 0 END) AS null_vendor_key,
    SUM(CASE WHEN ratecode_key     IS NULL THEN 1 ELSE 0 END) AS null_ratecode_key,
    SUM(CASE WHEN payment_type_key IS NULL THEN 1 ELSE 0 END) AS null_payment_type_key
FROM taxi_dw.fact_taxi_trips;

-- Basic stats on amounts
SELECT
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount,
    AVG(total_amount) AS avg_total_amount
FROM taxi_dw.fact_taxi_trips;
