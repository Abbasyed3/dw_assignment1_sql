-- Cleaned / transformed trip table
DROP TABLE IF EXISTS taxi_dw.taxi_trips_clean;

CREATE TABLE taxi_dw.taxi_trips_clean AS
SELECT
    vendorid,
    ratecodeid,
    payment_type,
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
    DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

    -- unified date + split into units
    DATE(tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR    FROM tpep_pickup_datetime) AS trip_year,
    EXTRACT(QUARTER FROM tpep_pickup_datetime) AS trip_quarter,
    EXTRACT(MONTH   FROM tpep_pickup_datetime) AS trip_month,
    EXTRACT(DAY     FROM tpep_pickup_datetime) AS trip_day,
    EXTRACT(HOUR    FROM tpep_pickup_datetime) AS trip_hour,
    EXTRACT(DOW     FROM tpep_pickup_datetime) AS trip_dow
FROM taxi_dw.stg_yellow_tripdata
WHERE tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND total_amount          IS NOT NULL;

-- Optional: de-duplicate exact duplicate rows
DELETE FROM taxi_dw.taxi_trips_clean
USING (
    SELECT trip_date, vendorid, passenger_count, trip_distance, fare_amount,
           trip_year, trip_month, trip_day, trip_hour,
           COUNT(*) AS cnt
    FROM taxi_dw.taxi_trips_clean
    GROUP BY 1,2,3,4,5,6,7,8,9
    HAVING COUNT(*) > 1
) d
WHERE taxi_trips_clean.trip_date  = d.trip_date
  AND taxi_trips_clean.vendorid   = d.vendorid
  AND taxi_trips_clean.trip_year  = d.trip_year
  AND taxi_trips_clean.trip_month = d.trip_month
  AND taxi_trips_clean.trip_day   = d.trip_day
  AND taxi_trips_clean.trip_hour  = d.trip_hour;
