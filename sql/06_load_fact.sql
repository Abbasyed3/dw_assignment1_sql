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
    trip_duration_minutes,
    total_surcharges,
    total_amount
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
    c.trip_duration_minutes,
    (c.extra
     + c.mta_tax
     + c.tolls_amount
     + c.improvement_surcharge
     + c.congestion_surcharge
     + c.airport_fee) AS total_surcharges,
    c.total_amount
FROM taxi_dw.taxi_trips_clean c
JOIN taxi_dw.dim_datetime     d ON d.datetime_key =
       (c.trip_year*1000000 + c.trip_month*10000 + c.trip_day*100 + c.trip_hour)
JOIN taxi_dw.dim_vendor       v ON v.vendorid     = c.vendorid
JOIN taxi_dw.dim_rate_code    r ON r.ratecodeid   = CAST(c.ratecodeid AS INT)
JOIN taxi_dw.dim_payment_type p ON p.payment_type = c.payment_type;
