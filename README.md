# Assignment 1 – Data Warehousing (Taxi TLC – Abbas Syed)

## 1. Data Sourcing (2 pts)

### Data Source

- **TLC Yellow Taxi Trips (Jan 2023, Parquet)**
  - URL: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

### Data Dictionary

- Google Sheets data dictionary for this dataset:
  - https://docs.google.com/spreadsheets/d/11K22GrLOxfj3pEcFKnM_dzLQC3pekfMgwXTJjKfXmQo/edit?usp=sharing

### Scripts that gather the data

- `scripts/download_yellow_taxi_to_s3.py`  
  - Downloads the January 2023 TLC Yellow Taxi Parquet file from the TLC CloudFront URL.
  - Uploads the file into my AWS S3 bucket in the **raw** folder:  
    `s3://taxi-tlc-bucket-abbas/raw/yellow_tripdata_2023-01.parquet`

### Git Repository

This repository (`dw_assignment1_sql`) is used for Assignment 1.  
All scripts and documentation are version-controlled with Git and pushed to GitHub.

## 2. Storage (3 pts)

**Storage of choice**

- **AWS S3 bucket:** `taxi-tlc-bucket-abbas`
  - `raw/` layer: `s3://taxi-tlc-bucket-abbas/raw/yellow_tripdata_2023-01.parquet`
  - `clean/` and `warehouse/` folders reserved for later ETL steps.

- **AWS RDS PostgreSQL instance:** `taxi` (us-east-2)
  - Staging table: `public.raw_yellow_tripdata`

**Scripts updated to store data**

- `scripts/download_yellow_taxi_to_s3.py`
  - Downloads January 2023 TLC Yellow Taxi Parquet file from TLC CloudFront.
  - Uploads it to S3 under the `raw/` prefix.

- `scripts/load_taxi_to_postgres.py`
  - Reads the same TLC Parquet file.
  - Creates the staging table `public.raw_yellow_tripdata` if needed.
  - Bulk loads ~3.06M rows into the staging table using COPY from CSV.

All scripts are version-controlled in this GitHub repository.

---

## 3. Modeling (5 pts)

**Data warehouse model**

I designed a star schema in schema `taxi_dw` with:

- **Fact table:**
  - `taxi_dw.fact_taxi_trips`
  - Surrogate primary key: `trip_key (BIGSERIAL)`
  - Measures: `passenger_count`, `trip_distance`, `fare_amount`, `tip_amount`,
    `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`

- **Dimension tables (each with surrogate key):**
  - `taxi_dw.dim_vendor(vendor_key, vendorid, vendor_name)`
  - `taxi_dw.dim_rate_code(ratecode_key, ratecodeid, ratecode_desc)`
  - `taxi_dw.dim_payment_type(payment_type_key, payment_type, payment_desc)`
  - `taxi_dw.dim_datetime(datetime_key, pickup_datetime, pickup_date, pickup_hour, pickup_dow)`

`fact_taxi_trips` contains foreign keys to all four dimensions.

**Scripts that create the Data Warehouse**

- `sql/create_taxi_dw.sql`
  - Creates the schema `taxi_dw`.
  - Creates all four dimension tables and the fact table with surrogate keys.
  - Populates the dimensions from `public.raw_yellow_tripdata`.
  - Populates the fact table by joining `public.raw_yellow_tripdata` to the
    dimension tables.

**Scripts from previous steps**

- `scripts/load_taxi_to_postgres.py` serves as the ETL from S3/TLC into the
  staging table used by the warehouse creation script.

