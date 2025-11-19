#!/usr/bin/env python

"""
load_taxi_to_postgres.py

Loads the January 2023 NYC Yellow Taxi Parquet file into the Postgres
database on AWS RDS.

Steps:
 1. Download the TLC Parquet file from the public TLC URL into a pandas DataFrame.
 2. Create a raw table in Postgres if it does not exist.
 3. Bulk load all rows using COPY FROM STDIN (CSV).

Database connection details are read from environment variables:

  TAXI_DB_HOST      - RDS endpoint (e.g. taxi.cv4kekeuq5n7.us-east-2.rds.amazonaws.com)
  TAXI_DB_USER      - database user (e.g. postgres)
  TAXI_DB_PASSWORD  - database password
  TAXI_DB_NAME      - database name (e.g. postgres)

Run from the project root:

  python scripts/load_taxi_to_postgres.py
"""

import os
import io
import sys
import pandas as pd
import psycopg2


# -----------------------------
# Configuration
# -----------------------------

# TLC public Parquet URL (Jan 2023 Yellow Taxi)
TLC_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)


RAW_TABLE_NAME = "raw_yellow_tripdata"


# -----------------------------
# Helpers
# -----------------------------

def get_db_connection():
    """Create a connection to the Postgres database using env variables."""
    host = os.environ.get("TAXI_DB_HOST")
    user = os.environ.get("TAXI_DB_USER", "postgres")
    password = os.environ.get("TAXI_DB_PASSWORD")
    dbname = os.environ.get("TAXI_DB_NAME", "postgres")

    if not host:
        raise RuntimeError("TAXI_DB_HOST is not set")
    if not password:
        raise RuntimeError("TAXI_DB_PASSWORD is not set")

    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=dbname,
        port=5432,
        connect_timeout=15,
    )
    conn.autocommit = False
    return conn


def create_raw_table(cur):
    """Create the raw table if it does not already exist.

    NOTE: passenger_count is stored as FLOAT to match the Parquet file (values like 1.0).
    """

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
        vendorid                INTEGER,
        tpep_pickup_datetime    TIMESTAMP,
        tpep_dropoff_datetime   TIMESTAMP,
        passenger_count         FLOAT,
        trip_distance           FLOAT,
        ratecodeid              FLOAT,
        store_and_fwd_flag      TEXT,
        pulocationid            INTEGER,
        dolocationid            INTEGER,
        payment_type            INTEGER,
        fare_amount             FLOAT,
        extra                   FLOAT,
        mta_tax                 FLOAT,
        tip_amount              FLOAT,
        tolls_amount            FLOAT,
        improvement_surcharge   FLOAT,
        total_amount            FLOAT,
        congestion_surcharge    FLOAT
    );
    """
    cur.execute(create_sql)


def download_taxi_data():
    """Download the TLC Parquet file into a pandas DataFrame."""
    print("Downloading TLC yellow taxi data (Jan 2023)...")
    df = pd.read_parquet(TLC_PARQUET_URL)
    print(f"Loaded {len(df):,} rows from TLC Parquet file.")

    # Keep only the columns we care about and in the right order
    expected_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns in Parquet file: {missing}")

    df = df[expected_cols].copy()

    # Light cleaning: make sure numeric columns are numeric
    numeric_cols = [
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Optional: replace NaNs in passenger_count with 0.0
    df["passenger_count"] = df["passenger_count"].fillna(0.0)

    return df


def copy_dataframe_to_postgres(df, cur):
    """Bulk copy the DataFrame into Postgres using COPY FROM STDIN."""

    # Column names in Postgres must be lowercase to match the table definition
    df_copy = df.rename(
        columns={
            "VendorID": "vendorid",
            "RatecodeID": "ratecodeid",
            "PULocationID": "pulocationid",
            "DOLocationID": "dolocationid",
        }
    )

    # Make sure order matches the table definition exactly
    ordered_cols = [
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]
    df_copy = df_copy[ordered_cols]

    print("Starting bulk COPY into Postgres...")

    buffer = io.StringIO()
    # No header row, comma-separated
    df_copy.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_sql = f"""
        COPY {RAW_TABLE_NAME} (
            vendorid,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            ratecodeid,
            store_and_fwd_flag,
            pulocationid,
            dolocationid,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge
        )
        FROM STDIN WITH (FORMAT CSV);
    """

    cur.copy_expert(copy_sql, buffer)
    print("COPY completed.")


def main():
    try:
        df = download_taxi_data()
    except Exception as e:
        print(" Failed to download or parse TLC Parquet file:", e)
        sys.exit(1)

    try:
        conn = get_db_connection()
    except Exception as e:
        print(" Failed to connect to Postgres:", e)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            print("Creating raw table (if not exists)...")
            create_raw_table(cur)

            print("Loading data into Postgres...")
            copy_dataframe_to_postgres(df, cur)

        conn.commit()
        print(" All done. Data committed to Postgres.")
    except Exception as e:
        conn.rollback()
        print(" Error while loading data. Transaction rolled back.")
        print(e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

