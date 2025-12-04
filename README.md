# NYC TLC Yellow Taxi – Cloud Data Warehouse (Assignments 1 & 2)

Author: Abbas Syed  
Course: CIS 9440 – Data Warehousing for Analytics

## 1. Project Overview

This project builds a small end-to-end data warehouse for the NYC TLC Yellow Taxi trips (January 2023).  
Assignment 1 focused on sourcing the data and loading it into cloud storage.  
Assignment 2 extends the project by:

- Transforming the raw TLC file into a clean analytical schema in Amazon Redshift.
- Modeling a star schema with one fact table and several dimensions.
- Serving the data through Tableau (online dashboard) and a Python script that exports a CSV summary for analysts.

## 2. Cloud Stack

- **Storage:** Amazon S3  
  - Bucket: `taxi-tlc-bucket-abbas`  
  - Objects: `raw/yellow_tripdata_2023-01.parquet` and `raw/yellow_tripdata_2023-01.csv`

- **Data Warehouse:** Amazon Redshift Serverless  
  - Workgroup: `default-workgroup`  
  - Database: `dev`  
  - Schema: `taxi_dw`  
  - IAM Role: `RedshiftS3AccessRole` (S3 read + Redshift commands)

- **Serving:**
  - Tableau (live connection to Redshift) – interactive dashboard on Tableau Public.
  - Python script that exports a daily taxi summary to CSV.

## 3. Repository Structure

```text
dw_assignment1_sql/
├── raw_data/
│   ├── yellow_tripdata_2023-01.parquet
│   └── yellow_tripdata_2023-01.csv
├── scripts/
│   ├── download_yellow_taxi_to_s3.py
│   ├── convert_parquet_to_csv.py
│   └── load_taxi_to_postgres.py        # Assignment 1 (local loading)
├── sql/
│   ├── 01_create_schema_and_staging.sql
│   ├── 02_create_and_load_clean_table.sql
│   ├── 03_create_dimensions.sql
│   ├── 04_load_dimensions.sql
│   ├── 05_create_fact.sql
│   ├── 06_load_fact.sql
│   ├── 07_sanity_checks.sql
│   └── assignment2_redshift_full_build.sql   # One-shot full rebuild
├── api/
│   └── export_taxi_summary.py          # Python "API" → CSV export
├── output/
│   └── taxi_daily_summary_2023_01.csv  # Generated summary file
├── docs/
│   ├── ASSIGNMENT 1 – DATA WAREHOUSING REPORT.pdf
│   ├── Assignment 1 – Taxi TLC Data Warehouse (Abbas Syed).pdf
│   ├── Assignment 2 Data Dictionary_ Data Mapping.pdf
│   └── Assignment 2 ERD.pdf
├── Assignment 2 – Taxi TLC Data Warehouse (Abbas Syed).twbx   # Tableau workbook
└── README.md

