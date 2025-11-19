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

