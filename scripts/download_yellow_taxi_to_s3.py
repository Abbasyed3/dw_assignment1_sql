import os
import pathlib
import boto3
import requests

# -------------------------------------------------
# Configuration
# -------------------------------------------------
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

BUCKET_NAME = "taxi-tlc-bucket-abbas"
S3_KEY = "raw/yellow_tripdata_2023-01.parquet"

# Local temp file (not part of deliverables – just used when script runs)
LOCAL_FILE = pathlib.Path("yellow_tripdata_2023-01.parquet")


def download_from_tlc():
    """Download the January 2023 Yellow Taxi data from TLC."""
    print(f"Downloading file from {TLC_URL} ...")
    resp = requests.get(TLC_URL, stream=True)
    resp.raise_for_status()

    with open(LOCAL_FILE, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"Saved file to {LOCAL_FILE.resolve()}")


def upload_to_s3():
    """Upload the downloaded file to the S3 bucket under raw/."""
    print(f"Uploading {LOCAL_FILE} to s3://{BUCKET_NAME}/{S3_KEY} ...")
    s3 = boto3.client("s3")
    s3.upload_file(str(LOCAL_FILE), BUCKET_NAME, S3_KEY)
    print("Upload completed.")


def main():
    download_from_tlc()
    upload_to_s3()
    print("Script 1 finished: data sourced and stored in S3 (raw layer).")


if __name__ == "__main__":
    main()

