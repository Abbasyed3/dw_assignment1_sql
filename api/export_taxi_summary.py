"""
export_taxi_summary.py

Simple Python "API" that connects to the Redshift data warehouse,
runs a summary query on taxi_dw.v_taxi_trips_star, and writes
the results to a CSV file that a data analyst can use.

Author: Abbas Syed
Assignment 2 - NYC Yellow Taxi DW in Redshift
"""

import os
import csv
from datetime import date
import psycopg2


# ----------------------------
# CONFIGURATION
# ----------------------------

# You can either hard-code these or set them as environment variables.
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "default-workgroup.411189321573.us-east-2.redshift-serverless.amazonaws.com")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DB   = os.environ.get("REDSHIFT_DB", "dev")          # change if your DB name is different
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "admin")
REDSHIFT_PWD  = os.environ.get("REDSHIFT_PWD", "Ngnw2025!")

OUTPUT_DIR    = os.environ.get("OUTPUT_DIR", "output")
OUTPUT_CSV    = os.path.join(OUTPUT_DIR, "taxi_daily_summary_2023_01.csv")


# ----------------------------
# SQL QUERY
# ----------------------------

SUMMARY_SQL = """
SELECT
    pickup_date,
    COUNT(*)                AS total_trips,
    SUM(passenger_count)    AS total_passengers,
    SUM(trip_distance)      AS total_trip_distance,
    SUM(total_amount)       AS total_revenue
FROM taxi_dw.v_taxi_trips_star
WHERE pickup_date BETWEEN %s AND %s
GROUP BY pickup_date
ORDER BY pickup_date;
"""


# ----------------------------
# CORE FUNCTION (API)
# ----------------------------

def get_taxi_daily_summary(start_date: date, end_date: date):
    """
    Connects to Redshift, runs the summary query, and returns rows as a list
    of dictionaries.
    """

    conn = None
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PWD,
        )
        with conn.cursor() as cur:
            cur.execute(SUMMARY_SQL, (start_date, end_date))
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        # Convert to list of dicts for easier CSV writing / reuse
        results = [
            dict(zip(colnames, row))
            for row in rows
        ]
        return results

    finally:
        if conn is not None:
            conn.close()


def write_summary_to_csv(start_date: date, end_date: date, output_path: str = OUTPUT_CSV):
    """
    High-level API function:
      1) Calls get_taxi_daily_summary
      2) Writes the results to a CSV file
    """

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    data = get_taxi_daily_summary(start_date, end_date)

    if not data:
        print("No data returned for the given date range.")
        return

    fieldnames = list(data[0].keys())  # ['pickup_date', 'total_trips', 'total_passengers', ...]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f" Wrote {len(data)} rows to CSV: {output_path}")


# ----------------------------
# COMMAND-LINE ENTRY POINT
# ----------------------------

if __name__ == "__main__":
    # For Assignment 2, we export the full January 2023 period
    start = date(2023, 1, 1)
    end   = date(2023, 1, 31)

    write_summary_to_csv(start, end)

