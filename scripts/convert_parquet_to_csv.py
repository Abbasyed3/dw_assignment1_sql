import pandas as pd
from pathlib import Path

RAW_DATA_DIR = Path("raw_data")
RAW_DATA_DIR.mkdir(exist_ok=True)

parquet_path = RAW_DATA_DIR / "yellow_tripdata_2023-01.parquet"
csv_path = RAW_DATA_DIR / "yellow_tripdata_2023-01.csv"

print("Reading parquet...")
df = pd.read_parquet(parquet_path)

print("Shape:", df.shape)
print("Writing CSV...")
df.to_csv(csv_path, index=False)

print("Done. CSV saved to:", csv_path)

