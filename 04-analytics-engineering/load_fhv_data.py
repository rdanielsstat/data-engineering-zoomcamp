from google.cloud import storage
import os
import urllib.request
import pandas as pd

# ================================
# CONFIG
# ================================
BUCKET_NAME = "sandbox-486719-taxi-data"
CREDENTIALS_FILE = "taxi_rides_ny/gcs.json"
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")

os.makedirs(RAW_DIR, exist_ok = True)
os.makedirs(PARQUET_DIR, exist_ok = True)

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

# ================================
# FHV FILES
# ================================
FHV_FILES = [
    f"fhv_tripdata_2019-{month:02d}.csv.gz" for month in range(1, 13)
]

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

# ================================
# COLUMN MAPPING & SCHEMA
# ================================
FHV_RENAME = {
    "dispatching_base_num": "dispatching_base_num",
    "pickup_datetime": "pickup_datetime",
    "dropOff_datetime": "dropoff_datetime",
    "PUlocationID": "pickup_location_id",
    "DOlocationID": "dropoff_location_id",
    "SR_Flag": "sr_flag",
    "Affiliated_base_number": "affiliated_base_number"
}

FHV_COLUMNS = [
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "sr_flag",
    "affiliated_base_number"
]

DATETIME_COLS = ["pickup_datetime", "dropoff_datetime"]

# ================================
# FUNCTIONS
# ================================
def download_file(file_name):
    url = f"{BASE_URL}/{file_name}"
    file_path = os.path.join(RAW_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Downloading {file_name}...")
        urllib.request.urlretrieve(url, file_path)
    return file_path

def transform_to_parquet(file_path):
    print(f"Processing {os.path.basename(file_path)}...")
    df = pd.read_csv(file_path, compression = "gzip", low_memory = False)

    # Rename columns
    df = df.rename(columns=FHV_RENAME)

    # Filter out records where dispatching_base_num is null
    df = df[df["dispatching_base_num"].notna()]

    # Parse datetimes
    for col in DATETIME_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors = "coerce")

    # Ensure all expected columns exist
    for col in FHV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Keep only desired columns in order
    df = df[FHV_COLUMNS]

    # Force string types before writing parquet
    df["dispatching_base_num"] = df["dispatching_base_num"].astype("string")
    df["pickup_location_id"] = df["pickup_location_id"].astype("string")
    df["dropoff_location_id"] = df["dropoff_location_id"].astype("string")
    df["sr_flag"] = df["sr_flag"].astype("string")
    df["affiliated_base_number"] = df["affiliated_base_number"].astype("string")

    # Save as Parquet
    parquet_name = os.path.basename(file_path).replace(".csv.gz", ".parquet")
    parquet_path = os.path.join(PARQUET_DIR, parquet_name)
    df.to_parquet(
        parquet_path,
        engine = "pyarrow",
        index = False,
        coerce_timestamps = "us",
        allow_truncated_timestamps = True
    )
    return parquet_path

def upload_to_gcs(parquet_path):
    blob_name = f"parquet/{os.path.basename(parquet_path)}"
    blob = bucket.blob(blob_name)

    # Delete existing parquet so upload overwrites
    if blob.exists():
        print(f"Deleting existing file: {blob_name}")
        blob.delete()

    print(f"Uploading {blob_name}...")
    blob.upload_from_filename(parquet_path)
    print(f"Uploaded gs://{BUCKET_NAME}/{blob_name}")

# ================================
# MAIN PIPELINE
# ================================
if __name__ == "__main__":
    for file_name in FHV_FILES:
        csv_path = download_file(file_name)
        parquet_path = transform_to_parquet(csv_path)
        upload_to_gcs(parquet_path)

    print("FHV 2019 ingestion complete!")