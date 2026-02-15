from google.cloud import storage
import os
import sys
import urllib.request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
from decimal import Decimal

client = storage.Client.from_service_account_json("gcs.json")

# ================================
# CONFIG
# ================================
BUCKET_NAME = "sandbox-486719-taxi-data"
CREDENTIALS_FILE = "gcs.json"
DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(RAW_DIR, exist_ok = True)
os.makedirs(PARQUET_DIR, exist_ok = True)

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

# ================================
# GENERATE URLS
# ================================
def generate_file_urls(data_type = "yellow", start_year = 2019, end_year = 2020):
    urls = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            month_str = f"{month:02d}"
            file_name = f"{data_type}_tripdata_{year}-{month_str}.csv.gz"
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{data_type}/{file_name}"
            urls.append((data_type, year, month, file_name, url))
    return urls

ALL_FILES = generate_file_urls("yellow") + generate_file_urls("green")

# ================================
# SCHEMAS
# ================================

YELLOW_RENAME = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "imp_surcharge",
    "airport_fee": "airport_fee",
    "total_amount": "total_amount",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id"
}

GREEN_RENAME = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "RatecodeID": "rate_code",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "ehail_fee": "ehail_fee",
    "airport_fee": "airport_fee",
    "total_amount": "total_amount",
    "payment_type": "payment_type",
    "trip_type": "trip_type",
    "improvement_surcharge": "imp_surcharge",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id"
}

# Full schema objects including dtypes, rename map, column order, datetime columns
YELLOW_SCHEMA = {
    "dtypes": {
        "vendor_id": "string",
        "pickup_datetime": "datetime64[ns]",
        "dropoff_datetime": "datetime64[ns]",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "rate_code": "string",
        "store_and_fwd_flag": "string",
        "payment_type": "string",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "imp_surcharge": "float64",
        "airport_fee": "float64",
        "total_amount": "float64",
        "pickup_location_id": "string",
        "dropoff_location_id": "string",
        "data_file_year": "Int64",
        "data_file_month": "Int64"
    },
    "rename_map": YELLOW_RENAME,
    "columns": [
        "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance",
        "rate_code", "store_and_fwd_flag", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "imp_surcharge", "airport_fee", "total_amount",
        "pickup_location_id", "dropoff_location_id", "data_file_year", "data_file_month"
    ],
    "datetime_cols": ["pickup_datetime", "dropoff_datetime"]
}

GREEN_SCHEMA = {
    "dtypes": {
        "vendor_id": "string",
        "pickup_datetime": "datetime64[ns]",
        "dropoff_datetime": "datetime64[ns]",
        "store_and_fwd_flag": "string",
        "rate_code": "string",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "ehail_fee": "float64",
        "airport_fee": "float64",
        "total_amount": "float64",
        "payment_type": "string",
        "distance_between_service": "float64",
        "time_between_service": "Int64",
        "trip_type": "string",
        "imp_surcharge": "float64",
        "pickup_location_id": "string",
        "dropoff_location_id": "string",
        "data_file_year": "Int64",
        "data_file_month": "Int64"
    },
    "rename_map": GREEN_RENAME,
    "columns": [
        "vendor_id", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "rate_code",
        "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "ehail_fee", "airport_fee", "total_amount", "payment_type",
        "distance_between_service", "time_between_service", "trip_type", "imp_surcharge",
        "pickup_location_id", "dropoff_location_id", "data_file_year", "data_file_month"
    ],
    "datetime_cols": ["pickup_datetime", "dropoff_datetime"]
}

# Define numeric columns for BigQuery NUMERIC
NUMERIC_COLS_YELLOW = [
    "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "imp_surcharge",
    "airport_fee", "total_amount"
]

NUMERIC_COLS_GREEN = NUMERIC_COLS_YELLOW + ["ehail_fee", "distance_between_service"]

INTEGER_COLS_YELLOW = ["passenger_count", "data_file_year", "data_file_month"]

INTEGER_COLS_GREEN = [
    "passenger_count",
    "time_between_service",
    "data_file_year",
    "data_file_month"
]

BQ_STRING_COLS_YELLOW = [
    "vendor_id","rate_code","store_and_fwd_flag","payment_type",
    "pickup_location_id","dropoff_location_id"
]

BQ_STRING_COLS_GREEN = [
    "vendor_id","store_and_fwd_flag","rate_code","payment_type",
    "trip_type","pickup_location_id","dropoff_location_id"
]

BQ_TIMESTAMP_COLS = ["pickup_datetime","dropoff_datetime"]

BQ_INTEGER_COLS_YELLOW = [
    "passenger_count","data_file_year","data_file_month"
]

BQ_INTEGER_COLS_GREEN = [
    "passenger_count","time_between_service","data_file_year","data_file_month"
]

BQ_NUMERIC_COLS_YELLOW = [
    "trip_distance","fare_amount","extra","mta_tax","tip_amount",
    "tolls_amount","imp_surcharge","airport_fee","total_amount"
]

BQ_NUMERIC_COLS_GREEN = [
    "trip_distance","fare_amount","extra","mta_tax","tip_amount",
    "tolls_amount","ehail_fee","airport_fee","total_amount",
    "distance_between_service","imp_surcharge"
]

# ================================
# BUCKET CREATION
# ================================
def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} exists.")
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket {bucket_name}")
    except Forbidden:
        print("Bucket exists but not accessible. Choose another name.")
        sys.exit(1)

# ================================
# DOWNLOAD
# ================================
def download_file(file_tuple):
    data_type, year, month, file_name, url = file_tuple
    file_path = os.path.join(RAW_DIR, file_name)

    try:
        print(f"Downloading {file_name}")
        urllib.request.urlretrieve(url, file_path)

        return (data_type, year, month, file_name, file_path)

    except Exception as e:
        print(f"Download failed: {file_name} {e}")
        return None

# ================================
# TRANSFORM CSV TO PARQUET
# ================================
def transform_to_parquet(file_info):
    data_type, year, month, file_name, csv_path = file_info

    schema = YELLOW_SCHEMA if data_type == "yellow" else GREEN_SCHEMA

    print(f"Transforming {file_name}")

    df = pd.read_csv(csv_path, compression = "gzip", low_memory = False)

    # Rename columns
    df = df.rename(columns = schema["rename_map"])

    # Add metadata columns
    df["data_file_year"] = year
    df["data_file_month"] = month

    # Parse datetimes
    for col in schema["datetime_cols"]:
        df[col] = pd.to_datetime(df[col], errors = "coerce")

    # Choose correct groups
    if data_type == "yellow":
        string_cols = BQ_STRING_COLS_YELLOW
        int_cols = BQ_INTEGER_COLS_YELLOW
        numeric_cols = BQ_NUMERIC_COLS_YELLOW
    else:
        string_cols = BQ_STRING_COLS_GREEN
        int_cols = BQ_INTEGER_COLS_GREEN
        numeric_cols = BQ_NUMERIC_COLS_GREEN

    # ---- STRING → STRING ----
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # ---- TIMESTAMP → TIMESTAMP ----
    for col in BQ_TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")

    # ---- INTEGER → INT64 ----
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # ---- NUMERIC → DECIMAL (BigQuery NUMERIC) ----
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

    # Ensure all expected columns exist
    for col in schema["dtypes"].keys():
        if col not in df.columns:
            df[col] = pd.NA

    # 2. Convert datetime columns to true timestamp
    for col in schema["datetime_cols"]:
        df[col] = pd.to_datetime(df[col], errors = "coerce")
        df[col] = df[col].astype("datetime64[us]")

    # 3. Force TRUE integer columns (fix float -> int problem)
    int_cols = INTEGER_COLS_YELLOW if data_type == "yellow" else INTEGER_COLS_GREEN

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors = "coerce").astype("Int64")

    # 4. Convert FLOAT columns to Decimal for BigQuery NUMERIC
    numeric_cols = NUMERIC_COLS_YELLOW if data_type == "yellow" else NUMERIC_COLS_GREEN

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors = "coerce").astype("float64")
            df[col] = df[col].apply(
                lambda x: Decimal(str(x)) if pd.notnull(x) else None
            )

    # Enforce column order
    df = df[schema["columns"]]

    parquet_name = file_name.replace(".csv.gz", ".parquet")
    parquet_path = os.path.join(PARQUET_DIR, parquet_name)

    df.to_parquet(
        parquet_path,
        engine = "pyarrow",
        index = False,
        coerce_timestamps = "us",
        allow_truncated_timestamps = True
    )

    return parquet_path

# ================================
# UPLOAD TO GCS
# ================================
def upload_to_gcs(file_path):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(f"parquet/{blob_name}")
    blob.chunk_size = CHUNK_SIZE

    print(f"Uploading {blob_name} to GCS...")
    blob.upload_from_filename(file_path)
    print(f"Uploaded gs://{BUCKET_NAME}/parquet/{blob_name}")

# ================================
# MAIN PIPELINE
# ================================
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # 1 Download
    with ThreadPoolExecutor(max_workers = 4) as executor:
        downloads = list(executor.map(download_file, ALL_FILES))
    downloads = [d for d in downloads if d]

    # 2 Transform
    with ThreadPoolExecutor(max_workers = 4) as executor:
        parquet_files = list(executor.map(transform_to_parquet, downloads))

    # 3 Upload
    with ThreadPoolExecutor(max_workers = 4) as executor:
        executor.map(upload_to_gcs, parquet_files)

    print("Pipeline complete!")