from google.cloud import bigquery

# ================================
# CONFIG
# ================================
PROJECT_ID = "sandbox-486719"
DATASET_NAME = "nytaxi"
BUCKET_NAME = "sandbox-486719-taxi-data"
CREDENTIALS_FILE = "taxi_rides_ny/gcs.json"

client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"

# ================================
# FHV SCHEMA
# ================================
fhv_schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("pickup_location_id", "STRING"),
    bigquery.SchemaField("dropoff_location_id", "STRING"),
    bigquery.SchemaField("sr_flag", "NUMERIC"),  # <--- change to NUMERIC
    bigquery.SchemaField("affiliated_base_number", "STRING"),
]

# ================================
# CREATE DATASET
# ================================
def create_dataset():
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        client.get_dataset(dataset_id)
        print("Dataset already exists.")
    except Exception:
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_NAME}")

# ================================
# LOAD TABLE FROM GCS
# ================================
def load_table(table_name, gcs_uri, schema):
    table_id = f"{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.PARQUET,
        write_disposition = "WRITE_TRUNCATE",
        schema = schema,
        autodetect = False,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    print(f"Starting load job for {table_name}...")
    load_job.result()
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_name}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    create_dataset()

    fhv_uri = f"gs://{BUCKET_NAME}/parquet/fhv_tripdata_2019-*.parquet"
    load_table("stg_fhv_tripdata", fhv_uri, fhv_schema)

    print("FHV 2019 BigQuery load complete!")