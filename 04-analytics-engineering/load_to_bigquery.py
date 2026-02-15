from google.cloud import bigquery

# ================================
# CONFIG
# ================================
PROJECT_ID = "sandbox-486719"
DATASET_NAME = "nytaxi"
BUCKET_NAME = "sandbox-486719-taxi-data"

CREDENTIALS_FILE = "gcs.json"

client = bigquery.Client.from_service_account_json(
    CREDENTIALS_FILE, project = PROJECT_ID
)

# ================================
# SCHEMAS
# ================================
green_schema = [
    bigquery.SchemaField("vendor_id", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("rate_code", "STRING"),
    bigquery.SchemaField("passenger_count", "INT64"),
    bigquery.SchemaField("trip_distance", "NUMERIC"),
    bigquery.SchemaField("fare_amount", "NUMERIC"),
    bigquery.SchemaField("extra", "NUMERIC"),
    bigquery.SchemaField("mta_tax", "NUMERIC"),
    bigquery.SchemaField("tip_amount", "NUMERIC"),
    bigquery.SchemaField("tolls_amount", "NUMERIC"),
    bigquery.SchemaField("ehail_fee", "NUMERIC"),
    bigquery.SchemaField("airport_fee", "NUMERIC"),
    bigquery.SchemaField("total_amount", "NUMERIC"),
    bigquery.SchemaField("payment_type", "STRING"),
    bigquery.SchemaField("distance_between_service", "NUMERIC"),
    bigquery.SchemaField("time_between_service", "INT64"),
    bigquery.SchemaField("trip_type", "STRING"),
    bigquery.SchemaField("imp_surcharge", "NUMERIC"),
    bigquery.SchemaField("pickup_location_id", "STRING"),
    bigquery.SchemaField("dropoff_location_id", "STRING"),
    bigquery.SchemaField("data_file_year", "INT64"),
    bigquery.SchemaField("data_file_month", "INT64"),
]

yellow_schema = [
    bigquery.SchemaField("vendor_id", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("passenger_count", "INT64"),
    bigquery.SchemaField("trip_distance", "NUMERIC"),
    bigquery.SchemaField("rate_code", "STRING"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("payment_type", "STRING"),
    bigquery.SchemaField("fare_amount", "NUMERIC"),
    bigquery.SchemaField("extra", "NUMERIC"),
    bigquery.SchemaField("mta_tax", "NUMERIC"),
    bigquery.SchemaField("tip_amount", "NUMERIC"),
    bigquery.SchemaField("tolls_amount", "NUMERIC"),
    bigquery.SchemaField("imp_surcharge", "NUMERIC"),
    bigquery.SchemaField("airport_fee", "NUMERIC"),
    bigquery.SchemaField("total_amount", "NUMERIC"),
    bigquery.SchemaField("pickup_location_id", "STRING"),
    bigquery.SchemaField("dropoff_location_id", "STRING"),
    bigquery.SchemaField("data_file_year", "INT64"),
    bigquery.SchemaField("data_file_month", "INT64"),
]

dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"

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

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config = job_config,
    )

    print(f"Starting load job for {table_name}...")
    load_job.result()  # wait for completion

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_name}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    create_dataset()

    # Load yellow taxis
    yellow_uri = f"gs://{BUCKET_NAME}/parquet/yellow_tripdata_*.parquet"
    load_table("yellow_tripdata", yellow_uri, yellow_schema)

    # Load green taxis
    green_uri = f"gs://{BUCKET_NAME}/parquet/green_tripdata_*.parquet"
    load_table("green_tripdata", green_uri, green_schema)

    print("BigQuery load complete!")