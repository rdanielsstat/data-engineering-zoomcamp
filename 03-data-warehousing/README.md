![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=googlebigquery&logoColor=white)
![Google Cloud Storage](https://img.shields.io/badge/Google%20Cloud%20Storage-4285F4?logo=googlecloud&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![DLT](https://img.shields.io/badge/DLT-Data%20Loading-8A2BE2)
![Data Warehouse](https://img.shields.io/badge/Data%20Warehouse-Analytics-orange)

# NYC Taxi Data Warehousing (BigQuery)

This project explores the process of moving raw public data into a cloud data warehouse and optimizing it for analytical queries using Google BigQuery.

The dataset used is the **NYC Yellow Taxi trip data for Jan–Jun 2024**.
The work focuses on ingestion, storage patterns, and query cost optimization.

## Dataset

Source: NYC Taxi & Limousine Commission Trip Records
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Files used:

- `yellow_tripdata_2024-01.parquet`
- `yellow_tripdata_2024-02.parquet`
- `yellow_tripdata_2024-03.parquet`
- `yellow_tripdata_2024-04.parquet`
- `yellow_tripdata_2024-05.parquet`
- `yellow_tripdata_2024-06.parquet`

## Local Prototyping with DuckDB

Before loading data into the cloud, the dataset was explored locally using DuckDB.

This allowed:
- Quick validation of schema and queries
- Fast experimentation without cloud costs
- A simple development workflow before deploying to GCP

## Uploading Data to Google Cloud Storage

Data was uploaded to GCS using two different approaches.

### DLT pipeline

A pipeline built with the DLT library handled ingestion using a service account for authentication.

### Python upload script

A custom script was also created using the Google Cloud Python SDK.
This script downloads the files, creates the bucket if needed, and uploads the data in parallel with retry logic.

Using both approaches helped demonstrate different ways of working with GCP services.

## BigQuery Tables

Two table types were created in BigQuery to compare behavior and performance.

### External table

The external table queries the Parquet files directly from GCS.

```sql
CREATE OR REPLACE EXTERNAL TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sandbox-486719-nyc-taxi-raw/yellow_tripdata_2024-*.parquet']
) ;
```

This keeps storage in GCS and avoids data duplication.

### Native table

The data was then loaded into BigQuery managed storage.

```sql
CREATE OR REPLACE TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_native` AS
SELECT * 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;
```

This improves query performance and enables warehouse optimizations.

## Row Count Verification

After creating the external and native tables, a row count check was performed to confirm that the load into BigQuery completed correctly and that both tables contain the same number of records.

```sql
SELECT COUNT(*) AS total_records 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;

SELECT COUNT(*) AS total_records 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Storage Efficiency and Data Read Estimation

External tables show 0 MB estimates because metadata is not managed by BigQuery. Native tables provide accurate estimates because they use managed columnar storage.

```sql
-- Querying External Table (Estimated: 0 MB)
SELECT COUNT(DISTINCT PULocationID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;

-- Querying Native Table (Estimated: 155.12 MB)
SELECT COUNT(DISTINCT PULocationID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

This difference highlights how BigQuery can better optimize and predict query cost when data is stored natively.

## Columnar Storage and Bytes Scanned

BigQuery’s cost is determined by the number of columns scanned. Adding more columns to a query increases the bytes processed linearly.

```sql
-- Lower processing cost (single column scan)
SELECT PULocationID 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;

-- Higher processing cost (multi-column scan)
SELECT PULocationID, DOLocationID 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

This demonstrates how limiting selected columns can significantly reduce query cost.

## Data Quality and Filtering

Identifying records with a fare_amount of 0 to detect potential data anomalies.

```sql
SELECT COUNT(*) AS zero_fare_count
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native`
 WHERE fare_amount = 0 ;
```

## Table Optimization: Partitioning and Clustering

To optimize for date-based filtering and vendor-based ordering, implement partitioning and clustering.

```sql
   CREATE OR REPLACE TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID AS
   SELECT * 
     FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Query Performance Comparison

Comparing the optimized table against the non-partitioned table for a March 1–15 date range scan.

* Non-partitioned Scan: 310.24 MB
* Partitioned Scan: 26.84 MB

```sql
-- Scan on standard materialized table
SELECT DISTINCT(VendorID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native`
 WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' ;

-- Scan on optimized table
SELECT DISTINCT(VendorID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_partitioned_clustered`
 WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' ;
```

Partitioning significantly reduces the amount of data scanned.

## BigQuery Metadata Optimization

BigQuery stores table statistics that allow some queries to run without scanning data.

```sql
SELECT COUNT(*) FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

Estimated bytes processed: 0 bytes

The result comes from metadata rather than a full table scan.