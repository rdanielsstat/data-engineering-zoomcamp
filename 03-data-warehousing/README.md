# Project Overview: Multi-Stage Data Ingestion and Warehousing

This project focuses on the NYC Yellow Taxi data from the first half of 2024. I worked with six monthly files (January through June) and built a pipeline to move that data from raw files into a professional cloud setup, demonstrating both local prototyping and production-ready cloud ingestion.

## Data Acquisition and Local Prototyping

The ingestion began with programmatic retrieval of the following source files:

- `yellow_tripdata_2024-01.parquet`
- `yellow_tripdata_2024-02.parquet`
- `yellow_tripdata_2024-03.parquet`
- `yellow_tripdata_2024-04.parquet`
- `yellow_tripdata_2024-05.parquet`
- `yellow_tripdata_2024-06.parquet`

The data can be downloaded manually from [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).  

For initial development and local testing, the data was ingested into a **DuckDB** instance. This provided:

- High-performance analytical queries
- Schema validation
- Low-cost experimentation before moving to the cloud

## Cloud Orchestration and Ingestion Patterns
The transition to **Google Cloud Platform (GCP)** involved two primary ingestion methodologies:

### 1. Automated Data Loading (DLT)
- Implemented using the **DLT (Data Load Tool)** library.
- Streamlines migration of data into GCP.
- Secured via **service account key authentication**, ensuring a reproducible pipeline.

### 2. Custom API Integration
- Secondary ingestion script leveraging `urllib` for data retrieval and the **Google Cloud Python SDK** for bucket management.
- Authentication handled via **Google Cloud CLI/SDK**, using managed identity credentials.

By using both DLT and native Google Cloud APIs, the project demonstrates proficiency in **high-level orchestration tools** and **direct cloud service integration**.

# 2024 Yellow NYC Taxi Data: BigQuery Optimization and Workflow

This section outlines the process for ingesting, optimizing, and analyzing the 2024 NYC Yellow Taxi dataset within GCP, comparing external and native tables, and demonstrating performance improvements.

## Data ingestion: external vs. native tables
The project began by establishing two distinct table types to compare performance and storage behaviors.

### External table
The data remains in Google Cloud Storage (GCS). This creates a "link" without moving the physical data.
```sql
CREATE OR REPLACE EXTERNAL TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sandbox-486719-nyc-taxi-raw/yellow_tripdata_2024-*.parquet']
) ;
```
### Native (materialized) table
Data is ingested into BigQuery’s managed storage for better performance.

```sql
CREATE OR REPLACE TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_native` AS
SELECT * 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;
```

## Row count verification

A baseline check to ensure both table structures reflect the same total volume of records.

```sql
SELECT COUNT(*) AS total_records 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;

SELECT COUNT(*) AS total_records 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Storage efficiency and data read estimation
External tables show 0 MB estimates because metadata is not managed by BigQuery. Native tables provide accurate estimates because they use managed columnar storage.

```sql
-- Querying External Table (Estimated: 0 MB)
SELECT COUNT(DISTINCT PULocationID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_external` ;

-- Querying Native Table (Estimated: 155.12 MB)
SELECT COUNT(DISTINCT PULocationID)
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Columnar storage analysis
BigQuery’s cost is determined by the number of columns scanned. Adding more columns to a query increases the bytes processed linearly.

```sql
-- Lower processing cost (single column scan)
SELECT PULocationID 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;

-- Higher processing cost (multi-column scan)
SELECT PULocationID, DOLocationID 
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Data quality and filtering
Identifying records with a fare_amount of 0 to detect potential data anomalies.

```sql
SELECT COUNT(*) AS zero_fare_count
  FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native`
 WHERE fare_amount = 0 ;
```

## Table optimization: partitioning and clustering
To optimize for date-based filtering and vendor-based ordering, implement partitioning and clustering.

```sql
   CREATE OR REPLACE TABLE `sandbox-486719.rides_dataset.yellow_taxi_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID AS
   SELECT * 
     FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

## Performance benchmarking
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

## Metadata caching mechanisms
For native tables, count(*) estimates 0 bytes because it retrieves the answer from pre-calculated metadata.

```sql
SELECT count(*) FROM `sandbox-486719.rides_dataset.yellow_taxi_2024_native` ;
```

**Observation:** 0 bytes processed.
**Technical Detail:** BigQuery’s metadata layer stores statistics like total row count, making this operation instantaneous and free.