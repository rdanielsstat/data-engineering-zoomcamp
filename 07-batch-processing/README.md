![PySpark](https://img.shields.io/badge/PySpark-EE4C2C?logo=apache-spark&logoColor=white)
![Google Cloud Dataproc](https://img.shields.io/badge/Dataproc-4285F4?logo=googlecloud&logoColor=white)

# PySpark Data Engineering Project: NYC Yellow Taxi November 2025

This project demonstrates practical experience with **Apache Spark and PySpark** in a data engineering workflow. Using the NYC Yellow Taxi dataset for November 2025, we explore data ingestion, partitioning, querying, and analysis both locally and in the cloud. The project also integrates Spark with Google Cloud (BigQuery and DataProc), showcasing scalable data processing.

The goals of this project include:

- Installing and setting up PySpark locally and on a cloud cluster
- Loading and transforming Parquet datasets
- Repartitioning data and understanding file sizes
- Working with Spark DataFrames and SQL views
- Performing analytical queries such as counting records and finding extreme values
- Demonstrating Spark's user interface and cluster management

## Environment Setup and Spark Installation

To start, PySpark was installed locally and verified using:

```bash
pyspark
```

Result: `4.1.1`

Alternatively, using a Python script to create a Spark session:

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySparkProject") \
    .getOrCreate()

# Print Spark version
print(spark.version)
```

Run the script:

```bash
python spark_test.py
```

Output confirms the Spark version: `4.1.1`.

Concept:

Setting up PySpark locally allows testing transformations on a single machine before scaling to a cloud cluster. Spark sessions are the entry point to using the Spark API for DataFrames, RDDs, and SQL.

## Data Ingestion and Partitioning

The dataset used is the Yellow Taxi November 2025 Parquet file. After loading into a Spark DataFrame, the data was repartitioned into 4 partitions and saved as Parquet files. Average size of the Parquet files was then calculated in Python. See full script below.

```python
import os
import urllib.request
from pyspark.sql import SparkSession

# URLs and paths
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
folder_path = "data/pq/yellow/2025/11"
output_path = folder_path  # save in the same folder

# Make sure folder exists
os.makedirs(folder_path, exist_ok = True)

# Download the file
file_name = os.path.join(folder_path, "yellow_tripdata_2025-11.parquet")

if not os.path.exists(file_name):
    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, file_name)
    print("Download complete.")
else:
    print("File already exists, skipping download.")

# Start Spark session
spark = SparkSession.builder \
    .appName("PySparkProject") \
    .getOrCreate()

# Read the Parquet file
df = spark.read.parquet(file_name)
print("Number of rows:", df.count())
df.show(5)

# Repartition to 4
df_repart = df.repartition(4)

# Write to parquet (overwrite if exists)
df_repart.write.mode("overwrite").parquet(output_path)
print(f"Repartitioned Parquet files written to {output_path}")

# Calculate average size of parquet files
parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
sizes_mb = [os.path.getsize(os.path.join(output_path, f)) / (1024*1024) for f in parquet_files]

if sizes_mb:
    avg_size = sum(sizes_mb) / len(sizes_mb)
    print("Parquet files sizes (MB):", ["{:.2f}".format(s) for s in sizes_mb])
    print("Average Parquet file size (MB): {:.2f}".format(avg_size))
else:
    print("No parquet files found in", output_path)
```

Result:
```text
Parquet files sizes (MB): ['24.41', '24.40', '24.43', '24.42']
Average Parquet file size (MB): 24.42
```

Concepts:
- Repartitioning optimizes parallelism for Spark transformations and can affect file size.
- Parquet is a columnar format; saving partitions allows Spark to efficiently read subsets of data in parallel.
- Understanding average file size helps plan for cluster resources and performance tuning.

## Basic Data Exploration

I created a new column with just the date part of the pickup timestamp, then filtered for trips on November 15, 2025.

```python
from pyspark.sql.functions import col, to_date

# Read the repartitioned folder
df = spark.read.parquet("data/pq/yellow/2025/11/")

# Create a new column with just the date
df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

# Filter for November 15, 2025
df_15 = df.filter(col("pickup_date") == "2025-11-15")

# Count trips
trip_count = df_15.count()
print("Number of trips on 2025-11-15:", trip_count)
```

Result: 162,604

Concepts:
- Filtering DataFrames by date demonstrates Spark’s ability to handle column transformations efficiently.
- Counting records validates data ingestion and partitioning.

## Trip Duration Analysis

Trip durations were computed in hours.

```python
from pyspark.sql.functions import to_timestamp, unix_timestamp, max as spark_max

# Read the repartitioned Parquet folder
df = spark.read.parquet("data/pq/yellow/2025/11/")

# Convert pickup and dropoff to timestamps
df = df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"))) \
       .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime")))

# Compute trip duration in hours
df = df.withColumn("trip_hours", (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 3600)

# Find the maximum trip duration
max_trip_hours = df.select(spark_max(col("trip_hours"))).collect()[0][0]
print("Longest trip in hours:", max_trip_hours)
```

Result: 90.6

Concepts:
- Converting timestamps to numeric durations allows quantitative analysis.
- Using Spark’s built-in functions (`unix_timestamp`, `max`) ensures computations scale with large datasets.

## Spark User Interface

Spark provides a local web UI to monitor jobs and stages. By default, it runs on:

`localhost:4040`

This interface allows inspection of task execution, shuffle operations, and memory usage.

Concepts:
- Monitoring Spark jobs is critical for debugging and performance tuning.
- Even when running locally, the UI mirrors what would be visible on a cluster.

## Analytical Queries: Pickup Zones

To analyze pickup locations, I loaded the zone lookup CSV. I read the CSV into a Spark DataFrame and created temporary views. I then identified the least frequent pickup zone.

```python
# Download the zones CSV
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
local_csv_path = "taxi_zone_lookup.csv"

urllib.request.urlretrieve(url, local_csv_path)

# Read the CSV into a Spark DataFrame
df_zones = spark.read.option("header", "true").csv(local_csv_path)

df_zones.write.mode("overwrite").parquet("zones")

# Create a temp view for SQL queries
df_zones.createOrReplaceTempView("zones")

# Create a temp view for the November 2025 data
df.createOrReplaceTempView("yellow_trips")

least_freq_zone = spark.sql("""
    SELECT z.Zone AS zone_name, COUNT(*) AS trip_count
      FROM yellow_trips y
      JOIN zones z
        ON y.PULocationID = z.LocationID
     GROUP BY z.Zone
    HAVING COUNT(*) > 0
     ORDER BY trip_count ASC
     LIMIT 1
""")

least_freq_zone.show(truncate = False)
```

Alternatively:

```python
# Using Python
from pyspark.sql import functions as F

# Join with zones DataFrame
df_joined = df.join(
    df_zones, 
    df.PULocationID == df_zones.LocationID,
    how = 'inner'
)

# Count trips per Zone
df_counts = df_joined.groupBy("Zone").count()

# Order ascending to find the least frequent pickup zone
df_least = df_counts.orderBy("count", ascending = True).limit(1)

# Show the result
df_least.show(truncate = False)
```

Result: Arden Heights

Concepts:
- Temporary views allow SQL-style queries on DataFrames.
- Joining trip data with lookup tables demonstrates enrichment and relational operations.
- Aggregation and ordering reveal patterns and outliers.

## Spark in the Cloud

Spark was also tested on a **Google Cloud Dataproc cluster**, using both a local session for development and a remote cluster for scaled execution. Spark submit was used to pass command-line arguments specifying dataset paths and years. Cloud deployment allows large datasets to be processed efficiently and integrates with BigQuery for downstream analytics.

Concepts:
- Cloud Spark clusters enable distributed computing across multiple nodes.
- Command-line arguments increase flexibility and reproducibility of scripts.
- Integration with BigQuery showcases end-to-end cloud data engineering pipelines.

## Summary

This project demonstrates:
- Setting up PySpark locally and in the cloud
- Data ingestion, repartitioning, and Parquet storage
- Basic exploratory analysis and aggregation
- Working with SQL views and joins
- Trip duration computations and analytics
- Monitoring Spark jobs via the UI

Through this workflow, key data engineering concepts are reinforced, including:
- Parallel processing and partitioning
- Scalable data transformations
- Reproducibility and workflow automation
- Integration of Spark with cloud services