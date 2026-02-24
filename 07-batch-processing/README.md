## PySpark version

```bash
pyspark
```
Result: `4.1.1`

Or alternatively, using a Python script:

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

Result: `4.1.1`

## Partitioned data file size

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

## Count records

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

## Longest trip

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

## User interface

By default, the local user interface uses port `4040`. E.g., `localhost:4040`.

## Least frequent pickup location zone

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