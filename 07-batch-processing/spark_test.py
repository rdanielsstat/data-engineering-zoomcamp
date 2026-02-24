from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySparkProject") \
    .getOrCreate()

# Print Spark version
print(spark.version)