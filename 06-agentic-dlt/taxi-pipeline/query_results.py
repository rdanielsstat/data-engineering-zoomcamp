import duckdb

# Connect to the database created by taxi_pipeline
con = duckdb.connect("taxi_pipeline.duckdb")

# Question 1: Start date and end date
print("=" * 60)
print("Q1: Start and End Date of Dataset")
print("=" * 60)
result = con.execute("""
    SELECT 
        MIN(trip_pickup_date_time) as start_date,
        MAX(trip_dropoff_date_time) as end_date
    FROM taxi_pipeline_dataset.trips
""").fetchall()
print(f"Start Date: {result[0][0]}")
print(f"End Date: {result[0][1]}")

# Question 2: Proportion of trips paid with credit card
print("\n" + "=" * 60)
print("Q2: Proportion of Trips Paid with Credit Card")
print("=" * 60)
result = con.execute("""
    SELECT 
        payment_type,
        COUNT(*) as trip_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM taxi_pipeline_dataset.trips
    GROUP BY payment_type
    ORDER BY trip_count DESC
""").fetchall()
for row in result:
    print(f"Payment Type: {row[0]}, Count: {row[1]}, Percentage: {row[2]}%")

# Question 3: Total amount of money generated in tips
print("\n" + "=" * 60)
print("Q3: Total Amount of Money Generated in Tips")
print("=" * 60)
result = con.execute("""
    SELECT 
        SUM(tip_amt) as total_tips
    FROM taxi_pipeline_dataset.trips
""").fetchall()
print(f"Total Tips: ${result[0][0]:,.2f}")

con.close()