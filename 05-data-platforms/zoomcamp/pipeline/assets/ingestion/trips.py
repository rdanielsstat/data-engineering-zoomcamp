"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-zoomcamp

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendorid
    type: DOUBLE
    description: A code indicating the TPEP provider that provided the record
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (yellow taxis only)
  - name: lpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged (green taxis only)
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (yellow taxis only)
  - name: lpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged (green taxis only)
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
  - name: payment_type
    type: DOUBLE

@bruin"""

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
from io import BytesIO

def materialize():
    """
    Ingests NYC taxi trip data from TLC public endpoint.
    
    Fetches parquet files for specified taxi types and date range,
    loads them into DataFrames, and returns concatenated results.
    """
    # Read date range from Bruin environment variables
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Read taxi_types variable from BRUIN_VARS
    bruin_vars_str = os.environ.get("BRUIN_VARS", "{}")
    bruin_vars = json.loads(bruin_vars_str)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    # Base URL for TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of URLs for each taxi type and month in the date range
    dataframes = []
    extraction_timestamp = datetime.now()
    
    current_date = start_date.replace(day=1)  # Start from first day of start month
    
    while current_date <= end_date:
        year_month = current_date.strftime("%Y-%m")
        
        for taxi_type in taxi_types:
            # Construct filename: <taxi_type>_tripdata_<year>-<month>.parquet
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = f"{base_url}{filename}"
            
            try:
                # Fetch parquet file
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Read parquet into DataFrame
                parquet_data = BytesIO(response.content)
                df = pd.read_parquet(parquet_data)
                
                # Normalize column names to lowercase so staging/report can rely on them
                df.columns = [c.strip().lower() if isinstance(c, str) else c for c in df.columns]
                
                # Ensure fare/tip/total columns exist for staging and report (TLC parquet has them; fill 0 if missing)
                for col, default in [("fare_amount", 0.0), ("tip_amount", 0.0), ("total_amount", 0.0)]:
                    if col not in df.columns:
                        df[col] = default
                
                # Add taxi_type column
                df["taxi_type"] = taxi_type
                
                # Add extracted_at timestamp for lineage/debugging
                df["extracted_at"] = extraction_timestamp
                
                dataframes.append(df)
                print(f"Successfully fetched {filename}: {len(df)} rows")
                
            except requests.exceptions.RequestException as e:
                print(f"Warning: Failed to fetch {url}: {e}")
                # Continue with other files even if one fails
                continue
        
        # Move to next month
        current_date = current_date + relativedelta(months=1)
    
    if not dataframes:
        raise ValueError("No data was successfully fetched. Check date range and taxi_types.")
    
    # Concatenate all DataFrames
    final_dataframe = pd.concat(dataframes, ignore_index=True)
    
    print(f"Total rows ingested: {len(final_dataframe)}")
    
    return final_dataframe