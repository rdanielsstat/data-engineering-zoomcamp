import json
import time
import sys
from pathlib import Path
import math

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer

# Load green trips data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]
df = pd.read_parquet(url, columns=columns)

# Replace all NaNs with None for valid JSON
df = df.where(pd.notnull(df), None)

# JSON serializer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka setup
server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

t0 = time.time()
topic_name = 'green-trips'

# Send each record
for _, row in df.iterrows():
    record = row.to_dict()

    # Ensure datetime formatting
    record['lpep_pickup_datetime'] = pd.to_datetime(record['lpep_pickup_datetime']).strftime('%Y-%m-%d %H:%M:%S')
    record['lpep_dropoff_datetime'] = pd.to_datetime(record['lpep_dropoff_datetime']).strftime('%Y-%m-%d %H:%M:%S')

    # Extra safety: convert any remaining NaN floats to None
    for k, v in record.items():
        if isinstance(v, float) and (v != v):  # catches any residual NaN
            record[k] = None

    producer.send(topic_name, value=record)
    print(f"Sent: {record}")

producer.flush()
t1 = time.time()
print(f'Took {(t1 - t0):.2f} seconds')