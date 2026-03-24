import pandas as pd

# Load the Parquet data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'tip_amount'
]
df = pd.read_parquet(url, columns=columns)

# Convert pickup time to datetime
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])

# Set pickup time as index
df = df.set_index('lpep_pickup_datetime')

# Resample by 1-hour tumbling window and sum tips
hourly_tips = df['tip_amount'].resample('1h').sum()

# Find the hour with the maximum total tip
max_tip_hour = hourly_tips.idxmax()
max_tip_amount = hourly_tips.max()

print(f"Hour with highest total tips: {max_tip_hour}")
print(f"Total tips in that hour: {max_tip_amount}")