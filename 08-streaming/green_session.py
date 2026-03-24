import pandas as pd

# Load the Parquet data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'PULocationID'
]
df = pd.read_parquet(url, columns=columns)

# Convert pickup time to datetime
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])

# Sort by location and pickup time
df = df.sort_values(['PULocationID', 'lpep_pickup_datetime'])

# Define a session: gap > 5 minutes starts a new session
gap = pd.Timedelta(minutes=5)
df['prev_time'] = df.groupby('PULocationID')['lpep_pickup_datetime'].shift(1)
df['new_session'] = (df['lpep_pickup_datetime'] - df['prev_time'] > gap) | (df['prev_time'].isna())
df['session_id'] = df.groupby('PULocationID')['new_session'].cumsum()

# Count trips per session
session_counts = df.groupby(['PULocationID', 'session_id']).size().reset_index(name='num_trips')

# Find the session with the most trips
max_session = session_counts['num_trips'].max()
max_info = session_counts[session_counts['num_trips'] == max_session]

print("Longest session(s):")
print(max_info)
print(f"\nMaximum number of trips in a session: {max_session}")