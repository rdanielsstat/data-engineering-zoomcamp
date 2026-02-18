/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
#  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
#  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
#  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pulocationid
    type: integer
    description: TLC Taxi Zone in which the taximeter was engaged
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dolocationid
    type: integer
    description: TLC Taxi Zone in which the taximeter was disengaged
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: Timestamp when the data was extracted from the source
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: trip_distance
    type: double
    description: The elapsed trip distance in miles reported by the taximeter
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: store_and_fwd_flag
    type: varchar
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: Numeric code signifying how the passenger paid for the trip
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: Fare amount in dollars
    checks:
      - name: non_negative
  - name: tip_amount
    type: double
    description: Tip amount in dollars
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total amount charged in dollars
    checks:
      - name: non_negative

# Optional: custom checks (e.g. row_count_positive) fail when time_interval runs
# process a window with no data, so they are omitted here.
# Docs: https://getbruin.com/docs/bruin/quality/custom

@bruin */

-- Normalize pickup/dropoff (yellow=tpep_*, green=lpep_* when present) and location (pulocationid or pu_location_id).
-- Join with payment lookup; deduplicate by composite key; filter invalid rows.
-- Uses only tpep_* and pu_location_id/do_location_id so staging works when only yellow taxi is ingested (no lpep_* columns).
WITH normalized AS (
  SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    pu_location_id AS pulocationid,
    do_location_id AS dolocationid,
    taxi_type,
    extracted_at,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    payment_type,
    COALESCE(fare_amount, 0) AS fare_amount,
    COALESCE(tip_amount, 0) AS tip_amount,
    COALESCE(total_amount, 0) AS total_amount
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND taxi_type IS NOT NULL
    AND extracted_at IS NOT NULL
    AND passenger_count IS NOT NULL
    AND trip_distance IS NOT NULL
    AND store_and_fwd_flag IS NOT NULL
    AND payment_type IS NOT NULL
),
with_rn AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pulocationid, dolocationid, taxi_type
      ORDER BY extracted_at DESC
    ) AS rn
  FROM normalized
)
SELECT
  pickup_datetime,
  dropoff_datetime,
  pulocationid,
  dolocationid,
  taxi_type,
  extracted_at,
  CAST(passenger_count AS INTEGER) AS passenger_count,
  trip_distance,
  store_and_fwd_flag,
  CAST(payment_type AS INTEGER) AS payment_type,
  fare_amount,
  tip_amount,
  total_amount
FROM with_rn
INNER JOIN ingestion.payment_lookup pl ON pl.payment_type_id = CAST(with_rn.payment_type AS INTEGER)
WHERE with_rn.rn = 1