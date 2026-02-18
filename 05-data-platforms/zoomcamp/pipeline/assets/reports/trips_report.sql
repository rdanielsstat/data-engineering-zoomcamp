/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

# Same incremental pattern as staging: rebuild only the time window for the run.
materialization:
  type: table
#  strategy: time_interval
#  incremental_key: trip_date
#  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: Date of trip (from pickup_datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: Numeric code for how the passenger paid
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Sum of passenger_count across trips
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: double
    description: Sum of trip_distance in miles
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Sum of fare_amount in dollars
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: double
    description: Sum of tip_amount in dollars
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Sum of total_amount in dollars
    checks:
      - name: non_negative
  - name: average_fare_amount
    type: double
    description: Average fare_amount in dollars
    checks:
      - name: non_negative
  - name: average_passenger_count
    type: double
    description: Average passengers per trip
    checks:
      - name: non_negative

# No row_count_positive check: with time_interval, a run can legitimately insert 0 rows for its window.

@bruin */

-- Aggregate staging trips by date, taxi_type, payment_type for analytics.
-- Filter by run window so time_interval only inserts this window's aggregates.
SELECT
  CAST(pickup_datetime AS DATE) AS trip_date,
  taxi_type,
  payment_type,
  COUNT(*) AS trip_count,
  SUM(COALESCE(CAST(passenger_count AS BIGINT), 0)) AS total_passengers,
  SUM(COALESCE(trip_distance, 0)) AS total_trip_distance,
  SUM(COALESCE(fare_amount, 0)) AS total_fare_amount,
  SUM(COALESCE(tip_amount, 0)) AS total_tip_amount,
  SUM(COALESCE(total_amount, 0)) AS total_amount,
  AVG(COALESCE(fare_amount, 0)) AS average_fare_amount,
  AVG(COALESCE(passenger_count, 0)) AS average_passenger_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type