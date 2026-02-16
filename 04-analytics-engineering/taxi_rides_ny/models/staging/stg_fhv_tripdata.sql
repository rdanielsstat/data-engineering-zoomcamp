{{ 
  config(
    materialized = 'view'
  ) 
}}

with source as (
    select *
    from {{ source('raw', 'fhv_tripdata') }}
    where EXTRACT(year from pickup_datetime) = 2019
      and dispatching_base_num is not null
)

select
    dispatching_base_num,
    cast(PUlocationID as int64) as pickup_location_id,
    cast(DOlocationID as int64) as dropoff_location_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type
from source