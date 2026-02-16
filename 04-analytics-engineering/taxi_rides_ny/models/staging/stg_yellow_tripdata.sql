with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers (standardized naming for consistency across yellow/green)
        cast(cast(vendor_id as numeric) as int64) as vendor_id,
        cast(cast(rate_code as numeric) as int64) as rate_code_id,
        cast(pickup_location_id as integer) as pickup_location_id,
        cast(dropoff_location_id as integer) as dropoff_location_id,

        -- timestamps (standardized naming)
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(imp_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(cast(payment_type as numeric) as int64) as payment_type

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where vendor_id is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}