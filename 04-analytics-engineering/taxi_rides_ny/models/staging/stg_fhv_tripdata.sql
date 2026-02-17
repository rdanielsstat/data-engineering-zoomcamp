with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (

    select
        -- keep renaming pattern even if names already match
        dispatching_base_num as dispatching_base_num,
        pickup_datetime as pickup_datetime,
        dropoff_datetime as dropoff_datetime,
        pickup_location_id as pickup_location_id,
        dropoff_location_id as dropoff_location_id,
        sr_flag as sr_flag,
        affiliated_base_number as affiliated_base_number
     from source
    where dispatching_base_num is not null

)

select * from renamed