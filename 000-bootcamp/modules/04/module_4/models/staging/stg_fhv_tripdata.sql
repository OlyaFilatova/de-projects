with source as (

    select * from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num as dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(sr_flag as integer) as sr_flag,
        affiliated_base_number as affiliated_base_number

    from source
    where dispatching_base_num IS NOT NULL

)

select * from renamed
