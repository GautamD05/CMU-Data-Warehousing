with source as (
    select * from {{ source('main', 'green_tripdata') }}
),
-- Keep all the columns datatype as is and drop ehail_fee since it has null for all records
renamed as (
    select
        VendorID,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID,
        PUlocationID,
        DOlocationID,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename
    from source
)

select * from renamed

