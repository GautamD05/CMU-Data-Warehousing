with source as (
    select * from {{ source('main', 'fhv_tripdata') }}
),
 -- Keep all the columns datatype as is and drop SR_Flag since it has null for all records
renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID,
        DOlocationID,
        Affiliated_base_number,
        filename
    from source
)
select * from renamed