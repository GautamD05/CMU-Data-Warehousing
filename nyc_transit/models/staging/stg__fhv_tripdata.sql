with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

renamed as (

    select
        trim(upper(dispatching_base_num)) as  dispatching_base_num, --some ids are lowercase
        pickup_datetime,  -- Pickup datetime
        dropoff_datetime,  -- Dropoff datetime
        pulocationid,  -- Pickup location ID
        dolocationid,  -- Dropoff location ID
        --sr_flag, always null so chuck it
        trim(upper(affiliated_base_number)) as affiliated_base_number,
        filename  -- Source file name

    from source

)

select * from renamed