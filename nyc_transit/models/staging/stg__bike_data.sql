-- Define the source CTE (Common Table Expression)
with source as (
    -- Select all columns from the source table 'bike_data' in the 'main' schema
    select 
        tripduration,
        starttime,
        stoptime,
        "start station id" as start_station_id,
        start_station_name,
        "start station latitude" as start_station_latitude,
        "start station longitude" as start_station_longitude,
        "end station id" as end_station_id,
        end_station_name,
        "end station latitude" as end_station_latitude,
        "end station longitude" as end_station_longitude,
        bikeid,
        usertype,
        "birth year" as birth_year,
        gender,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from {{ source('main', 'bike_data') }}
),

-- Define the renamed CTE to cast and rename columns to appropriate data types
renamed as (
    select
        tripduration::int as tripduration,  -- Convert tripduration to integer
        coalesce(starttime::timestamp, started_at::timestamp) as starttime,  -- Merge starttime and started_at columns as timestamp
        coalesce(stoptime::timestamp, ended_at::timestamp) as stoptime,    -- Merge stoptime and ended_at columns as timestamp
        coalesce(start_station_id::int, start_station_id::int) as start_station_id,  -- Merge start_station_id columns as integer
        start_station_name,  -- Keep start_station_name as varchar
        coalesce(start_station_latitude::double, start_lat::double) as start_station_latitude,  -- Merge start_station_latitude columns as double
        coalesce(start_station_longitude::double, start_lng::double) as start_station_longitude,  -- Merge start_station_longitude columns as double
        coalesce(end_station_id::int, end_station_id::int) as end_station_id,  -- Merge end_station_id columns as integer
        end_station_name,  -- Keep end_station_name as varchar
        coalesce(end_station_latitude::double, end_lat::double) as end_station_latitude,  -- Merge end_station_latitude columns as double
        coalesce(end_station_longitude::double, end_lng::double) as end_station_longitude,  -- Merge end_station_longitude columns as double
        bikeid::int as bikeid,  -- Convert bikeid to integer
        usertype,  -- Keep usertype as varchar
        birth_year::int as birth_year,  -- Convert birth_year to integer
        gender::int as gender,  -- Convert gender to integer
        ride_id::varchar as ride_id,  -- Convert ride_id to varchar
        rideable_type,  -- Keep rideable_type as varchar
        member_casual  -- Keep member_casual as varchar
    from source
)

-- Select all columns from the renamed CTE
select * from renamed
