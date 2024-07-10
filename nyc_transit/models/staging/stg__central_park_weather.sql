with source as (
    -- Select all columns from the source table 'central_park_weather' in the 'main' schema
    select * from {{ source('main', 'central_park_weather') }}
),

-- Define the renamed CTE to cast and rename columns to appropriate data types
renamed as (
    select
        station as station_id,  -- Keep station as varchar but rename to station_id
        name as station_name,  -- Rename name to station_name
        date::date as date,  -- Convert date to date type
        awnd::double as average_wind_speed,  -- Convert awnd to double and rename to average_wind_speed
        prcp::double as precipitation,  -- Convert prcp to double and rename to precipitation
        snow::double as snowfall,  -- Convert snow to double and rename to snowfall
        snwd::double as snow_depth,  -- Convert snwd to double and rename to snow_depth
        tmax::int as max_temperature,  -- Convert tmax to integer and rename to max_temperature
        tmin::int as min_temperature  -- Convert tmin to integer and rename to min_temperature
    from source
)

-- Select all columns from the renamed CTE
select * from renamed
