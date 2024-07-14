with source as (

    select * from {{ source('main', 'central_park_weather') }}

),

renamed as (

    select
        station,  -- Weather station ID
        name,  -- Weather station name
        date::date as date,  -- Observation date
        awnd::double as awnd,  -- Average wind speed
        prcp::double as prcp,  -- Precipitation
        snow::double as snow,  -- Snowfall
        snwd::double as snwd,  -- Snow depth
        tmax::int as tmax,  -- Maximum temperature
        tmin::int as tmin,  -- Minimum temperature
        filename  -- Source file name

    from source

)

select
    station,
    name, 
    date,
    awnd,
    prcp,
    snow,
    snwd,
    tmax,
    tmin,
    filename
from renamed