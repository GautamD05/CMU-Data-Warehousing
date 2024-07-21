-- Create a dimension table for bike stations distinct bike stations (start and end) with their respective IDs, names, latitudes, and longitudes
with all_stations as (
-- Select unique starting stations
    select
        start_station_id as station_id, 
        start_station_name as station_name,
        start_lat as station_lat, 
        start_lng as station_lng
    from {{ ref('stg__bike_data') }}
    union all
-- Select unique ending stations
    select
        end_station_id as station_id, 
        end_station_name as station_name,
        end_lat as station_lat, 
        end_lng as station_lng
    from {{ ref('stg__bike_data') }}
)
select 
    station_id, -- The unique identifier for each station
    max(station_name) as station_name, -- Since the names for a given station_id should be the same MAX will return the name
    avg(station_lat) as station_lat, -- Average latitude to resolve discrepancies
    avg(station_lng) as station_lng -- Average longitude to resolve discrepancies
from all_stations
where station_id is not null
group by station_id
