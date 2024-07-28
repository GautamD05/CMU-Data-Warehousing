select 
-- Extract the day of the week from the trip start timestamp
    weekday(started_at_ts) as weekday, 
-- Count the total number of trips for each day of the week
    count(*) as total_trips, 
-- Sum the total trip duration in seconds for each day of the week
    sum(duration_sec) as total_trip_duration_secs 
from {{ ref('mart__fact_all_bike_trips') }}
group by all;
