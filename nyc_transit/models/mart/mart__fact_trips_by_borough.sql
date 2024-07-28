select 
-- Selects borough name from the locations dimension table, Count of trips per borough
   loc.borough, count(*) as trips
from {{ ref('mart__fact_all_taxi_trips') }} taxi  -- fact table containing all taxi trips
join {{ ref('mart__dim_locations') }} loc on taxi.pulocationid = loc.locationid  -- Join with the locations dimension table
group by all -- Group by borough to get the trip count per borough