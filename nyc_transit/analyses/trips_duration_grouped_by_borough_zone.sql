select 
  borough, 
  zone, 
  count(*) as trips,  -- Count the total number of trips by zone and borough
  avg(duration_min) avg_duration_mins -- Calculate the average trip duration in minutes for trips by zone and borough
from {{ ref('mart__fact_all_taxi_trips') }} taxi
join {{ ref('mart__dim_locations') }} loc on taxi.DOlocationID = loc.LocationID
group by all; -- Group the results by borough and zone to get the count of trips and average duration for each