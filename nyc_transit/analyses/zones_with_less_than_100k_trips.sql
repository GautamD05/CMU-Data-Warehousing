select 
	Zone, 
	count(*) as trips  -- Count the total number of trips for each zone
from {{ ref('mart__fact_all_taxi_trips') }} taxi 
join {{ ref('mart__dim_locations') }} loc on taxi.pulocationid = loc.locationid 
group by all 
having trips < 100000; -- Filter to include only zones with less than 100000 trips
