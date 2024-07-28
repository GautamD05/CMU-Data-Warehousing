select 
	count(*) as total_no_valid_pickup_trips -- Count of unmatched trips
from {{ ref('mart__fact_all_taxi_trips') }} taxi -- Reference the fact table containing all taxi trips
left join {{ ref('mart__dim_locations') }} loc -- Reference the dimension table containing location details
on taxi.pulocationid = loc.LocationID
where loc.LocationID is null; -- Filter for trips where the pickup location ID is not found in the dimension table
