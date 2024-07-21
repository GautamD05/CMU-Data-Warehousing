select 
    count(*) as total_trips -- Calculate the total number of trips
from {{ ref('mart__fact_all_taxi_trips') }} taxi
join {{ ref('mart__dim_locations') }} loc
on taxi.DOlocationID = loc.LocationID
where loc.service_zone in ('Airports', 'EWR') -- Filter the results to include only trips ending in 'Airports' or 'EWR' service zones
group by all
