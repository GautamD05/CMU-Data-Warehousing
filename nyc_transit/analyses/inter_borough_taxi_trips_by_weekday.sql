with all_taxi_trips as (
-- Calculate total trips by weekday, excluding trips with null pickup or dropoff locations
    select 
        weekday(pickup_datetime) as weekday, 
        count(*) as total_trips
    from {{ ref('mart__fact_all_taxi_trips') }} taxi
    where PUlocationID is not null and DOlocationID is not null
    group by weekday
),
diff_borough as (
-- Calculate different borough trips by weekday 
    select 
        weekday(pickup_datetime) as weekday, 
        count(*) as diff_borough_trips
    from {{ ref('mart__fact_all_taxi_trips') }} taxi
    join {{ ref('mart__dim_locations') }} ploc on taxi.PUlocationID = ploc.LocationID
    join {{ ref('mart__dim_locations') }} dloc on taxi.DOlocationID = dloc.LocationID
    where ploc.borough != dloc.borough
    group by weekday
)
-- Select trips by weekday and calculate percentage
select 
    all_taxi_trips.weekday,
    all_taxi_trips.total_trips,
-- Calculate number of  trips starting and ending in a different borough and percentage of trips with different start/end
-- coalesce to handle cases where there might be no inter-borough trips for a given weekday
    coalesce(diff_borough.diff_borough_trips, 0) as different_borough_trips,
    (coalesce(diff_borough.diff_borough_trips, 0) / all_taxi_trips.total_trips::float) * 100 || '%' as percent_different_borough
from all_taxi_trips
-- Left join to ensure that all weekdays from all_taxi_trips are included even if there are no corresponding  different borough trips for that weekday
left join diff_borough on all_taxi_trips.weekday = diff_borough.weekday
order by all_taxi_trips.weekday;
