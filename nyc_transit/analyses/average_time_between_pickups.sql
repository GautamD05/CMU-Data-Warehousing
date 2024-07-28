-- Calculates the time difference to the previous pickup for each trip within the same zone
with prev_pickup as (
     select
        zone, 
        datediff('second', lag(pickup_datetime) 
        over (partition by zone order by pickup_datetime), pickup_datetime) as prev_pickup_time
     from {{ ref('mart__fact_all_taxi_trips') }} taxi
     join {{ ref('mart__dim_locations') }} loc on taxi.pulocationid = loc.locationid)
-- Calculates the average time difference between pickups per zone
select 
    zone, 
    avg(prev_pickup_time) as average_time_between_pickups
from prev_pickup
group by zone
order by zone; -- ordered by zone
