with trips_renamed as
(
-- Select columns from the bike data and label them as 'bike'
	select 'bike' as type, started_at_ts, ended_at_ts from {{ ref('stg__bike_data') }} 
	union all
-- Select columns from the fhv trip data and label them as 'fhv'
	select 'fhv' as type, pickup_datetime, dropoff_datetime from {{ ref('stg__fhv_tripdata') }} 
	union all
-- Select columns from the fhvhv trip data and label them as 'fhvhv'
	select 'fhvhv' as type, pickup_datetime, dropoff_datetime from {{ ref('stg__fhvhv_tripdata') }} 
	union all
-- Select columns from the green trip data and label them as 'green'
	select 'green' as type, lpep_pickup_datetime, lpep_dropoff_datetime from {{ ref('stg__green_tripdata') }} 
	union all
-- Select columns from the yellow trip data and label them as 'yellow'
	select 'yellow' as type, tpep_pickup_datetime, tpep_dropoff_datetime from {{ ref('stg__yellow_tripdata') }} 
)

select 
	type, 
	started_at_ts, 
	ended_at_ts, 
	datediff('minute', started_at_ts, ended_at_ts) as duration_min, -- Calculate trip duration in minutes
	datediff('second', started_at_ts, ended_at_ts) as duration_sec  -- Calculate trip duration in seconds
from trips_renamed
