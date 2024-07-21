with trips_renamed as
(
-- Select columns from the fhv trip data and label them as 'fhv'
	select 'fhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
	from {{ ref('stg__fhv_tripdata') }} 
	union all
-- Select columns from the FHVHV trip data and label them as 'fhvhv'
	select 'fhvhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
	from {{ ref('stg__fhvhv_tripdata') }} 
	union all
-- Select columns from the green trip data and label them as 'green'
	select 'green' as type, lpep_pickup_datetime, lpep_dropoff_datetime, pulocationid, dolocationid
	from {{ ref('stg__green_tripdata') }} 
	union all
-- Select columns from the yellow trip data and label them as 'green'
	select 'yellow' as type, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid
	from {{ ref('stg__yellow_tripdata') }} 
)

-- Select to calculate and output the unified data set
select 
	type, 
	pickup_datetime, 
	dropoff_datetime, 
	datediff('minute', pickup_datetime, dropoff_datetime) as duration_min, -- Calculate trip duration in minutes
	datediff('second', pickup_datetime, dropoff_datetime) as duration_sec, -- Calculate trip duration in seconds
	pulocationid, 
	dolocationid
from trips_renamed
