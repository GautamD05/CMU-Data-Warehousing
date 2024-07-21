select 
	started_at_ts, 
	ended_at_ts, 
	datediff('minute', started_at_ts, ended_at_ts) as duration_min, -- Calculate the trip duration in minutes
	datediff('second', started_at_ts, ended_at_ts) as duration_sec, -- Calculate the trip duration in seconds
	start_station_id,
	end_station_id
from {{ ref('stg__bike_data') }}