-- Aggregate trip data by day and type, and calculate the number of trips and the average trip duration
select
	type,
	date_trunc('day', started_at_ts)::date as date, -- Truncate the start timestamp to the day and cast to a date
	count(*) as trips, -- Count the number of trips for each type and day
	avg(duration_min) as average_trip_duration_min -- Calculate the average trip duration in minutes for each type and day
from {{ ref('mart__fact_all_trips') }}
group by all