with events as (
  select
  -- Convert string timestamps to actual timestamp data types
	strptime(insert_timestamp, '%d/%m/%Y %H:%M') as insert_timestamp,
    event_id,
    event_type,
    user_id,
	-- Convert string timestamps to actual timestamp data types
    strptime(event_timestamp, '%d/%m/%Y %H:%M') as event_timestamp
  from {{ ref('events') }})
select *
-- Use QUALIFY to filter rows keeping only the latest entry per event_id based on insert_timestamp
from events qualify row_number() over (partition by event_id order by insert_timestamp desc) = 1;