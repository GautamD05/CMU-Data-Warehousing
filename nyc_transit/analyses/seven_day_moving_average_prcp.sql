with weather_data as (
    select 
        date,
        prcp,
	-- Extract precipitation values for the previous and next 3 days using lag and lead functions
        lag(prcp, 3) over (order by date) as prcp_3_days_ago,
        lag(prcp, 2) over (order by date) as prcp_2_days_ago,
        lag(prcp, 1) over (order by date) as prcp_1_day_ago,
        lead(prcp, 1) over (order by date) as prcp_1_day_ahead,
        lead(prcp, 2) over (order by date) as prcp_2_days_ahead,
        lead(prcp, 3) over (order by date) as prcp_3_days_ahead
    from {{ ref('stg__central_park_weather') }})
select 
    date,
    prcp,
    (
	-- coalesce to replace NULL values with 0 for each precipitation value
        coalesce(prcp, 0) +
        coalesce(prcp_3_days_ago, 0) + 
        coalesce(prcp_2_days_ago, 0) + 
        coalesce(prcp_1_day_ago, 0) + 
        coalesce(prcp_1_day_ahead, 0) + 
        coalesce(prcp_2_days_ahead, 0) + 
        coalesce(prcp_3_days_ahead, 0)
    ) / (
	-- case to count only non null values
        1 + -- Current day
        case when prcp_3_days_ago is not null then 1 else 0 end + 
        case when prcp_2_days_ago is not null then 1 else 0 end + 
        case when prcp_1_day_ago is not null then 1 else 0 end + 
        case when prcp_1_day_ahead is not null then 1 else 0 end + 
        case when prcp_2_days_ahead is not null then 1 else 0 end + 
        case when prcp_3_days_ahead is not null then 1 else 0 end)
		as seven_day_moving_avg_prec
from weather_data
order by date; -- -- order by date to maintain chronological order
