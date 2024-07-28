with prcp_on_any_day as (
    select
        date,
        (prcp + snow) > 0 as prcp_flag --falg set to true if either prcp or snow is greater than 0
    from {{ ref('stg__central_park_weather') }}
),
-- Calculate trips and next day precipitation flag
trips_with_precipitation as (
    select
        prec.date,
        prcp_flag,
        lead(prcp_flag) over (order by prec.date) as prcp_flag_next_day, -- get precipitation flag for the next day
        t.trips as trips_current -- Trips for the current day
    from prcp_on_any_day prec
    join {{ ref('mart__fact_all_trips_daily') }} t on prec.date = t.date and t.type = 'bike'
)
select
-- calculate average trips for days with precipitation and days preceding precipitation
    avg(case when prcp_flag then trips_current end) as avg_trips_on_precipitation_days,
    avg(case when prcp_flag_next_day then trips_current end) as avg_trips_on_days_preceding_precipitation
from trips_with_precipitation;
