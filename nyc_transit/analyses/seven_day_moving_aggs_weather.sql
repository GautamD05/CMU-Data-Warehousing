select date,
-- Calculate the minimum, maximum, average, sum precipitation and snow over a 7-day window
  min(prcp) over seven_day as seven_day_moving_min_prcp,
  max(prcp) over seven_day as seven_day_moving_max_prcp,
  avg(prcp) over seven_day as seven_day_moving_avg_prcp,
  sum(prcp) over seven_day as seven_day_moving_sum_prcp,
  min(snow) over seven_day as seven_day_moving_min_snow,
  max(snow) over seven_day as seven_day_moving_max_snow,
  avg(snow) over seven_day as seven_day_moving_avg_snow,
  sum(snow) over seven_day as seven_day_moving_sum_snow
from {{ ref('stg__central_park_weather') }} 
WINDOW seven_day as (
  order by date asc
  -- Define the window as 3 days before and 3 days after the current date
  range between INTERVAL 3 DAYS PRECEDING and INTERVAL 3 DAYS FOLLOWING)
order by date;