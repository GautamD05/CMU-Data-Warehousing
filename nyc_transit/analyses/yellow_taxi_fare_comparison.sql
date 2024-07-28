select 
  fare_amount,
  zone,
-- Calculate the average fare amount partitioned by zone
  avg(fare_amount) over (partition by zone) zone_average_fare, 
  borough,
-- Calculate the average fare amount partitioned by borough
  avg(fare_amount) over (partition by borough) borough_average_fare
from {{ ref('stg__yellow_tripdata') }} yellow
join {{ ref('mart__dim_locations') }} loc on yellow.PUlocationID = loc.locationid;