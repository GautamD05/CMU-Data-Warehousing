-- pivot operation on the table `mart__fact_trips_by_borough`
pivot {{ ref('mart__fact_trips_by_borough') }} 
-- column to pivot on
on borough 
-- Use the first value of the trips column as the aggregate function
using first(trips);