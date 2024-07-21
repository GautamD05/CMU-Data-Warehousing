-- Select all columns from the 'taxi_zone_lookup' table
select
  {{ dbt_utils.star(ref('taxi+_zone_lookup')) }}
from {{ ref('taxi+_zone_lookup') }}