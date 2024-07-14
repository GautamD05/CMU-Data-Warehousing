with source as (

    select * from {{ source('main', 'green_tripdata') }}

),

renamed as (

    select
        vendorid,  -- A code indicating the LPEP provider that provided the record
        lpep_pickup_datetime,  -- The date and time when the meter was engaged
        lpep_dropoff_datetime,  -- The date and time when the meter was disengaged
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,  -- This flag indicates whether the trip record was held in vehicle memory before sending to the vendor.
        ratecodeid,  -- The final rate code in effect at the end of the trip.
        pulocationid,  -- TLC Taxi Zone in which the taximeter was engaged
        dolocationid,  -- TLC Taxi Zone in which the taximeter was disengaged
        passenger_count::int as passenger_count,  -- The number of passengers in the vehicle
        trip_distance,  -- The elapsed trip distance in miles reported by the taximeter
        fare_amount,  -- The time-and-distance fare calculated by the meter
        extra,  -- Miscellaneous extras and surcharges. 
        mta_tax,  -- $0.50 MTA tax that is automatically triggered based on the metered rate in use
        tip_amount,  -- This field is automatically populated for credit card tips. Cash tips are not included
        tolls_amount,  -- Total amount of all tolls paid in trip
        -- ehail_fee,  -- Removed due to 100% null source data
        improvement_surcharge,  -- $0.30 improvement surcharge assessed on hailed trips at the flag drop.
        total_amount,  -- The total amount charged to passengers. Does not include cash tips
        payment_type,  -- A numeric code signifying how the passenger paid for the trip. 
        trip_type,  -- A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver. 
        congestion_surcharge,  -- Total amount collected in trip for NYS congestion surcharge
        filename  -- Source file name

    from source
      WHERE lpep_pickup_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
        AND trip_distance >= 0 -- drop negative trip_distance
)

select * from renamed