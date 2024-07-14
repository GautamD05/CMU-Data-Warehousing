with source as (

    select * from {{ source('main', 'yellow_tripdata') }}

),

renamed as (

    select
        vendorid,  -- A code indicating the LPEP provider that provided the record.
        tpep_pickup_datetime,  -- The date and time when the meter was engaged.
        tpep_dropoff_datetime,  -- The date and time when the meter was disengaged.
        passenger_count::int as passenger_count,  -- The number of passengers in the vehicle.
        trip_distance,  -- The elapsed trip distance in miles reported by the taximeter.
        ratecodeid,  -- The final rate code in effect at the end of the trip. 
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,  -- This flag indicates whether the trip record was held in vehicle memory before sending to the vendor
        pulocationid,  -- TLC Taxi Zone in which the taximeter was engaged.
        dolocationid,  -- TLC Taxi Zone in which the taximeter was disengaged.
        payment_type,  -- A numeric code signifying how the passenger paid for the trip. 
        fare_amount,  -- The time-and-distance fare calculated by the meter.
        extra,  -- Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges.
        mta_tax,  -- $0.50 MTA tax that is automatically triggered based on the metered rate in use.
        tip_amount,  -- This field is automatically populated for credit card tips. Cash tips are not included.
        tolls_amount,  -- Total amount of all tolls paid in trip.
        improvement_surcharge,  -- $0.30 improvement surcharge assessed trips at the flag drop.
        total_amount,  -- The total amount charged to passengers. Does not include cash tips.
        congestion_surcharge,  -- Total amount collected in trip for NYS congestion surcharge.
        airport_fee,  -- $1.25 for pick up only at LaGuardia and John F. Kennedy Airports.
        filename  -- Source file name

    from source
        WHERE tpep_pickup_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
          AND trip_distance >= 0 -- drop negative trip_distance
)

select * from renamed