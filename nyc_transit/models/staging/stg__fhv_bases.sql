with source as (
    -- Select all columns from the source table 'fhv_bases' in the 'main' schema
    select * from {{ source('main', 'fhv_bases') }}
),

-- Define the renamed CTE to cast and rename columns to appropriate data types
renamed as (
    select
        base_number as base_number,  -- Keep base_number as it is
        base_name as base_name,  -- Keep base_name as it is
        dba as doing_business_as,  -- Rename dba to doing_business_as for better readability
        dba_category as dba_category  -- Keep dba_category as it is
    from source
)

-- Select all columns from the renamed CTE
select * from renamed
