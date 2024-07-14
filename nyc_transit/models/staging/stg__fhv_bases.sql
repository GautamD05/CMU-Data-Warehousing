with source as (

    select * from {{ source('main', 'fhv_bases') }}

),

renamed as (

    select
        -- clean up the base_num to be properly linked as foreign keys
        trim(upper(base_number)) as base_number,
        base_name,  -- Base name
        dba,  -- Doing business as name
        dba_category,  -- DBA category
        filename  -- Source file name

    from source

)

select * from renamed
