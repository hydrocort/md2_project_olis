with source as (
    select
        seller_id,
        trim(seller_zip_code_prefix) as seller_zip_prefix,
        trim(lower(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,
        -- Remove Meltano metadata columns
        -- _sdc_extracted_at, _sdc_table_version
    from {{ source('olis_raw_dataset', 'sellers') }}
    where seller_id is not null
)

select
    seller_id,
    seller_zip_prefix,
    seller_city,
    seller_state
from source

