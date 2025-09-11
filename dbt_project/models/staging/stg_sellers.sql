with base as (
    select
        seller_id,
        trim(seller_zip_code_prefix) as seller_zip_prefix,
        trim(lower(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,
        
        safe_cast(_sdc_extracted_at as timestamp) as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'sellers') }}
),
ranked as (
  select *, row_number() over (partition by seller_id order by modified_at desc) rn
  from base
)
select * except(rn) from ranked where rn=1