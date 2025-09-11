with base as (
    select
        customer_id,
        customer_unique_id,
        trim(lower(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        trim(customer_zip_code_prefix) as customer_zip_prefix,

        safe_cast(_sdc_extracted_at as timestamp) as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'customers') }} 
),
ranked as (
  select *, row_number() over (partition by customer_id order by modified_at desc) rn
  from base
)

select * except(rn) from ranked where rn=1