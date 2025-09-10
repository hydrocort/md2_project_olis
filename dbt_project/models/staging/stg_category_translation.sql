with base as (
    SELECT 
    product_category_name, 
    product_category_name_english,

    _sdc_extracted_at as modified_at
    FROM {{ source(env_var('RAW_DATASET_NAME'), 'product_category_name_translation') }}
),
ranked as (
  select *, row_number() over (partition by product_category_name order by modified_at desc) rn
  from base
)
select * except(rn) from ranked where rn=1