with base as (
    select
        trim(order_id) as order_id,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_sequential), '') AS INT64), 1) as payment_sequential,
        upper(trim(payment_type)) as payment_type,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_installments), '') AS INT64), 0) as payment_installments,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_value), '') AS NUMERIC), 0) as payment_value,

        safe_cast(_sdc_extracted_at as timestamp) as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'payments') }}
),
ranked as (
  select *, row_number() over (partition by order_id, payment_sequential order by modified_at desc) rn
  from base
)
select * except(rn) from ranked where rn=1