with source as (
    select
        trim(order_id) as order_id,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_sequential), '') AS INT64), 1) as payment_sequential,
        upper(trim(payment_type)) as payment_type,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_installments), '') AS INT64), 0) as payment_installments,
        COALESCE(SAFE_CAST(NULLIF(trim(payment_value), '') AS NUMERIC), 0) as payment_value
    from {{ ref('payments_snapshot') }}
    where order_id is not null
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
from source