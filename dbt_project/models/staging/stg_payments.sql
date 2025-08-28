with source as (
    select
        trim(order_id) as order_id,
        safe_cast(trim(payment_sequential) as int64) as payment_sequential,
        lower(trim(payment_type)) as payment_type,
        safe_cast(trim(payment_installments) as int64) as payment_installments,
        safe_cast(trim(payment_value) as numeric) as payment_value
    from {{ source('olis_raw_dataset', 'payments') }}
    where order_id is not null
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
from source