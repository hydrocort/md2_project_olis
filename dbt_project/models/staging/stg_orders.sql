with source as (
    select
        trim(order_id) as order_id,
        trim(customer_id) as customer_id,
        lower(trim(order_status)) as order_status,
        safe_cast(trim(order_purchase_timestamp) as timestamp) as order_purchase_timestamp,
        safe_cast(trim(order_approved_at) as timestamp) as order_approved_at,
        safe_cast(trim(order_delivered_carrier_date) as timestamp) as order_delivered_carrier_date,
        safe_cast(trim(order_delivered_customer_date) as timestamp) as order_delivered_customer_date,
        safe_cast(trim(order_estimated_delivery_date) as timestamp) as order_estimated_delivery_date
    from {{ source(env_var('RAW_DATASET_NAME'), 'orders') }}
    where order_id is not null
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
from source