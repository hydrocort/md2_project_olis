with base as (
    select
        trim(o.order_id) as order_id,
        trim(o.customer_id) as customer_id,
        upper(trim(o.order_status)) as order_status,
        safe_cast(trim(o.order_purchase_timestamp) as timestamp) as order_purchase_timestamp,
        safe_cast(trim(o.order_approved_at) as timestamp) as order_approved_at,
        safe_cast(trim(o.order_delivered_carrier_date) as timestamp) as order_delivered_carrier_date,
        safe_cast(trim(o.order_delivered_customer_date) as timestamp) as order_delivered_customer_date,
        safe_cast(trim(o.order_estimated_delivery_date) as timestamp) as order_estimated_delivery_date,

        safe_cast(o._sdc_extracted_at as timestamp) as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'orders') }} o
),
ranked as (
  select *,
    row_number() over (partition by order_id order by modified_at desc) as rn
  from base
)

select * except(rn)
from ranked
where rn = 1