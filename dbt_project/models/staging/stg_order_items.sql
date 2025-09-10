with base as (
    select
        trim(order_id) as order_id,
        safe_cast(trim(order_item_id) as int64) as order_item_id,
        trim(product_id) as product_id,
        trim(seller_id) as seller_id,
        safe_cast(trim(shipping_limit_date) as timestamp) as shipping_limit_date,
        safe_cast(trim(price) as numeric) as price,
        safe_cast(trim(freight_value) as numeric) as freight_value,

        _sdc_extracted_at as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'order_items') }}
    where order_id is not null
),
ranked as (
  select *, row_number() over (partition by order_id, order_item_id order by modified_at desc) rn
  from base
)

select * except(rn) from ranked where rn=1