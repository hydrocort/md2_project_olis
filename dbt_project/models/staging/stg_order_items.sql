with source as (
    select
        trim(order_id) as order_id,
        safe_cast(trim(order_item_id) as int64) as order_item_id,
        trim(product_id) as product_id,
        trim(seller_id) as seller_id,
        safe_cast(trim(shipping_limit_date) as timestamp) as shipping_limit_date,
        safe_cast(trim(price) as numeric) as price,
        safe_cast(trim(freight_value) as numeric) as freight_value
    from {{ ref('order_items_snapshot') }}
    where order_id is not null
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
from source