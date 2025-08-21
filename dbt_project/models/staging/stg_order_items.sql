
{{ config(materialized='view') }}
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM {{ source('raw', 'order_items') }}
