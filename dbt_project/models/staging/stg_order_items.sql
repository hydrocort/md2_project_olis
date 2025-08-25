
{{ config(materialized='view') }}
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM {{ source('olis_raw_dataset', 'order_items') }}
