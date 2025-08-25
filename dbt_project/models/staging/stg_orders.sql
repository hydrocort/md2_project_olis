
{{ config(materialized='view') }}
SELECT
    order_id,
    customer_id,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_ts,
    CAST(order_approved_at AS TIMESTAMP) AS approved_ts,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_ts,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
FROM {{ source('olis_raw_dataset', 'orders') }}
