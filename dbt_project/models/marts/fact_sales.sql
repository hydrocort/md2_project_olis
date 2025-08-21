
{{ config(materialized='table') }}

WITH order_totals AS (
    SELECT
        i.order_id,
        SUM(i.price + i.freight_value) AS order_amount
    FROM {{ ref('stg_order_items') }} i
    GROUP BY i.order_id
),
payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    DATE(o.order_ts) AS order_date,
    ot.order_amount,
    p.total_payment
FROM {{ ref('stg_orders') }} o
JOIN order_totals ot ON o.order_id = ot.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
