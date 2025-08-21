
{{ config(materialized='table') }}
SELECT DISTINCT
    DATE(order_ts) AS date,
    EXTRACT(YEAR FROM order_ts) AS year,
    EXTRACT(MONTH FROM order_ts) AS month,
    EXTRACT(DAY FROM order_ts) AS day,
    EXTRACT(DAYOFWEEK FROM order_ts) AS day_of_week
FROM {{ ref('stg_orders') }}
