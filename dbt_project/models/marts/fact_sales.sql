{{ config(materialized='table') }}

-- Cast & sanitize at the edges, then aggregate
WITH order_items_typed AS (
  SELECT
    i.order_id,
    -- strip anything that's not digit, dot, or minus, then cast
    COALESCE(SAFE_CAST(REGEXP_REPLACE(i.price,          r'[^0-9.\-]', '') AS NUMERIC), 0) AS price_num,
    COALESCE(SAFE_CAST(REGEXP_REPLACE(i.freight_value,  r'[^0-9.\-]', '') AS NUMERIC), 0) AS freight_num
  FROM {{ ref('stg_order_items') }} i
),
order_totals AS (
  SELECT
    order_id,
    SUM(price_num + freight_num) AS order_amount
  FROM order_items_typed
  GROUP BY order_id
),
payments_typed AS (
  SELECT
    p.order_id,
    COALESCE(SAFE_CAST(REGEXP_REPLACE(p.payment_value, r'[^0-9.\-]', '') AS NUMERIC), 0) AS payment_num
  FROM {{ ref('stg_payments') }} p
),
payments AS (
  SELECT
    order_id,
    SUM(payment_num) AS total_payment
  FROM payments_typed
  GROUP BY order_id
),
orders_typed AS (
  SELECT
    o.order_id,
    o.customer_id,
    -- If order_ts is already a TIMESTAMP/DATE, this DATE() is fine.
    -- If it's a string, this SAFE.PARSE handles it; adjust the format if needed.
    CASE
      WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(o.order_ts AS STRING)) IS NOT NULL
        THEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(o.order_ts AS STRING)))
      ELSE DATE(o.order_ts)  -- works when order_ts is TIMESTAMP/DATE
    END AS order_date
  FROM {{ ref('stg_orders') }} o
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  ot.order_amount,
  p.total_payment
FROM orders_typed o
JOIN order_totals ot ON o.order_id = ot.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
