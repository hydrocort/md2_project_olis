{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['order_key'],
    unique_key='order_key'
) }}
WITH order_base AS (
  SELECT
    -- Primary key
    order_id as order_key,
    
    -- Order attributes (already cleaned in staging)
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    modified_at
    
  FROM {{ ref('stg_orders') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

order_metrics AS (
  SELECT
    *,
    
    -- Total days from purchase to delivery (NULL if missing dates)
    CASE 
      WHEN order_delivered_customer_date IS NOT NULL 
        AND order_purchase_timestamp IS NOT NULL
      THEN DATE_DIFF(
        DATE(order_delivered_customer_date), 
        DATE(order_purchase_timestamp), 
        DAY
      )
      ELSE NULL
    END as days_to_delivery,
    
    -- Days early/late vs estimated delivery (+ = late, - = early, 0 = on time)
    CASE 
      WHEN order_delivered_customer_date IS NOT NULL 
        AND order_estimated_delivery_date IS NOT NULL
      THEN DATE_DIFF(
        DATE(order_delivered_customer_date), 
        DATE(order_estimated_delivery_date), 
        DAY
      )
      ELSE NULL
    END as delivery_vs_estimate_days,
    
    -- TRUE = on time/early, FALSE = late, NULL = can't determine
    CASE 
      WHEN order_delivered_customer_date IS NOT NULL 
        AND order_estimated_delivery_date IS NOT NULL
        AND DATE_DIFF(
          DATE(order_delivered_customer_date), 
          DATE(order_estimated_delivery_date), 
          DAY
        ) <= 0
      THEN TRUE
      WHEN order_delivered_customer_date IS NOT NULL 
        AND order_estimated_delivery_date IS NOT NULL
      THEN FALSE
      ELSE NULL
    END as is_delivered_on_time
    
  FROM order_base
)

SELECT * FROM order_metrics