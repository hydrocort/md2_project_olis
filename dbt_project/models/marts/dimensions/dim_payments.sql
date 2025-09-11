{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['payment_key'],
    unique_key='payment_key',
) }}
WITH payment_aggregation AS (
  SELECT
    -- Primary key (one record per order)
    order_id as payment_key,
    
    -- Payment method flags: MAX() returns TRUE if ANY payment in order uses this method
    MAX(CASE WHEN payment_type = 'credit_card' THEN TRUE ELSE FALSE END) as uses_credit_card,
    MAX(CASE WHEN payment_type = 'boleto' THEN TRUE ELSE FALSE END) as uses_boleto,
    MAX(CASE WHEN payment_type = 'voucher' THEN TRUE ELSE FALSE END) as uses_voucher,
    MAX(CASE WHEN payment_type = 'debit_card' THEN TRUE ELSE FALSE END) as uses_debit_card,
    
    -- Primary payment type: ARRAY_AGG collects payment types, takes 1st by sequence, extracts value
    ARRAY_AGG(payment_type ORDER BY payment_sequential LIMIT 1)[OFFSET(0)] as primary_payment_type,
    
    -- Payment aggregation metrics
    SUM(payment_value) as total_payment_value,
    SUM(payment_installments) as total_installments,
    COUNT(DISTINCT payment_type) as payment_methods_count,
    COUNT(*) as payment_transactions_count,
    MAX(modified_at) as modified_at
    
  FROM {{ ref('stg_payments') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
  GROUP BY order_id
)

SELECT * FROM payment_aggregation