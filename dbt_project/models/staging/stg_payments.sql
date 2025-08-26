
{{ config(materialized='view') }}
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('raw', 'payments') }}
