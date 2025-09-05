{% snapshot payments_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key=['order_id', 'payment_sequential'],
      strategy='check',
      check_cols=[
        'payment_type',
        'payment_installments',
        'payment_value'
      ]
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'payments') }}
WHERE order_id IS NOT NULL AND payment_sequential IS NOT NULL
{% endsnapshot %}