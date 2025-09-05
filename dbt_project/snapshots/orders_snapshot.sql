{% snapshot orders_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='order_id',
      strategy='check',
      check_cols=['order_status']
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'orders') }}
WHERE order_id IS NOT NULL
{% endsnapshot %}