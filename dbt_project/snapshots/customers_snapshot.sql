{% snapshot customers_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'customer_city', 'customer_state', 'customer_zip_prefix'] 
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'customers') }} 
WHERE customer_id IS NOT NULL
{% endsnapshot %}