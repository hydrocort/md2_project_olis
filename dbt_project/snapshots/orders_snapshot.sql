{% snapshot orders_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='order_id',
      strategy='timestamp',
      updated_at='_sdc_extracted_at'
    )
}}
SELECT * FROM {{ source(env_var('RAW_DATASET_NAME'), 'orders') }}
{% endsnapshot %}