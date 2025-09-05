{% snapshot order_items_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='order_item_id',
      strategy='check',
      check_cols=[
        'order_id',
        'product_id',
        'seller_id',
        'shipping_limit_date',
        'price',
        'freight_value'
      ] 
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'order_items') }} 
{% endsnapshot %}