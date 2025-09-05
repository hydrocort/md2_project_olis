{% snapshot products_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='product_id',
      strategy='check',
      check_cols=[
        'product_category_name',
        'product_name_length',
        'product_description_length',
        'product_photos_qty',
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
      ]
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'products') }} 
WHERE product_id IS NOT NULL
{% endsnapshot %}