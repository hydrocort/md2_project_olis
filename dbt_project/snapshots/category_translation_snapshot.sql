{% snapshot category_translation_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='product_category_name',
      strategy='check',
      check_cols=[
        'product_category_name_english'
      ]
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'product_category_name_translation') }}  
{% endsnapshot %}