{% snapshot sellers_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='seller_id',
      strategy='check',
      check_cols=[
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state'
      ]
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'sellers') }} 
{% endsnapshot %}