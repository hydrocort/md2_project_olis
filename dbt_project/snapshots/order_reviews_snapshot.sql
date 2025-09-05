{% snapshot order_reviews_snapshot %}
{{
    config(
      target_schema=env_var('SNAPSHOT_DATASET_NAME'),
      unique_key='review_id',
      strategy='check',
      check_cols=[
        'order_id',
        'review_score',
        'review_comment_title',
        'review_comment_message',
        'review_creation_date',
        'review_answer_timestamp'
      ]
    )
}}
SELECT *
FROM {{ source(env_var('RAW_DATASET_NAME'), 'order_reviews') }}
WHERE review_id IS NOT NULL AND order_id IS NOT NULL
{% endsnapshot %}