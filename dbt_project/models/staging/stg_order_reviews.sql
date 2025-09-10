with base as (
    select
        trim(review_id) as review_id,
        trim(order_id) as order_id,
        COALESCE(SAFE_CAST(NULLIF(trim(review_score), '') AS INT64), 0) as review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        SAFE_CAST(NULLIF(trim(review_creation_date), '') AS timestamp) as review_creation_date,
        SAFE_CAST(NULLIF(trim(review_answer_timestamp), '') AS timestamp) as review_answer_timestamp,

        SAFE_CAST(_sdc_extracted_at AS timestamp) as modified_at
    from {{ source(env_var('RAW_DATASET_NAME'), 'order_reviews') }}
),
ranked as (
  select *, row_number() over (partition by review_id, order_id order by modified_at desc) rn
  from base
)
select * except(rn) from ranked where rn=1