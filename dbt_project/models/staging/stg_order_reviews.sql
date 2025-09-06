with source as (
    select
        trim(review_id) as review_id,
        trim(order_id) as order_id,
        COALESCE(SAFE_CAST(NULLIF(trim(review_score), '') AS INT64), 0) as review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        SAFE_CAST(NULLIF(trim(review_creation_date), '') AS timestamp) as review_creation_date,
        SAFE_CAST(NULLIF(trim(review_answer_timestamp), '') AS timestamp) as review_answer_timestamp
    from {{ ref('order_reviews_snapshot') }}
    where review_id is not null
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
from source