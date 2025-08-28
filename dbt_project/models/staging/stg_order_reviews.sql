with source as (
    select
        trim(review_id) as review_id,
        trim(order_id) as order_id,
        safe_cast(trim(review_score) as int64) as review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        safe_cast(trim(review_creation_date) as timestamp) as review_creation_date,
        safe_cast(trim(review_answer_timestamp) as timestamp) as review_answer_timestamp
    from {{ source('olis_raw_dataset', 'order_reviews') }}
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