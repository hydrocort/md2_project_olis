{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['review_key'],
    unique_key='review_key'
) }}
WITH review_base AS (
  SELECT
    -- Primary key
    order_id as review_key,
    
    -- Review attributes (already cleaned in staging)
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    modified_at
    
  FROM {{ ref('stg_order_reviews') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

-- Deduplicate reviews: keep most recent review per order
deduplicated_reviews AS (
  SELECT 
    review_key,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    modified_at,
    -- Use ROW_NUMBER to rank reviews by creation date (most recent first)
    ROW_NUMBER() OVER (
      PARTITION BY review_key 
      ORDER BY review_creation_date DESC, review_score DESC
    ) as rn
  FROM review_base
),

unique_reviews AS (
  SELECT 
    review_key,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    modified_at
  FROM deduplicated_reviews
  WHERE rn = 1  -- Keep only the most recent review per order
),

review_metrics AS (
  SELECT
    r.*,
    
    -- Comment availability flags (TRUE if comment exists, FALSE if empty/null)
    CASE 
      WHEN review_comment_title IS NOT NULL AND review_comment_title != '' THEN TRUE
      ELSE FALSE
    END as has_comment_title,
    
    CASE 
      WHEN review_comment_message IS NOT NULL AND review_comment_message != '' THEN TRUE
      ELSE FALSE
    END as has_comment_message,
    
    -- Review timing: days from order purchase to review creation
    CASE 
      WHEN review_creation_date IS NOT NULL 
        AND EXISTS (
          SELECT 1 FROM {{ ref('stg_orders') }} o 
          WHERE o.order_id = r.review_key 
          AND o.order_purchase_timestamp IS NOT NULL
        )
      THEN (
        SELECT DATE_DIFF(
          DATE(r.review_creation_date), 
          DATE(o.order_purchase_timestamp), 
          DAY
        )
        FROM {{ ref('stg_orders') }} o 
        WHERE o.order_id = r.review_key
      )
      ELSE NULL
    END as days_to_review
    
  FROM unique_reviews r  -- Use deduplicated reviews instead of review_base
)

SELECT * FROM review_metrics