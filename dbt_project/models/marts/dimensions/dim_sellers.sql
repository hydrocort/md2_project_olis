{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['seller_key'],
    unique_key='seller_key'
) }}
WITH seller_base AS (
  SELECT
    -- Primary key
    seller_id as seller_key,
    
    -- Location data (cleaned from staging)
    seller_city,
    seller_state,
    seller_zip_prefix,
    modified_at
    
  FROM {{ ref('stg_sellers') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

seller_regions AS (
  SELECT
    s.*,
    
    -- Regional classifications via seed table join
    COALESCE(r.region, 'Unknown') as seller_region,
    COALESCE(r.economic_zone, 'Unknown') as seller_economic_zone,
    
    -- Full state name for display
    COALESCE(r.state_name, s.seller_state) as seller_state_name
    
  FROM seller_base s
  LEFT JOIN {{ ref('brazil_state_regions') }} r
    ON UPPER(s.seller_state) = UPPER(r.state_code)
)

SELECT * FROM seller_regions