WITH customer_base AS (
  SELECT
    -- Primary key
    customer_id as customer_key,
    
    -- Customer identifiers
    customer_unique_id,
    
    -- Location data (cleaned from staging)
    customer_city,
    customer_state,
    customer_zip_prefix
    
  FROM {{ ref('stg_customers') }}
  WHERE customer_id IS NOT NULL
),

customer_regions AS (
  SELECT
    c.*,
    
    -- Regional classifications via seed table join
    COALESCE(r.region, 'Unknown') as customer_region,
    COALESCE(r.economic_zone, 'Unknown') as customer_economic_zone,
    
    -- Full state name for display
    COALESCE(r.state_name, c.customer_state) as customer_state_name
    
  FROM customer_base c
  LEFT JOIN {{ ref('brazil_state_regions') }} r
    ON UPPER(c.customer_state) = UPPER(r.state_code)
)

SELECT * FROM customer_regions
ORDER BY customer_key