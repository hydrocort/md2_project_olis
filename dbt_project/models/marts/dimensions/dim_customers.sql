{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['customer_key'],
    unique_key='customer_key'
) }}

WITH customer_base AS (
  SELECT
    -- Primary key (using customer_unique_id as the unique identifier)
    customer_unique_id as customer_key,
    
    -- Customer identifiers (take any customer_id for each unique customer)
    ANY_VALUE(customer_id) as customer_id_original,

    -- Location data (cleaned from staging - should be consistent for each customer)
    ANY_VALUE(customer_city) as customer_city,
    ANY_VALUE(customer_state) as customer_state,
    ANY_VALUE(customer_zip_prefix) as customer_zip_prefix,
    MAX(modified_at) as modified_at

  FROM {{ ref('stg_customers') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
  GROUP BY customer_unique_id
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