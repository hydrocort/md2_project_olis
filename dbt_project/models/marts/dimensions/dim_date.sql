WITH order_dates AS (
  SELECT DISTINCT
    DATE(order_purchase_timestamp) as full_date
  FROM {{ ref('stg_orders') }}
  WHERE order_purchase_timestamp IS NOT NULL
  {% if is_incremental() %}
    AND modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

date_components AS (
  SELECT
    -- Primary key: YYYY-MM-DD format for easy joining
    FORMAT_DATE('%Y-%m-%d', full_date) as date_key,
    
    -- Full date for date operations
    full_date,
    
    -- Year components
    EXTRACT(YEAR FROM full_date) as year,
    
    -- Month components
    EXTRACT(MONTH FROM full_date) as month,
    CASE 
      WHEN EXTRACT(MONTH FROM full_date) = 1 THEN 'January'
      WHEN EXTRACT(MONTH FROM full_date) = 2 THEN 'February'
      WHEN EXTRACT(MONTH FROM full_date) = 3 THEN 'March'
      WHEN EXTRACT(MONTH FROM full_date) = 4 THEN 'April'
      WHEN EXTRACT(MONTH FROM full_date) = 5 THEN 'May'
      WHEN EXTRACT(MONTH FROM full_date) = 6 THEN 'June'
      WHEN EXTRACT(MONTH FROM full_date) = 7 THEN 'July'
      WHEN EXTRACT(MONTH FROM full_date) = 8 THEN 'August'
      WHEN EXTRACT(MONTH FROM full_date) = 9 THEN 'September'
      WHEN EXTRACT(MONTH FROM full_date) = 10 THEN 'October'
      WHEN EXTRACT(MONTH FROM full_date) = 11 THEN 'November'
      WHEN EXTRACT(MONTH FROM full_date) = 12 THEN 'December'
    END as month_name,
    
    -- Quarter calculation
    CASE 
      WHEN EXTRACT(MONTH FROM full_date) IN (1, 2, 3) THEN 1
      WHEN EXTRACT(MONTH FROM full_date) IN (4, 5, 6) THEN 2
      WHEN EXTRACT(MONTH FROM full_date) IN (7, 8, 9) THEN 3
      WHEN EXTRACT(MONTH FROM full_date) IN (10, 11, 12) THEN 4
    END as quarter,
    
    -- Day of week components
    EXTRACT(DAYOFWEEK FROM full_date) as day_of_week,
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 1 THEN 'Sunday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 2 THEN 'Monday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 3 THEN 'Tuesday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 4 THEN 'Wednesday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 5 THEN 'Thursday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 6 THEN 'Friday'
      WHEN EXTRACT(DAYOFWEEK FROM full_date) = 7 THEN 'Saturday'
    END as day_name,
    
    -- Weekend flag for business analysis
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN TRUE
      ELSE FALSE
    END as is_weekend
    
  FROM order_dates
)

SELECT * FROM date_components
ORDER BY full_date