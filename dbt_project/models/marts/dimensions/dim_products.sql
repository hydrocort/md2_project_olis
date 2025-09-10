{{ config(
    partition_by={"field":"modified_at","data_type":"timestamp","granularity":"day"},
    cluster_by=['product_key'],
    unique_key='product_key'
) }}
WITH product_base AS (
  SELECT
    -- Primary key
    product_id as product_key,
    
    -- Product attributes (already cleaned in staging)
    product_category_name as product_category_portuguese,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,
    product_name_length,
    product_description_length,
    
    -- Calculated volume (already calculated in staging)
    product_volume_cm3,
    modified_at
    
  FROM {{ ref('stg_products') }}
  {% if is_incremental() %}
    where modified_at >
      (select coalesce(max(modified_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

product_categories AS (
  SELECT
    p.*,
    
    -- Category translation (English names)
    COALESCE(ct.product_category_name_english, p.product_category_portuguese) as product_category_english
    
  FROM product_base p
  LEFT JOIN {{ ref('stg_category_translation') }} ct
    ON LOWER(TRIM(p.product_category_portuguese)) = LOWER(TRIM(ct.product_category_name))
)

SELECT * FROM product_categories