with base as (

    select
        trim(product_id) as product_id, 
        trim(product_category_name) as product_category_name,
        COALESCE(SAFE_CAST(NULLIF(trim(product_name_lenght), '') AS INT64), 0) as product_name_length,
        COALESCE(SAFE_CAST(NULLIF(trim(product_description_lenght), '') AS INT64), 0) as product_description_length,
        COALESCE(SAFE_CAST(NULLIF(trim(product_photos_qty), '') AS INT64), 0) as product_photos_qty,
        COALESCE(SAFE_CAST(NULLIF(trim(product_weight_g), '') AS FLOAT64), 0) as product_weight_g,
        COALESCE(SAFE_CAST(NULLIF(trim(product_length_cm), '') AS FLOAT64), 0) as product_length_cm,
        COALESCE(SAFE_CAST(NULLIF(trim(product_height_cm), '') AS FLOAT64), 0) as product_height_cm,
        COALESCE(SAFE_CAST(NULLIF(trim(product_width_cm), '') AS FLOAT64), 0) as product_width_cm,

        -- Calculated volume (with null handling)
        CASE 
            WHEN product_length_cm != '' AND SAFE_CAST(product_length_cm AS FLOAT64) IS NOT NULL
                AND product_height_cm != '' AND SAFE_CAST(product_height_cm AS FLOAT64) IS NOT NULL
                AND product_width_cm != '' AND SAFE_CAST(product_width_cm AS FLOAT64) IS NOT NULL
                AND SAFE_CAST(product_length_cm AS FLOAT64) > 0
                AND SAFE_CAST(product_height_cm AS FLOAT64) > 0
                AND SAFE_CAST(product_width_cm AS FLOAT64) > 0
            THEN SAFE_CAST(product_length_cm AS FLOAT64) * 
                 SAFE_CAST(product_height_cm AS FLOAT64) * 
                 SAFE_CAST(product_width_cm AS FLOAT64)
            ELSE 0.0
        END as product_volume_cm3,

        safe_cast(_sdc_extracted_at as timestamp) as modified_at

    from  {{ source(env_var('RAW_DATASET_NAME'), 'products') }} prds
),
ranked as (
  select *, row_number() over (partition by product_id order by modified_at desc) rn
  from base
)
select * except(rn) from ranked where rn=1