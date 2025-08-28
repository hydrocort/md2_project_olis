with source as (

    select
        trim(product_id) as product_id, 
        trim(product_category_name) as product_category_name,
        SAFE_CAST(NULLIF(trim(product_name_lenght), '') AS INT64) as product_name_length,
        SAFE_CAST(NULLIF(trim(product_description_lenght), '') AS INT64) as product_description_length,
        SAFE_CAST(NULLIF(trim(product_photos_qty), '') AS INT64) as product_photos_qty,
        SAFE_CAST(NULLIF(trim(product_weight_g), '') AS INT64) as product_weight_g,
        SAFE_CAST(NULLIF(trim(product_length_cm), '') AS INT64) as product_length_cm,
        SAFE_CAST(NULLIF(trim(product_height_cm), '') AS INT64) as product_height_cm,
        SAFE_CAST(NULLIF(trim(product_width_cm), '') AS INT64) as product_width_cm
    from  {{ source('olis_raw_dataset', 'products') }} prds
    where product_id is not null

)

select
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from source