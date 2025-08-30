SELECT 
product_category_name, 
product_category_name_english 
FROM {{ source(env_var('RAW_DATASET_NAME'), 'product_category_name_translation') }}