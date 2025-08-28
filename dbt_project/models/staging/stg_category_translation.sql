SELECT 
product_category_name, 
product_category_name_english 
FROM {{ source('olis_raw_dataset', 'product_category_name_translation') }}