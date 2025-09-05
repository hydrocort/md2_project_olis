SELECT 
product_category_name, 
product_category_name_english 
FROM {{ ref('category_translation_snapshot') }}