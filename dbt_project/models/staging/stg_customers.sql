with source as (
    select
        customer_id,
        customer_unique_id,
        trim(lower(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        trim(customer_zip_code_prefix) as customer_zip_prefix,
    from {{ ref('customers_snapshot') }} 
    where customer_id is not null
)

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_prefix
from source