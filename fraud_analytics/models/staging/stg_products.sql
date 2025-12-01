select
    product_id,
    product_name,
    category,
    cast(price as int64) as price,
    cast(created_date as timestamp) as created_at
from {{ source('zidan_finpro', 'products') }}