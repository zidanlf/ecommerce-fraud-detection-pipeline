select
    order_id,
    user_id,
    product_id,
    cast(quantity as int64) as quantity,
    cast(amount as int64) as amount,
    status,
    country,
    cast(created_date as timestamp) as created_at
from {{ source('zidan_finpro', 'orders') }}