select
    order_id,
    user_id,
    product_id,
    cast(quantity as int64) as quantity,
    cast(amount as int64) as amount,
    country,
    status,
    cast(created_date as timestamp) as created_at
from `finpro-purwadhika`.`zidan_finpro`.`orders`