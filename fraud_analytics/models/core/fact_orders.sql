-- fact_orders.sql
select
    o.order_id,
    o.user_id,
    o.product_id,
    o.quantity,
    o.amount,
    o.status,
    o.country,
    o.created_at,
    p.category,
    p.product_name,
    p.price as unit_price
from {{ ref('stg_orders') }} o
left join {{ ref('stg_products') }} p on o.product_id = p.product_id