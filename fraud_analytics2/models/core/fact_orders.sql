select o.*, p.category, p.product_name, p.price as unit_price
from {{ ref('stg_orders') }} o
left join {{ ref('stg_products') }} p on o.product_id = p.product_id
