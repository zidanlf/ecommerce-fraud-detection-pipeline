select
    u.name as user_name,
    u.email,
    count(f.order_id) as total_fraud_attempts,
    sum(f.amount) as total_fraud_value,
    string_agg(distinct f.country, ', ') as fraud_locations
from {{ ref('fact_orders') }} f
left join {{ ref('dim_users') }} u on f.user_id = u.user_id
where f.status = 'frauds'
group by 1, 2
order by total_fraud_attempts desc