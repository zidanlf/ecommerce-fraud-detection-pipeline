with fraud_data as (
    select * from {{ ref('fact_orders') }}
    where status = 'frauds'
)
select
    f.user_id,
    u.name,
    u.email,
    count(f.order_id) as total_fraud_cases,
    sum(f.amount) as total_fraud_amount,
    string_agg(distinct f.country, ', ') as fraud_locations
from fraud_data f
left join {{ ref('dim_users') }} u on f.user_id = u.user_id
group by 1, 2, 3
order by total_fraud_cases desc