-- mart_fraud_by_product.sql
select
    category,
    product_name,
    count(order_id) as total_transactions,
    sum(case when status = 'frauds' then 1 else 0 end) as total_fraud_cases,
    round(
        (sum(case when status = 'frauds' then 1 else 0 end) / count(order_id)) * 100, 
        2
    ) as fraud_rate_percentage
from {{ ref('fact_orders') }}
group by 1, 2
having total_fraud_cases > 0
order by total_fraud_cases desc