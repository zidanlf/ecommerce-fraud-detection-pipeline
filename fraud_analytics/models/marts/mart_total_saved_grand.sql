with summary as (
    select
        sum(case when status = 'frauds' then amount else 0 end) as total_saved_amount,
        sum(case when status = 'genuine' then amount else 0 end) as total_revenue,
        sum(amount) as total_transaction_value,
        count(case when status = 'frauds' then order_id end) as total_fraud_cases
    from {{ ref('fact_orders') }}
)

select 
    total_saved_amount,
    total_revenue,
    total_transaction_value,
    total_fraud_cases,
    round((total_saved_amount / nullif(total_transaction_value, 0)) * 100, 2) as saved_rate_percentage
from summary