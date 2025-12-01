select
    date(created_at) as transaction_date,
    sum(case when status = 'frauds' then amount else 0 end) as total_saved_amount,
    sum(case when status = 'genuine' then amount else 0 end) as total_revenue,
    count(order_id) as total_transactions
from {{ ref('fact_orders') }}
group by 1
order by 1 desc