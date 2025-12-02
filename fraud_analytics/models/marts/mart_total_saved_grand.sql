with summary as (
    select
        -- Hitung total uang dari transaksi yang statusnya 'frauds'
        sum(case when status = 'frauds' then amount else 0 end) as total_saved_amount,
        
        -- Hitung total uang dari transaksi 'genuine' (Revenue)
        sum(case when status = 'genuine' then amount else 0 end) as total_revenue,
        
        -- Hitung total semua transaksi (Fraud + Genuine)
        sum(amount) as total_transaction_value,
        
        -- Hitung jumlah kasus fraud
        count(case when status = 'frauds' then order_id end) as total_fraud_cases
    from {{ ref('fact_orders') }}
)

select 
    total_saved_amount,
    total_revenue,
    total_transaction_value,
    total_fraud_cases,
    -- Hitung persentase uang yang diselamatkan dibanding total transaksi
    round((total_saved_amount / nullif(total_transaction_value, 0)) * 100, 2) as saved_rate_percentage
from summary