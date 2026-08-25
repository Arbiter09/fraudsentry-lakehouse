-- Daily per-account rollup: transaction volume, spend, and flagged-fraud
-- count. This is the gold-layer table analysts/dashboards would query
-- directly, separate from the row-level anomaly scores Databricks writes
-- to gold_scored_transactions.

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    account_id,
    transaction_date,
    count(*)                                   as transaction_count,
    sum(amount)                                as total_amount,
    avg(amount)                                as avg_amount,
    sum(case when is_fraud = 1 then 1 else 0 end) as flagged_fraud_count
from transactions
group by account_id, transaction_date
