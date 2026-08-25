-- Thin cleaning layer over the silver Delta table: renames for clarity and
-- drops the one column (ingested_at) nothing downstream needs.

select
    transaction_id,
    account_id,
    timestamp   as transacted_at,
    dt          as transaction_date,
    amount,
    merchant_category,
    city,
    lat,
    lon,
    is_fraud
from {{ source('fraudsentry', 'silver_transactions') }}
