-- Singular test: a transaction timestamped in the future indicates a
-- clock-skew or bad-injection bug upstream, not a real transaction.
-- dbt tests pass when this query returns zero rows.

select transaction_id, transacted_at
from {{ ref('stg_transactions') }}
where transacted_at > current_timestamp()
