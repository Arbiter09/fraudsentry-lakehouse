-- Singular test: flags amounts high enough that they're almost certainly
-- a data-quality issue rather than a real (even fraudulent) transaction.
-- $50k is well above anything the generator produces, including its
-- high-amount fraud pattern (data_generator/generate_transactions.py
-- caps that at $9k) -- so any row here means a bug, not a signal.

select transaction_id, amount
from {{ ref('stg_transactions') }}
where amount > 50000
