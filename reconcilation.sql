-- reconciliation.sql

WITH successful_payments AS (
    -- 1. Identify successful payments and use ROW_NUMBER to prep for deduplication
    SELECT 
        payment_id,
        order_id,
        amount_cents,
        attempted_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY attempted_at DESC) as attempt_rank
    FROM payments
    WHERE status = 'SUCCESS'
      AND order_id IS NOT NULL
),

deduped_sales AS (
    -- 2. Deduplicate multiple payment attempts per order (keep the latest success)
    SELECT 
        payment_id,
        order_id,
        amount_cents
    FROM successful_payments
    WHERE attempt_rank = 1
),

orphan_payments AS (
    -- 3. Isolate money received without an associated internal order
    SELECT 
        payment_id,
        amount_cents
    FROM payments
    WHERE status = 'SUCCESS'
      AND order_id IS NULL
),

aggregated_settlements AS (
    -- 4. Aggregate bank settlements to handle partial and duplicate records
    SELECT 
        payment_id,
        SUM(settled_amount_cents) as total_settled_cents
    FROM bank_settlements
    WHERE status = 'SETTLED'
      AND payment_id IS NOT NULL
    GROUP BY payment_id
)

-- 5. Generate Final Reconciliation Report
SELECT 
    'Total Expected Sales (Cleaned & Deduped)' AS metric,
    COALESCE(SUM(amount_cents) / 100.0, 0) AS value_usd
FROM deduped_sales

UNION ALL

SELECT 
    'Orphan Payments (Money without Orders)' AS metric,
    COALESCE(SUM(amount_cents) / 100.0, 0) AS value_usd
FROM orphan_payments

UNION ALL

SELECT 
    'Bank Settled Amount (Matched to Orders)' AS metric,
    COALESCE(SUM(s.total_settled_cents) / 100.0, 0) AS value_usd
FROM deduped_sales d
LEFT JOIN aggregated_settlements s ON d.payment_id = s.payment_id

UNION ALL

SELECT 
    'Discrepancy Gap (Expected Sales vs. Matched Bank Settled)' AS metric,
    -- Discrepancy = Internal Deduped Expected - Bank Settled
    COALESCE((SUM(d.amount_cents) - SUM(COALESCE(s.total_settled_cents, 0))) / 100.0, 0) AS value_usd
FROM deduped_sales d
LEFT JOIN aggregated_settlements s ON d.payment_id = s.payment_id;