-- Daily summary view
CREATE OR REPLACE VIEW daily_transaction_summary AS
SELECT 
    d.full_date as transaction_date,
    t.status,
    COUNT(*) as transaction_count,
    SUM(t.amount) as total_amount,
    AVG(t.amount) as average_amount,
    COUNT(DISTINCT t.user_id) as unique_users
FROM fact_transactions t
JOIN dim_date d ON t.date_id = d.date_id
GROUP BY d.full_date, t.status
ORDER BY d.full_date DESC, t.status;