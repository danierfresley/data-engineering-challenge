-- Users with >3 failed transactions in last 7 days
WITH failed_transactions AS (
    SELECT 
        user_id,
        COUNT(*) as failed_count
    FROM fact_transactions
    WHERE status = 'failed'
        AND created_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY user_id
    HAVING COUNT(*) > 3
)
SELECT 
    u.user_id,
    u.user_name,
    u.user_category,
    ft.failed_count,
    ft.failed_count * 100.0 / (SELECT COUNT(*) FROM fact_transactions WHERE user_id = u.user_id) as failure_rate
FROM dim_users u
JOIN failed_transactions ft ON u.user_id = ft.user_id
WHERE u.is_current = TRUE
ORDER BY ft.failed_count DESC;