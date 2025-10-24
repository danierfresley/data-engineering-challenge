\c data_warehouse;

-- Create tables
\i /docker-entrypoint-initdb.d/sql/ddl/create_tables.sql

-- Create views
\i /docker-entrypoint-initdb.d/sql/dml/daily_summary.sql

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_transactions_date ON fact_transactions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_user ON fact_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_status ON fact_transactions(status);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_processed ON raw_transactions(processed_at);

-- Insert initial date dimension data
INSERT INTO dim_date (date_id, full_date, day, month, year, quarter, day_of_week, day_name, month_name, is_weekend)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum AS full_date,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    TO_CHAR(datum, 'Month') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT '2023-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 365*5) AS SEQUENCE (DAY)
) DQ
ON CONFLICT (date_id) DO NOTHING;