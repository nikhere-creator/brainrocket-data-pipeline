-- BrainRocket Gaming Data Pipeline Schema
-- Dimension and fact tables for gaming transactions

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_game CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_daily_game_metrics CASCADE;
DROP VIEW IF EXISTS v_daily_game_report CASCADE;

-- Dimension table for games
CREATE TABLE dim_game (
    game_id SERIAL PRIMARY KEY,
    game_name VARCHAR(100) NOT NULL,
    game_genre VARCHAR(50) NOT NULL,
    release_date DATE,
    developer VARCHAR(100),
    price DECIMAL(10,2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension table for locations (countries/regions)
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    timezone VARCHAR(50),
    currency_code VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table for gaming transactions
CREATE TABLE fact_transactions (
    transaction_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES dim_game(game_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    user_id VARCHAR(50) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('purchase', 'in-game', 'subscription')),
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    transaction_date TIMESTAMP NOT NULL,
    platform VARCHAR(20) DEFAULT 'web',
    session_duration INTEGER,
    items_purchased INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_fact_transactions_game_id ON fact_transactions(game_id);
CREATE INDEX idx_fact_transactions_location_id ON fact_transactions(location_id);
CREATE INDEX idx_fact_transactions_transaction_date ON fact_transactions(transaction_date);
CREATE INDEX idx_fact_transactions_user_id ON fact_transactions(user_id);

-- Materialized view for daily game metrics
CREATE MATERIALIZED VIEW mv_daily_game_metrics AS
SELECT 
    DATE(transaction_date) AS transaction_day,
    game_id,
    location_id,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(amount) AS avg_transaction_value,
    SUM(CASE WHEN transaction_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN transaction_type = 'in-game' THEN amount ELSE 0 END) AS in_game_revenue,
    SUM(CASE WHEN transaction_type = 'subscription' THEN amount ELSE 0 END) AS subscription_revenue
FROM fact_transactions
GROUP BY DATE(transaction_date), game_id, location_id;

-- Reporting view for daily game reports
CREATE VIEW v_daily_game_report AS
SELECT 
    mg.transaction_day,
    g.game_name,
    g.game_genre,
    l.country_name,
    l.region,
    mg.total_transactions,
    mg.total_revenue,
    mg.unique_users,
    mg.avg_transaction_value,
    mg.purchase_count,
    mg.in_game_revenue,
    mg.subscription_revenue,
    ROUND((mg.total_revenue / NULLIF(mg.unique_users, 0)), 2) AS revenue_per_user
FROM mv_daily_game_metrics mg
JOIN dim_game g ON mg.game_id = g.game_id
JOIN dim_location l ON mg.location_id = l.location_id
ORDER BY mg.transaction_day DESC, mg.total_revenue DESC;
