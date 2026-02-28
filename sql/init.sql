-- Create schemas for the data warehouse layers
CREATE SCHEMA IF NOT EXISTS raw;        -- landing zone for source data
CREATE SCHEMA IF NOT EXISTS staging;    -- dbt staging layer
CREATE SCHEMA IF NOT EXISTS core;       -- dbt core layer (facts & dimensions)
CREATE SCHEMA IF NOT EXISTS mart;       -- dbt mart layer (business-facing)

-- Grant all privileges to the fintrade user
GRANT ALL PRIVILEGES ON SCHEMA raw TO fintrade;
GRANT ALL PRIVILEGES ON SCHEMA staging TO fintrade;
GRANT ALL PRIVILEGES ON SCHEMA core TO fintrade;
GRANT ALL PRIVILEGES ON SCHEMA mart TO fintrade;

-- =============================================
-- RAW TABLES
-- =============================================

-- Stock information
CREATE TABLE IF NOT EXISTS raw.stocks (
    symbol          VARCHAR(10) PRIMARY KEY,
    company_name    VARCHAR(100) NOT NULL,
    sector          VARCHAR(50),
    exchange        VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Trading accounts
CREATE TABLE IF NOT EXISTS raw.accounts (
    account_id      VARCHAR(20) PRIMARY KEY,
    user_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(100),
    account_type    VARCHAR(20),   -- 'individual', 'institutional'
    balance         DECIMAL(15,2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Orders
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        VARCHAR(20) PRIMARY KEY,
    account_id      VARCHAR(20) REFERENCES raw.accounts(account_id),
    symbol          VARCHAR(10) REFERENCES raw.stocks(symbol),
    order_type      VARCHAR(10),   -- 'buy', 'sell'
    quantity        INTEGER,
    price           DECIMAL(10,2),
    status          VARCHAR(20),   -- 'filled', 'cancelled', 'partial'
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Executed trades
CREATE TABLE IF NOT EXISTS raw.trades (
    trade_id        VARCHAR(20) PRIMARY KEY,
    order_id        VARCHAR(20) REFERENCES raw.orders(order_id),
    account_id      VARCHAR(20) REFERENCES raw.accounts(account_id),
    symbol          VARCHAR(10) REFERENCES raw.stocks(symbol),
    trade_type      VARCHAR(10),   -- 'buy', 'sell'
    quantity        INTEGER,
    price           DECIMAL(10,2),
    total_amount    DECIMAL(15,2),
    traded_at       TIMESTAMP DEFAULT NOW()
);

-- Daily stock prices (OHLCV)
CREATE TABLE IF NOT EXISTS raw.daily_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(10) REFERENCES raw.stocks(symbol),
    price_date      DATE NOT NULL,
    open_price      DECIMAL(10,2),
    high_price      DECIMAL(10,2),
    low_price       DECIMAL(10,2),
    close_price     DECIMAL(10,2),
    volume          BIGINT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(symbol, price_date)
);