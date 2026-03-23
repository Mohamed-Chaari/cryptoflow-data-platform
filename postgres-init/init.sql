-- PostgreSQL schema definition for CryptoFlow

-- Create ohlcv_1min table
CREATE TABLE IF NOT EXISTS ohlcv_1min (
    symbol VARCHAR(10) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    avg_volume NUMERIC,
    PRIMARY KEY (symbol, window_start)
);

-- Create alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    alert_type VARCHAR(10) NOT NULL,
    magnitude NUMERIC,
    timestamp TIMESTAMP NOT NULL
);

-- Create news_sentiment table
CREATE TABLE IF NOT EXISTS news_sentiment (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    sentiment_score NUMERIC NOT NULL,
    sentiment_label VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
