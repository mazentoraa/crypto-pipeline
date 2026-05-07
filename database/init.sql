-- =============================================================================
-- Crypto & News Analytics Pipeline — Database Schema
-- Auto-executed by PostgreSQL container on first startup
-- =============================================================================

-- ─── Table: crypto_prices ─────────────────────────────────────────────────
-- Stores raw cryptocurrency price data fetched from CoinGecko API.
-- One row per coin per fetch cycle (every 5 minutes).
CREATE TABLE IF NOT EXISTS crypto_prices (
    id               SERIAL PRIMARY KEY,
    coin_id          VARCHAR(50)     NOT NULL,          -- e.g., "bitcoin"
    symbol           VARCHAR(20)     NOT NULL,          -- e.g., "BTC"
    price_usd        NUMERIC(20, 8),                    -- Current price in USD
    market_cap       NUMERIC(30, 2),                    -- Total market capitalisation
    volume_24h       NUMERIC(30, 2),                    -- 24-hour trading volume
    price_change_24h NUMERIC(10, 4),                    -- % change in last 24 hours
    fetched_at       TIMESTAMP       DEFAULT NOW()      -- When the row was inserted
);

-- Speed up time-range queries and per-coin lookups
CREATE INDEX IF NOT EXISTS idx_crypto_prices_coin_id
    ON crypto_prices (coin_id);

CREATE INDEX IF NOT EXISTS idx_crypto_prices_fetched_at
    ON crypto_prices (fetched_at DESC);

-- ─── Table: news_headlines ────────────────────────────────────────────────
-- Stores news headlines consumed from the Kafka topic.
-- Sentiment is classified inline by the Kafka consumer.
CREATE TABLE IF NOT EXISTS news_headlines (
    id              SERIAL PRIMARY KEY,
    headline        TEXT            NOT NULL,           -- Full headline text
    source          VARCHAR(100),                       -- Publisher name
    published_at    TIMESTAMP,                          -- Original publish time
    sentiment_label VARCHAR(20),                        -- "positive" | "neutral" | "negative"
    sentiment_score NUMERIC(5, 4),                      -- TextBlob polarity: -1.0 to +1.0
    created_at      TIMESTAMP       DEFAULT NOW()       -- When consumer stored the row
);

CREATE INDEX IF NOT EXISTS idx_news_headlines_created_at
    ON news_headlines (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_headlines_sentiment
    ON news_headlines (sentiment_label);

-- ─── Table: price_aggregations ────────────────────────────────────────────
-- Stores daily OHLCV-style aggregations computed by the transformation layer.
-- One row per coin per day.
CREATE TABLE IF NOT EXISTS price_aggregations (
    id          SERIAL PRIMARY KEY,
    coin_id     VARCHAR(50)     NOT NULL,
    date        DATE            NOT NULL,
    avg_price   NUMERIC(20, 8),                         -- Average price for the day
    max_price   NUMERIC(20, 8),                         -- Highest price recorded
    min_price   NUMERIC(20, 8),                         -- Lowest price recorded
    avg_volume  NUMERIC(30, 2),                         -- Average trading volume
    data_points INTEGER,                                -- How many raw rows aggregated
    computed_at TIMESTAMP       DEFAULT NOW(),          -- When this was last computed

    -- Prevent duplicate aggregations for the same coin+day
    CONSTRAINT uq_price_agg_coin_date UNIQUE (coin_id, date)
);

CREATE INDEX IF NOT EXISTS idx_price_agg_date
    ON price_aggregations (date DESC);

-- ─── Confirmation message ─────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE 'Schema initialised successfully. Tables: crypto_prices, news_headlines, price_aggregations';
END
$$;