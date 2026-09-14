-- ============================================================
-- Raw Stock Observations
-- ============================================================

CREATE TABLE IF NOT EXISTS stock_ticks (
    symbol TEXT NOT NULL,

    event_time TIMESTAMP NOT NULL,

    price NUMERIC,

    -- Volume reported by the Alpha Vantage quote endpoint.
    -- This is not treated as an incremental trade volume.
    reported_volume BIGINT,

    source TEXT,

    ingest_ts TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- One-Minute Stock Analytics
-- ============================================================

CREATE TABLE IF NOT EXISTS stock_analytics (
    symbol TEXT NOT NULL,

    window_start TIMESTAMP NOT NULL,

    window_end TIMESTAMP NOT NULL,

    avg_price NUMERIC,

    min_price NUMERIC,

    max_price NUMERIC,

    -- Latest provider-reported volume observed
    -- within the one-minute window.
    latest_reported_volume BIGINT,

    ingest_ts TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (
        symbol,
        window_start,
        window_end
    )
);