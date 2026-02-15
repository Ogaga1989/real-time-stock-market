CREATE TABLE IF NOT EXISTS stock_ticks (
  symbol TEXT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  price NUMERIC,
  volume BIGINT,
  source TEXT,
  ingest_ts TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_analytics (
  symbol TEXT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  avg_price NUMERIC,
  min_price NUMERIC,
  max_price NUMERIC,
  total_volume BIGINT,
  ingest_ts TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY(symbol, window_start, window_end)
);