 -- Price time series (generic OHLCV; fill what you have)
CREATE TABLE IF NOT EXISTS market_prices (
    symbol TEXT,
    name   TEXT,
    ts     TIMESTAMP,
    open   DOUBLE,
    high   DOUBLE,
    low    DOUBLE,
    close  DOUBLE,
    volume DOUBLE,
    source TEXT,
    PRIMARY KEY (symbol, ts)
);

-- Basic news store
CREATE TABLE IF NOT EXISTS market_news (
    id            TEXT PRIMARY KEY,
    headline      TEXT,
    description   TEXT,
    url           TEXT,
    published_at  TIMESTAMP,
    source        TEXT,
    tickers       TEXT,       -- comma-separated or JSON if you prefer
    keywords      TEXT        -- query that fetched it
);
