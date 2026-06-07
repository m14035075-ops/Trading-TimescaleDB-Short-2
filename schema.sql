-- =====================================================================
-- NSE Tick Collector — TimescaleDB Schema  (v2 — review-fixes)
-- =====================================================================
-- एक बार चलाएं:  psql -U postgres -d marketdata -f schema.sql
-- TimescaleDB extension पहले से installed होनी चाहिए।
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1) Raw ticks table  (हर WebSocket tick — 100-500 ms में एक)
--    tick_volume = इस tick में कितनी quantity ट्रेड हुई (cumulative volume का delta)
--    depth       = optional Level-2 order book snapshot (JSONB)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks (
    ts          TIMESTAMPTZ      NOT NULL,
    exchange    TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    ltp         DOUBLE PRECISION NOT NULL,
    volume      BIGINT,                 -- cumulative day volume (broker से जैसा आया)
    tick_volume BIGINT,                 -- delta — इस tick की actual quantity
    bid         DOUBLE PRECISION,       -- best bid (top of book)
    ask         DOUBLE PRECISION,       -- best ask
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,       -- previous close
    depth       JSONB,                  -- Level-2 full order book (optional)
    raw         JSONB                   -- backup payload, debug के लिए (optional)
);

-- Hypertable — 1 दिन का chunk
SELECT create_hypertable(
    'ticks', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Lookup speed के लिए index (exchange first ताकि multi-exchange queries भी fast)
CREATE INDEX IF NOT EXISTS idx_ticks_ex_sym_ts
    ON ticks (exchange, symbol, ts DESC);

-- ---------------------------------------------------------------------
-- 2) 1-second OHLCV continuous aggregate
--    Volume = sum(tick_volume) — सही formula
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlc_1s
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 second', ts)              AS bucket,
    exchange,
    symbol,
    first(ltp, ts)                           AS open,
    max(ltp)                                 AS high,
    min(ltp)                                 AS low,
    last(ltp, ts)                            AS close,
    COALESCE(SUM(tick_volume), 0)::BIGINT    AS volume,
    COUNT(*)                                 AS tick_count
FROM ticks
GROUP BY bucket, exchange, symbol
WITH NO DATA;

-- हर 30 सेकंड में refresh
SELECT add_continuous_aggregate_policy('ohlc_1s',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 3) Compression policy — 7 दिन से पुराना data compress (~10x saving)
-- ---------------------------------------------------------------------
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,exchange',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 4) Retention (optional) — 1 साल से पुराना delete
-- ---------------------------------------------------------------------
-- SELECT add_retention_policy('ticks', INTERVAL '365 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 5) collector_gaps — disconnect periods log
--    failed_symbols में जिन symbols के लिए gap भर नहीं पाया उनकी list।
--    filled = TRUE तभी जब failed_symbols खाली हो।
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collector_gaps (
    id              BIGSERIAL    PRIMARY KEY,
    started_at      TIMESTAMPTZ  NOT NULL,
    ended_at        TIMESTAMPTZ  NOT NULL,
    reason          TEXT,
    filled          BOOLEAN      DEFAULT FALSE,
    attempts        INT          DEFAULT 0,
    failed_symbols  TEXT[]       DEFAULT '{}'::TEXT[],
    last_error      TEXT,
    last_attempt_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_gaps_unfilled
    ON collector_gaps (started_at) WHERE filled = FALSE;

-- ---------------------------------------------------------------------
-- 6) ohlc_1m_filled — gap_filler.py history API से जो भरे, यहाँ
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlc_1m_filled (
    ts          TIMESTAMPTZ NOT NULL,
    exchange    TEXT        NOT NULL,
    symbol      TEXT        NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    source      TEXT        DEFAULT 'history_api',
    PRIMARY KEY (ts, symbol, exchange)
);

SELECT create_hypertable(
    'ohlc_1m_filled', 'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------
-- 7) v_quotes_1m  — "prefer live, fallback to history" 1-minute view
--    जिन minutes में ticks आ चुके हैं वहाँ ohlc_1s से aggregate;
--    जहाँ live data नहीं था (disconnect window) वहाँ ohlc_1m_filled से।
--    ML training के लिए यह सबसे clean feed है।
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quotes_1m AS
WITH live_1m AS (
    SELECT
        time_bucket('1 minute', bucket) AS ts,
        exchange,
        symbol,
        first(open, bucket)             AS open,
        max(high)                       AS high,
        min(low)                        AS low,
        last(close, bucket)             AS close,
        SUM(volume)::BIGINT             AS volume
    FROM ohlc_1s
    GROUP BY 1, 2, 3
)
SELECT ts, exchange, symbol, open, high, low, close, volume, 'tick' AS source
FROM   live_1m
UNION ALL
SELECT f.ts, f.exchange, f.symbol, f.open, f.high, f.low, f.close, f.volume, f.source
FROM   ohlc_1m_filled f
LEFT JOIN live_1m lm
       ON lm.ts       = date_trunc('minute', f.ts)
      AND lm.exchange = f.exchange
      AND lm.symbol   = f.symbol
WHERE  lm.ts IS NULL;
