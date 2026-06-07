-- =====================================================================
-- NSE Tick Collector — TimescaleDB Schema  (v3 — review fixes round 2)
-- =====================================================================
-- एक बार चलाएं:  psql -U postgres -d marketdata -f schema.sql
-- TimescaleDB extension पहले से installed होनी चाहिए।
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1) ticks  — हर WebSocket tick (quote OR depth)
--    stream_type: 'quote' | 'depth'   (कौन से subscription से आया)
--    tick_volume: इस tick की actual quantity (cumulative volume का delta)
--    depth      : Level-2 order book snapshot (JSONB) — सिर्फ़ depth mode में
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks (
    ts          TIMESTAMPTZ      NOT NULL,
    exchange    TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    stream_type TEXT             NOT NULL DEFAULT 'quote',
    ltp         DOUBLE PRECISION NOT NULL,
    volume      BIGINT,                 -- cumulative day volume (broker)
    tick_volume BIGINT,                 -- delta — इस tick की quantity
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,       -- previous close
    depth       JSONB,                  -- Level-2 full order book
    raw         JSONB                   -- backup payload (optional)
);

SELECT create_hypertable(
    'ticks', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ticks_ex_sym_ts
    ON ticks (exchange, symbol, ts DESC);

-- ---------------------------------------------------------------------
-- 2) 1-second OHLCV continuous aggregate
--    materialized_only = false  →  TimescaleDB 2.13+ में real-time
--    aggregation enable होती है (latest seconds query में दिखें)।
--    Volume = sum(tick_volume) — सही formula (per-tick delta का sum)।
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlc_1s
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
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

SELECT add_continuous_aggregate_policy('ohlc_1s',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 3) Compression — 7 दिन से पुराना (~10x storage saving)
-- ---------------------------------------------------------------------
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,exchange',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 4) Retention (optional)
-- ---------------------------------------------------------------------
-- SELECT add_retention_policy('ticks', INTERVAL '365 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 5) collector_gaps — disconnect periods log (per-symbol retry)
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
-- 6) ohlc_1m_filled — gap_filler.py की भरी हुई 1-min bars
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
-- 7) v_quotes_1m  — smart 1-minute feed
--    quality column ML training में filter के लिए:
--      'full'    → live 1-sec data से aggregate, अच्छी coverage (≥ 50 sec)
--      'partial' → live data था पर coverage कम (<50 sec) — careful use
--      'history' → disconnect period में history API से भरा
--
--    Strict ML training के लिए:  WHERE quality IN ('full', 'history')
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
        COALESCE(SUM(volume), 0)::BIGINT AS volume,
        COUNT(*)                        AS sec_count
    FROM ohlc_1s
    GROUP BY 1, 2, 3
)
SELECT
    ts, exchange, symbol, open, high, low, close, volume,
    'tick' AS source,
    CASE WHEN sec_count >= 50 THEN 'full' ELSE 'partial' END AS quality
FROM   live_1m
UNION ALL
SELECT
    f.ts, f.exchange, f.symbol, f.open, f.high, f.low, f.close, f.volume,
    f.source,
    'history' AS quality
FROM   ohlc_1m_filled f
LEFT JOIN live_1m lm
       ON lm.ts       = date_trunc('minute', f.ts)
      AND lm.exchange = f.exchange
      AND lm.symbol   = f.symbol
      AND lm.sec_count >= 50         -- सिर्फ़ "full" live minutes को override मानो
WHERE  lm.ts IS NULL;
