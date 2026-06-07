-- =====================================================================
-- NSE Tick Collector — TimescaleDB Schema  (v4 — round-3 review fixes)
-- =====================================================================
--
-- नए install पर:
--     psql -U postgres -d marketdata -f schema.sql
--
-- पुराने (v2/v3) DB से upgrade पर — पहले एक बार:
--     DROP MATERIALIZED VIEW IF EXISTS ohlc_1s CASCADE;
--     DROP VIEW IF EXISTS v_quotes_1m;
--     DROP VIEW IF EXISTS v_quotes_1m_raw;
-- फिर schema.sql चलाएँ। (नए columns ALTER से idempotent जुड़ जाएँगे।)
--
-- TimescaleDB extension पहले से installed होनी चाहिए।
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1) ticks  — हर WebSocket tick (quote OR depth)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks (
    ts          TIMESTAMPTZ      NOT NULL,
    exchange    TEXT             NOT NULL,
    symbol      TEXT             NOT NULL,
    stream_type TEXT             NOT NULL DEFAULT 'quote',
    ltp         DOUBLE PRECISION NOT NULL,
    volume      BIGINT,
    tick_volume BIGINT,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    depth       JSONB,
    raw         JSONB,
    tick_uid    TEXT
);

-- Idempotent migration: पुराने schemas से columns missing हों तो जोड़ दे
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS stream_type TEXT;
ALTER TABLE ticks ALTER COLUMN stream_type SET DEFAULT 'quote';
UPDATE ticks SET stream_type = 'quote' WHERE stream_type IS NULL;
ALTER TABLE ticks ALTER COLUMN stream_type SET NOT NULL;

ALTER TABLE ticks ADD COLUMN IF NOT EXISTS tick_volume BIGINT;
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS depth       JSONB;
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS tick_uid    TEXT;

SELECT create_hypertable(
    'ticks', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ticks_ex_sym_ts
    ON ticks (exchange, symbol, ts DESC);

-- Spool replay duplicate से बचाव: ts + tick_uid पर unique
-- (TimescaleDB hypertable में partition column ts include होना ज़रूरी)
CREATE UNIQUE INDEX IF NOT EXISTS uq_ticks_dedupe
    ON ticks (ts, tick_uid);

-- ---------------------------------------------------------------------
-- 2) ohlc_1s  — 1-second OHLCV continuous aggregate
--
--    Note: CAGG यह assume करता है कि एक समय पर सिर्फ़ एक mode चल रहा है
--    (MODE=quote XOR MODE=depth)। दोनों stream_types का LTP/volume same
--    cumulative counter से आता है, इसलिए mixing harmless है, बस tick_count
--    inflate हो सकता है। forensic detail चाहिए तो ticks table से query करें।
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
-- 5) collector_gaps — disconnect periods (per-symbol retry)
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
-- 6) ohlc_1m_filled — gap_filler.py history bars
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
-- 7) Views: v_quotes_1m_raw (forensic — duplicates allowed)
--           v_quotes_1m     (clean — DISTINCT ON + quality priority)
--
--    quality column ML training में filter के लिए:
--      'full'    → live data, कोई gap overlap नहीं, tick coverage अच्छी
--      'partial' → live data था पर इस minute में disconnect-gap overlap
--      'sparse'  → live data था, gap नहीं, लेकिन कम trades (low liquidity)
--      'history' → disconnect period में history API से भरा
--
--    Strict ML feed के लिए:  v_quotes_1m WHERE quality IN ('full','history')
--    Low-liquidity OK:        v_quotes_1m WHERE quality != 'partial'
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quotes_1m_raw AS
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
),
gap_minutes AS (
    -- collector_gaps के सारे minutes (1-min boundary पर expand)
    SELECT DISTINCT
        time_bucket('1 minute', g) AS ts
    FROM (
        SELECT generate_series(started_at, ended_at, '1 minute'::interval) AS g
        FROM collector_gaps
    ) x
)
-- live row (always present) — quality gap-overlap और sec_count से
SELECT
    lm.ts, lm.exchange, lm.symbol,
    lm.open, lm.high, lm.low, lm.close, lm.volume,
    'tick' AS source,
    CASE
        WHEN gm.ts IS NOT NULL              THEN 'partial'
        WHEN lm.sec_count >= 10             THEN 'full'
        ELSE                                     'sparse'
    END AS quality
FROM live_1m lm
LEFT JOIN gap_minutes gm ON gm.ts = lm.ts
UNION ALL
-- history row — सिर्फ़ तब जब live data नहीं है, या live partial है
SELECT
    f.ts, f.exchange, f.symbol,
    f.open, f.high, f.low, f.close, f.volume,
    f.source,
    'history' AS quality
FROM ohlc_1m_filled f
LEFT JOIN live_1m lm
       ON lm.ts       = date_trunc('minute', f.ts)
      AND lm.exchange = f.exchange
      AND lm.symbol   = f.symbol
LEFT JOIN gap_minutes gm
       ON gm.ts       = date_trunc('minute', f.ts);
-- नोट: यहाँ duplicate possible है (live partial + history) — clean view
-- नीचे DISTINCT ON से एक ही row देता है, quality priority के साथ।

-- Clean view — ML training के लिए recommended
CREATE OR REPLACE VIEW v_quotes_1m AS
SELECT DISTINCT ON (ts, exchange, symbol)
       ts, exchange, symbol, open, high, low, close, volume, source, quality
FROM   v_quotes_1m_raw
ORDER  BY ts, exchange, symbol,
    CASE quality
        WHEN 'full'    THEN 1
        WHEN 'history' THEN 2
        WHEN 'sparse'  THEN 3
        WHEN 'partial' THEN 4
    END;
