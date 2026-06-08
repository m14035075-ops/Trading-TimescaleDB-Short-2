-- =====================================================================
-- NSE Tick Collector — TimescaleDB Schema  (v5 — round-4 review fixes)
-- =====================================================================
-- नए install पर:
--     psql -U postgres -d marketdata -f schema.sql
--
-- पुराने (v2/v3/v4) DB से upgrade — सिर्फ़ views drop करें
-- (CAGG `ohlc_1s` को drop मत करें — सब historical data चला जाएगा):
--     DROP VIEW IF EXISTS v_quotes_1m, v_quotes_1m_raw;
-- फिर schema.sql चलाएँ। ALTER TABLE से नए columns idempotent जुड़ेंगे।
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1) ticks
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

-- Idempotent migration columns
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

-- v5: 32-char SHA-256 tick_uid → spool replay safe
CREATE UNIQUE INDEX IF NOT EXISTS uq_ticks_dedupe
    ON ticks (ts, tick_uid);

-- ---------------------------------------------------------------------
-- 2) ohlc_1s — 1-second OHLCV continuous aggregate
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
-- 3) Compression
-- ---------------------------------------------------------------------
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,exchange',
    timescaledb.compress_orderby = 'ts DESC'
);

SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 4) collector_gaps — v5: UNIQUE constraint (ON CONFLICT safe)
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

-- v5 FIX: gap-spool replay duplicates के लिए unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS uq_collector_gaps_window
    ON collector_gaps (started_at, ended_at, COALESCE(reason, ''));

-- ---------------------------------------------------------------------
-- 5) ohlc_1m_filled
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
-- 6) Views — v5 fixes:
--    * gap_minutes: time_bucket floor on BOTH ends (last partial minute fix)
--    * v_quotes_1m_raw: history filter (live missing/partial only)
--    * v_quotes_1m: priority full > sparse > history > partial
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
    -- v5 FIX: time_bucket floor for start & end (partial minutes covered)
    SELECT DISTINCT gs AS ts
    FROM collector_gaps g
    CROSS JOIN LATERAL generate_series(
        time_bucket('1 minute', g.started_at),
        time_bucket('1 minute', g.ended_at),
        '1 minute'::interval
    ) AS gs
)
SELECT
    lm.ts, lm.exchange, lm.symbol,
    lm.open, lm.high, lm.low, lm.close, lm.volume,
    'tick' AS source,
    CASE
        WHEN gm.ts IS NOT NULL    THEN 'partial'
        WHEN lm.sec_count >= 10   THEN 'full'
        ELSE                            'sparse'
    END AS quality
FROM   live_1m lm
LEFT JOIN gap_minutes gm ON gm.ts = lm.ts
UNION ALL
-- v5 FIX: history सिर्फ़ तब include करो जब live data missing OR partial हो
SELECT
    f.ts, f.exchange, f.symbol,
    f.open, f.high, f.low, f.close, f.volume,
    f.source,
    'history' AS quality
FROM   ohlc_1m_filled f
LEFT JOIN live_1m lm
       ON lm.ts       = date_trunc('minute', f.ts)
      AND lm.exchange = f.exchange
      AND lm.symbol   = f.symbol
LEFT JOIN gap_minutes gm
       ON gm.ts       = date_trunc('minute', f.ts)
WHERE  lm.ts IS NULL OR gm.ts IS NOT NULL;

-- v5 FIX: clean view priority — live real data हमेशा > history fill
-- full (अच्छा coverage, no gap) > sparse (real low-liquidity) > history > partial (live पर gap भी)
CREATE OR REPLACE VIEW v_quotes_1m AS
SELECT DISTINCT ON (ts, exchange, symbol)
       ts, exchange, symbol, open, high, low, close, volume, source, quality
FROM   v_quotes_1m_raw
ORDER  BY ts, exchange, symbol,
    CASE quality
        WHEN 'full'    THEN 1
        WHEN 'sparse'  THEN 2
        WHEN 'history' THEN 3
        WHEN 'partial' THEN 4
    END;
