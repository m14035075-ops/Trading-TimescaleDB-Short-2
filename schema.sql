-- =====================================================================
-- NSE Tick Collector — TimescaleDB Schema
-- =====================================================================
-- एक बार चलाएं:  psql -U postgres -d marketdata -f schema.sql
-- TimescaleDB extension पहले से installed होनी चाहिए।
-- =====================================================================

-- TimescaleDB extension activate करें (अगर पहले से नहीं है)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1) ticks — हर WebSocket tick (raw data)
--    stream_type: 'quote' (Level-1) या 'depth' (Level-2)
--    tick_volume: इस tick में कितनी quantity ट्रेड हुई (cumulative volume का delta)
--    depth: पूरा order book snapshot (JSONB, सिर्फ depth mode में)
--    tick_uid: SHA-256 hash — duplicate-rokne के लिए unique key
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticks (
    ts          TIMESTAMPTZ      NOT NULL,                   -- timestamp (UTC में)
    exchange    TEXT             NOT NULL,                   -- NSE / BSE
    symbol      TEXT             NOT NULL,                   -- शेयर का नाम (RELIANCE, TCS...)
    stream_type TEXT             NOT NULL DEFAULT 'quote',   -- कौन से subscription से आया
    ltp         DOUBLE PRECISION NOT NULL,                   -- last traded price
    volume      BIGINT,                                      -- cumulative day volume
    tick_volume BIGINT,                                      -- इस tick की actual quantity (delta)
    bid         DOUBLE PRECISION,                            -- best bid price
    ask         DOUBLE PRECISION,                            -- best ask price
    open        DOUBLE PRECISION,                            -- day's open
    high        DOUBLE PRECISION,                            -- day's high
    low         DOUBLE PRECISION,                            -- day's low
    close       DOUBLE PRECISION,                            -- previous close
    depth       JSONB,                                       -- Level-2 order book
    raw         JSONB,                                       -- backup payload (debug)
    tick_uid    TEXT                                         -- dedup hash
);

-- अगर पुराने schema से upgrade कर रहे हैं तो ये columns add हो जाएँगे
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS stream_type TEXT;
ALTER TABLE ticks ALTER COLUMN stream_type SET DEFAULT 'quote';

-- पुराने NULL rows को 'quote' से backfill करो (सिर्फ़ तब जब NULL मौजूद हों)
-- ⚠️ बहुत बड़े production table पर migration off-hours में चलाएँ
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM ticks WHERE stream_type IS NULL LIMIT 1) THEN
        RAISE NOTICE 'पुराने NULL stream_type rows backfill कर रहे हैं';
        UPDATE ticks SET stream_type = 'quote' WHERE stream_type IS NULL;
    END IF;
END $$;
ALTER TABLE ticks ALTER COLUMN stream_type SET NOT NULL;

ALTER TABLE ticks ADD COLUMN IF NOT EXISTS tick_volume BIGINT;
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS depth       JSONB;
ALTER TABLE ticks ADD COLUMN IF NOT EXISTS tick_uid    TEXT;

-- Hypertable बनाओ — हर 1 दिन का अलग chunk
SELECT create_hypertable(
    'ticks', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Lookup speed के लिए index (exchange first → multi-exchange queries fast)
CREATE INDEX IF NOT EXISTS idx_ticks_ex_sym_ts
    ON ticks (exchange, symbol, ts DESC);

-- Spool replay duplicate से बचाव के लिए unique index
-- (TimescaleDB में partition column 'ts' include करना ज़रूरी है)
CREATE UNIQUE INDEX IF NOT EXISTS uq_ticks_dedupe
    ON ticks (ts, tick_uid);

-- ---------------------------------------------------------------------
-- 2) ohlc_1s — 1-second OHLCV continuous aggregate
--    materialized_only = false → real-time aggregation enable
--    Volume = sum(tick_volume) — सही formula (per-tick delta का sum)
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlc_1s
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 second', ts)              AS bucket,       -- 1-sec time bucket
    exchange,
    symbol,
    first(ltp, ts)                           AS open,         -- bucket का पहला ltp
    max(ltp)                                 AS high,         -- highest ltp
    min(ltp)                                 AS low,          -- lowest ltp
    last(ltp, ts)                            AS close,        -- bucket का आख़िरी ltp
    COALESCE(SUM(tick_volume), 0)::BIGINT    AS volume,       -- कुल tick_volumes का sum
    COUNT(*)                                 AS tick_count    -- कितने ticks आए
FROM ticks
GROUP BY bucket, exchange, symbol
WITH NO DATA;

-- हर 30 सेकंड में background में refresh
SELECT add_continuous_aggregate_policy('ohlc_1s',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '10 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 3) Compression — 7 दिन से पुराना data compress (~10x storage saving)
-- ---------------------------------------------------------------------
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,exchange',
    timescaledb.compress_orderby   = 'ts DESC'
);

SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 4) Retention (optional) — 1 साल से पुराना data delete करो
-- ---------------------------------------------------------------------
-- SELECT add_retention_policy('ticks', INTERVAL '365 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- 5) collector_gaps — disconnect periods log
--    हर gap window track करता है ताकि बाद में gap_filler उसे history से भर सके
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collector_gaps (
    id              BIGSERIAL    PRIMARY KEY,
    started_at      TIMESTAMPTZ  NOT NULL,             -- gap शुरू हुआ
    ended_at        TIMESTAMPTZ  NOT NULL,             -- gap खत्म हुआ
    reason          TEXT,                              -- कारण: ws_reconnect, startup_gap...
    filled          BOOLEAN      DEFAULT FALSE,        -- क्या gap भर गया?
    attempts        INT          DEFAULT 0,            -- कितनी बार कोशिश हुई
    failed_symbols  TEXT[]       DEFAULT '{}'::TEXT[], -- कौन से symbols fail हुए
    last_error      TEXT,                              -- last error message
    last_attempt_at TIMESTAMPTZ                        -- last try कब हुआ
);

-- सिर्फ़ unfilled gaps के लिए index (faster lookup)
CREATE INDEX IF NOT EXISTS idx_gaps_unfilled
    ON collector_gaps (started_at) WHERE filled = FALSE;

-- gap-spool replay duplicates से बचाव के लिए unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS uq_collector_gaps_window
    ON collector_gaps (started_at, ended_at, COALESCE(reason, ''));

-- ---------------------------------------------------------------------
-- 6) ohlc_1m_filled — gap_filler.py द्वारा भरी हुई 1-min bars
--    जब live data नहीं था (disconnect period), तब broker history API से
--    1-minute candles लाकर यहाँ store करते हैं
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ohlc_1m_filled (
    ts          TIMESTAMPTZ NOT NULL,           -- bar का timestamp
    exchange    TEXT        NOT NULL,
    symbol      TEXT        NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    source      TEXT        DEFAULT 'history_api',  -- कहाँ से आया
    PRIMARY KEY (ts, symbol, exchange)
);

SELECT create_hypertable(
    'ohlc_1m_filled', 'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------
-- 7) v_quotes_1m_raw — raw forensic view
--    हर 1-min bucket को 'full', 'partial', 'sparse', या 'history' label देता है
--    quality matrix:
--      gap_minutes overlap: हाँ      → 'partial'
--      live coverage ≥ 10s, no gap   → 'full'
--      live coverage <10s, no gap    → 'sparse'   (low-liquidity)
--      ohlc_1m_filled से आया         → 'history'
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quotes_1m_raw AS
WITH live_1m AS (
    -- 1-second buckets को 1-minute में aggregate करो
    SELECT
        time_bucket('1 minute', bucket) AS ts,
        exchange,
        symbol,
        first(open, bucket)             AS open,
        max(high)                       AS high,
        min(low)                        AS low,
        last(close, bucket)             AS close,
        COALESCE(SUM(volume), 0)::BIGINT AS volume,
        COUNT(*)                        AS sec_count   -- कितने seconds में data था
    FROM ohlc_1s
    GROUP BY 1, 2, 3
),
gap_minutes AS (
    -- collector_gaps के सारे minutes (1-min boundary पर)
    SELECT DISTINCT gs AS ts
    FROM collector_gaps g
    CROSS JOIN LATERAL generate_series(
        time_bucket('1 minute', g.started_at),
        time_bucket('1 minute', g.ended_at),
        '1 minute'::interval
    ) AS gs
)
-- Live data (हर minute के लिए quality label)
SELECT
    lm.ts, lm.exchange, lm.symbol,
    lm.open, lm.high, lm.low, lm.close, lm.volume,
    'tick' AS source,
    CASE
        WHEN gm.ts IS NOT NULL    THEN 'partial'   -- gap overlap → partial
        WHEN lm.sec_count >= 10   THEN 'full'      -- अच्छी coverage
        ELSE                            'sparse'   -- low-liquidity
    END AS quality
FROM   live_1m lm
LEFT JOIN gap_minutes gm ON gm.ts = lm.ts
UNION ALL
-- History data (सिर्फ़ तब include जब live missing या gap में हो)
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

-- ---------------------------------------------------------------------
-- 8) v_quotes_1m — clean ML training feed
--    DISTINCT ON से duplicates हटाता है, quality priority के साथ:
--      full > sparse > history > partial
--    (live real data हमेशा > history fill, sparse > history)
-- ---------------------------------------------------------------------
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
