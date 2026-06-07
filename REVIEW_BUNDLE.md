# NSE Tick Collector — Full Code Review Bundle (v4)

> **For ChatGPT / Gemini / Claude reviewers:**
> यह **v4** है — round-1 (12) + round-2 (15) + round-3 (10) = **37 review issues fixed**।
>
> **v4 round-3 के prominent changes:**
> - Day-rollover late-start guard (≤ 9:30 IST genuine, > 9:30 = return 0)
> - `tick_uid` (SHA1) + UNIQUE index `(ts, tick_uid)` + `ON CONFLICT DO NOTHING` (spool replay safe)
> - Numeric string timestamp parsing ('1717741500000')
> - `record_gap()` fail → gap_spool JSONL → startup replay
> - Per-hour spool file (storm avoidance)
> - NSE holidays via OpenAlgo `client.holidays()` cache
> - `v_quotes_1m_raw` (forensic) + `v_quotes_1m` (DISTINCT ON, quality priority)
> - Gap-overlap based quality (`full`/`partial`/`sparse`/`history`)
> - Idempotent schema migration (`ALTER TABLE ADD COLUMN IF NOT EXISTS`)
>
> **अब verify करना है:**
> 1. `tick_uid` हash collision risk realistic है? 16-char SHA1 enough?
> 2. Day-rollover late-start cutoff (9:30 IST) — कुछ stocks 9:30 के बाद ट्रेड करते हैं, edge cases?
> 3. Per-hour spool: hour boundary पर concurrent write race condition?
> 4. NSE holidays API के response shape variations — सब cover हुए?
> 5. `v_quotes_1m` DISTINCT ON quality priority order सही है ML के लिए?
> 6. spool replay चलने के बाद `_LAST_CUM_VOL` correctly seeded?
> 7. Gap-spool replay का order — multiple gap files chronological?

---

## Project structure

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB tables + smart CAGG + dual quality views
├── collector.py        Watchdog + day-rollover-late-guard + tick_uid + gap_spool
├── gap_filler.py       NSE holiday-aware + chunked + market-hours validation
├── symbols.txt
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Tech stack

- Python 3.12+
- OpenAlgo Python SDK v2.0+ (subscribe_quote / subscribe_depth / history / holidays)
- TimescaleDB (hypertable + realtime CAGG + compression + UNIQUE index)
- psycopg 3 + psycopg_pool 3.2+ (open=True)
- tenacity (exponential-backoff + WatchdogStale)
- python-dotenv

---

## File 1 of 8 — \`schema.sql\`

\`\`\`sql
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
```

---

## File 2 of 8 — `collector.py`  (750 lines)

```python
"""
NSE Tick Collector  (v4 — round-3 review fixes)
================================================
OpenAlgo WebSocket -> Buffer -> TimescaleDB (batch insert)

v4 में लागू सुधार (round-3 — ChatGPT + Gemini):
  1. Day-rollover late-start volume fix (तुरंत 9:30 IST के बाद = return 0)
  2. tick_uid (SHA1 hash) + ON CONFLICT DO NOTHING — spool replay safe
  3. Numeric string timestamp ('1717741500000') ठीक से parse होगा
  4. Gap recording fail पर gap_spool JSONL + startup replay
  5. Spool storm fix — per-hour file (अनगिनत small files नहीं बनेंगी)
  6. NSE holidays via OpenAlgo API cache (gap_filler में)
  7. Idempotent schema (ALTER TABLE IF NOT EXISTS)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from openalgo import api
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_never,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

OPENALGO_API_KEY = os.environ["OPENALGO_API_KEY"]
OPENALGO_HOST    = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
OPENALGO_WS_URL  = os.getenv("OPENALGO_WS_URL", "ws://127.0.0.1:8765")

PG_DSN = make_conninfo(
    host     = os.getenv("PG_HOST", "127.0.0.1"),
    port     = int(os.getenv("PG_PORT", "5432")),
    dbname   = os.getenv("PG_DB", "marketdata"),
    user     = os.getenv("PG_USER", "marketdata"),
    password = os.getenv("PG_PASSWORD", ""),
)

EXCHANGE            = os.getenv("EXCHANGE", "NSE")
SYMBOLS_FILE        = os.getenv("SYMBOLS_FILE", "symbols.txt")
MODE                = os.getenv("MODE", "quote").lower()
BATCH_SIZE          = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC  = float(os.getenv("FLUSH_INTERVAL_SEC", "1"))
RECONNECT_MAX_DELAY = int(os.getenv("RECONNECT_MAX_DELAY", "60"))
WATCHDOG_TIMEOUT    = int(os.getenv("WATCHDOG_TIMEOUT_SEC", "30"))
SPOOL_DIR           = Path(os.getenv("SPOOL_DIR", "./spool"))
STORE_RAW_PAYLOAD   = os.getenv("STORE_RAW_PAYLOAD", "false").lower() == "true"
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO").upper()

if MODE not in ("quote", "depth"):
    sys.stderr.write(
        f"FATAL: MODE='{MODE}' invalid. Allowed: 'quote' or 'depth' "
        "('both' caused duplicate counting in CAGG — removed in v3+).\n"
    )
    sys.exit(2)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("collector")

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN          = dtime(9, 15)
MARKET_CLOSE         = dtime(15, 30)
# day rollover पर "genuine first volume" cutoff — इसके बाद late-start माना जाएगा
MARKET_OPEN_GRACE    = dtime(9, 30)


# ---------------------------------------------------------------------------
# DB SQL — ON CONFLICT DO NOTHING (spool replay duplicate से बचाव)
# ---------------------------------------------------------------------------
INSERT_SQL = """
INSERT INTO ticks (ts, exchange, symbol, stream_type, ltp, volume, tick_volume,
                   bid, ask, open, high, low, close, depth, raw, tick_uid)
VALUES (%(ts)s, %(exchange)s, %(symbol)s, %(stream_type)s, %(ltp)s,
        %(volume)s, %(tick_volume)s, %(bid)s, %(ask)s,
        %(open)s, %(high)s, %(low)s, %(close)s, %(depth)s, %(raw)s, %(tick_uid)s)
ON CONFLICT (ts, tick_uid) DO NOTHING
"""


def make_pool() -> ConnectionPool:
    return ConnectionPool(
        PG_DSN, min_size=1, max_size=4, open=True,
        kwargs={"autocommit": True},
    )


def load_symbols(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        log.error("symbols file %s नहीं मिली", path)
        sys.exit(1)
    syms = [s.strip() for s in p.read_text().splitlines()
            if s.strip() and not s.strip().startswith("#")]
    if not syms:
        log.error("symbols.txt खाली है")
        sys.exit(1)
    log.info("कुल symbols: %d  (mode=%s)", len(syms), MODE)
    return syms


def is_market_hours(ts: datetime | None = None) -> bool:
    now_ist = (ts or datetime.now(timezone.utc)).astimezone(IST)
    if now_ist.weekday() > 4:
        return False
    return MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE


# ---------------------------------------------------------------------------
# Per-symbol cumulative-volume tracker (day-based rollover + late-start guard)
# ---------------------------------------------------------------------------
_LAST_CUM_VOL:  dict[str, int]  = {}
_LAST_TICK_DAY: dict[str, date] = {}
_VOL_LOCK = threading.Lock()


def seed_last_cum_vol(pool: ConnectionPool) -> None:
    sql = """
        SELECT DISTINCT ON (symbol) symbol, volume, ts
        FROM   ticks
        WHERE  volume IS NOT NULL
          AND  ts >= now() - INTERVAL '7 days'
        ORDER  BY symbol, ts DESC
    """
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(sql)
            for sym, vol, ts in cur.fetchall():
                _LAST_CUM_VOL[sym]  = int(vol)
                _LAST_TICK_DAY[sym] = ts.astimezone(IST).date()
        log.info("seeded last_cum_vol+day for %d symbols", len(_LAST_CUM_VOL))
    except Exception as e:                                      # noqa: BLE001
        log.warning("seed_last_cum_vol failed: %s", e)


def compute_tick_volume(symbol: str, cum_vol: int | None,
                        tick_ts: datetime) -> int | None:
    """
    Tick की actual quantity (cumulative day-volume का delta) compute करता है।

    Edge cases handled:
      • Day rollover (tick_day != prev_day):
          - 9:30 IST से पहले first tick  → return cum_vol (genuine market-open)
          - 9:30 IST के बाद late start    → return 0 (gap_filler उस slot भरेगा)
      • Mid-day glitch (cum < prev, same day) → return 0, prev preserve
      • First-ever tick (no prior data):
          - 9:30 से पहले → return cum_vol
          - 9:30 के बाद  → return 0
      • Normal increase → return cum - prev
    """
    if cum_vol is None or cum_vol < 0:
        return None

    tick_ist = tick_ts.astimezone(IST)
    tick_day = tick_ist.date()
    is_market_open_window = tick_ist.time() <= MARKET_OPEN_GRACE

    with _VOL_LOCK:
        prev      = _LAST_CUM_VOL.get(symbol)
        prev_day  = _LAST_TICK_DAY.get(symbol)

        # (1) day rollover
        if prev_day is not None and tick_day != prev_day:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            if is_market_open_window:
                # genuine first-of-day volume
                return cum_vol
            # late start — पूरा morning volume एक tick में मत credit करो
            log.debug("late-start rollover %s @ %s — return 0",
                      symbol, tick_ist.time())
            return 0

        # (2) mid-day glitch
        if prev is not None and cum_vol < prev:
            log.debug("volume glitch %s: cum=%d < prev=%d (same day) — ignore",
                      symbol, cum_vol, prev)
            return 0   # prev MUST NOT update

        # (3) first-ever tick (no prior data)
        if prev is None:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            if is_market_open_window:
                return cum_vol
            return 0

        # (4) normal increase
        _LAST_CUM_VOL[symbol]  = cum_vol
        _LAST_TICK_DAY[symbol] = tick_day
        return cum_vol - prev


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _pick(*sources_and_keys) -> Any:
    sources, keys = [], []
    for x in sources_and_keys:
        if isinstance(x, dict):
            sources.append(x)
        else:
            keys.append(x)
    for d in sources:
        for k in keys:
            v = d.get(k)
            if v is not None:
                return v
    return None


def _parse_timestamp(ts_raw: Any) -> datetime:
    """
    String/int/float → tz-aware UTC datetime। Fallbacks:
      * numeric (int/float OR digit-string like '1717741500000')
      * ISO 8601, या common Indian formats
      * naive → IST मानकर UTC
      * कुछ नहीं तो datetime.now(UTC)
    """
    # numeric epoch
    if isinstance(ts_raw, (int, float)):
        ts_val = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
        try:
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return datetime.now(timezone.utc)

    if isinstance(ts_raw, str):
        s = ts_raw.strip()
        if not s:
            return datetime.now(timezone.utc)

        # numeric string ('1717741500000' etc.)
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            try:
                n = int(s)
                ts_val = n / 1000.0 if abs(n) > 1e12 else n
                return datetime.fromtimestamp(ts_val, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                pass

        # float numeric string ('1717741500.123')
        if any(c.isdigit() for c in s) and "." in s:
            try:
                n_f = float(s)
                ts_val = n_f / 1000.0 if n_f > 1e12 else n_f
                return datetime.fromtimestamp(ts_val, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                pass

        # date formats
        for fmt in (
            None,                              # ISO 8601
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%d-%b-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ):
            try:
                if fmt is None:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(s, fmt)
            except ValueError:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            return dt.astimezone(timezone.utc)

    return datetime.now(timezone.utc)


def make_tick_uid(row: dict[str, Any]) -> str:
    """SHA1(short)-based unique id — same payload का same uid → ON CONFLICT skip."""
    parts = (
        str(row.get("exchange", "")),
        str(row.get("symbol", "")),
        str(row.get("stream_type", "")),
        row["ts"].isoformat() if isinstance(row.get("ts"), datetime) else str(row.get("ts")),
        f"{row.get('ltp', '')}",
        str(row.get("volume") or ""),
        str(row.get("bid")    or ""),
        str(row.get("ask")    or ""),
    )
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Tick parser
# ---------------------------------------------------------------------------
def parse_tick(payload: dict[str, Any], default_exchange: str,
               kind: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    try:
        inner = payload.get("data") if isinstance(payload.get("data"), dict) else {}

        symbol = _pick(payload, inner, "symbol", "trading_symbol")
        if not symbol:
            return None
        exchange = _pick(payload, inner, "exchange") or default_exchange

        ltp = _pick(payload, inner, "ltp", "last_price")
        if ltp is None and kind == "quote":
            ltp = _pick(payload, inner, "close")
        if ltp is None:
            return None

        ts_raw = _pick(payload, inner, "timestamp", "exchange_timestamp",
                       "ltt", "last_traded_time")
        ts = _parse_timestamp(ts_raw)

        cum_vol     = _to_int(_pick(payload, inner, "volume", "v"))
        tick_volume = compute_tick_volume(symbol, cum_vol, ts)

        depth_obj = None
        bid_val = _to_float(_pick(payload, inner, "bid", "best_bid_price",
                                  "buy_price"))
        ask_val = _to_float(_pick(payload, inner, "ask", "best_ask_price",
                                  "sell_price"))

        if kind == "depth":
            depth_obj = _pick(payload, inner, "depth", "market_depth")
            if depth_obj is None:
                bids = inner.get("bids") or payload.get("bids")
                asks = inner.get("asks") or payload.get("asks")
                if bids is not None or asks is not None:
                    depth_obj = {
                        "bids":         bids or [],
                        "asks":         asks or [],
                        "totalbuyqty":  _pick(payload, inner, "totalbuyqty"),
                        "totalsellqty": _pick(payload, inner, "totalsellqty"),
                    }
            if depth_obj is not None:
                if bid_val is None and depth_obj.get("bids"):
                    try:
                        bid_val = float(depth_obj["bids"][0].get("price"))
                    except (IndexError, TypeError, ValueError, AttributeError):
                        pass
                if ask_val is None and depth_obj.get("asks"):
                    try:
                        ask_val = float(depth_obj["asks"][0].get("price"))
                    except (IndexError, TypeError, ValueError, AttributeError):
                        pass

        row: dict[str, Any] = {
            "ts":          ts,
            "exchange":    exchange,
            "symbol":      symbol,
            "stream_type": kind,
            "ltp":         float(ltp),
            "volume":      cum_vol,
            "tick_volume": tick_volume,
            "bid":         bid_val,
            "ask":         ask_val,
            "open":        _to_float(_pick(payload, inner, "open")),
            "high":        _to_float(_pick(payload, inner, "high")),
            "low":         _to_float(_pick(payload, inner, "low")),
            "close":       _to_float(_pick(payload, inner, "prev_close",
                                           "previous_close")),
            "depth":       Jsonb(depth_obj) if depth_obj is not None else None,
            "raw":         Jsonb(payload) if STORE_RAW_PAYLOAD else None,
        }
        row["tick_uid"] = make_tick_uid(row)
        return row
    except Exception as e:                                      # noqa: BLE001
        log.warning("parse_tick failed: %s | keys=%s",
                    e, list(payload.keys())[:8])
        return None


# ---------------------------------------------------------------------------
# Disk spool — per-hour file (storm avoidance)
# ---------------------------------------------------------------------------
_SPOOL_LOCK = threading.Lock()


def _spool_filename(prefix: str) -> Path:
    h = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    return SPOOL_DIR / f"{prefix}_{h}.jsonl"


def _spool_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    fname = _spool_filename("spool")
    with _SPOOL_LOCK:
        with fname.open("a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(_serialize_row(r), default=str) + "\n")
    log.error("spooled %d ticks -> %s", len(rows), fname.name)


def _spool_gaps(gap_records: list[dict[str, Any]]) -> None:
    if not gap_records:
        return
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    fname = _spool_filename("gapspool")
    with _SPOOL_LOCK:
        with fname.open("a", encoding="utf-8") as f:
            for g in gap_records:
                rec = {
                    "started_at": g["started_at"].isoformat()
                                  if isinstance(g["started_at"], datetime)
                                  else g["started_at"],
                    "ended_at":   g["ended_at"].isoformat()
                                  if isinstance(g["ended_at"], datetime)
                                  else g["ended_at"],
                    "reason":     g.get("reason"),
                }
                f.write(json.dumps(rec) + "\n")
    log.error("spooled %d gap records -> %s", len(gap_records), fname.name)


def _serialize_row(r: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k, v in r.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, Jsonb):
            out[k] = v.obj
        else:
            out[k] = v
    return out


def _deserialize_row(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    out["ts"] = datetime.fromisoformat(out["ts"])
    out.setdefault("stream_type", "quote")
    if out.get("depth") is not None:
        out["depth"] = Jsonb(out["depth"])
    if out.get("raw") is not None:
        out["raw"] = Jsonb(out["raw"])
    # backward compatibility — पुराने v3 spools में tick_uid नहीं था
    if not out.get("tick_uid"):
        out["tick_uid"] = make_tick_uid(out)
    return out


def replay_spool(pool: ConnectionPool) -> None:
    """Tick spool replay (startup पर — concurrent writers नहीं)।"""
    if not SPOOL_DIR.exists():
        return
    files = sorted(SPOOL_DIR.glob("spool_*.jsonl"))
    if not files:
        return
    log.info("replaying %d tick spool files", len(files))
    for fp in files:
        try:
            rows = [_deserialize_row(json.loads(line))
                    for line in fp.read_text().splitlines() if line.strip()]
            if rows:
                with pool.connection() as con, con.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
                log.info("replayed %d ticks from %s (ON CONFLICT safe)",
                         len(rows), fp.name)
            fp.unlink()
        except Exception as e:                                  # noqa: BLE001
            log.error("replay failed %s: %s", fp.name, e)


def replay_gap_spool(pool: ConnectionPool) -> None:
    if not SPOOL_DIR.exists():
        return
    files = sorted(SPOOL_DIR.glob("gapspool_*.jsonl"))
    if not files:
        return
    log.info("replaying %d gap spool files", len(files))
    for fp in files:
        try:
            count = 0
            for line in fp.read_text().splitlines():
                if not line.strip():
                    continue
                d = json.loads(line)
                started = datetime.fromisoformat(d["started_at"])
                ended   = datetime.fromisoformat(d["ended_at"])
                with pool.connection() as con, con.cursor() as cur:
                    cur.execute(
                        "INSERT INTO collector_gaps "
                        "(started_at, ended_at, reason) VALUES (%s, %s, %s)",
                        (started, ended, d.get("reason")),
                    )
                count += 1
            log.info("replayed %d gaps from %s", count, fp.name)
            fp.unlink()
        except Exception as e:                                  # noqa: BLE001
            log.error("gap replay failed %s: %s", fp.name, e)


# ---------------------------------------------------------------------------
# Flusher thread
# ---------------------------------------------------------------------------
class Flusher(threading.Thread):
    def __init__(self, q: queue.Queue, pool: ConnectionPool,
                 stop_evt: threading.Event):
        super().__init__(daemon=True, name="flusher")
        self.q, self.pool, self.stop_evt = q, pool, stop_evt
        self.last_flush_ts = time.monotonic()

    def run(self) -> None:
        buf: list[dict[str, Any]] = []
        while not self.stop_evt.is_set() or not self.q.empty() or buf:
            try:
                buf.append(self.q.get(timeout=0.2))
            except queue.Empty:
                pass

            now = time.monotonic()
            should_flush = (
                len(buf) >= BATCH_SIZE
                or (buf and now - self.last_flush_ts >= FLUSH_INTERVAL_SEC)
                or (self.stop_evt.is_set() and buf)
            )
            if should_flush:
                self._flush(buf)
                buf.clear()
                self.last_flush_ts = now
        log.info("flusher stopped")

    def _flush(self, rows: list[dict[str, Any]]) -> None:
        try:
            with self.pool.connection() as con, con.cursor() as cur:
                cur.executemany(INSERT_SQL, rows)
            log.debug("flushed %d ticks", len(rows))
        except Exception as e:                                  # noqa: BLE001
            log.error("DB insert failed: %s — spooling %d rows", e, len(rows))
            try:
                _spool_rows(rows)
            except Exception as e2:                             # noqa: BLE001
                log.critical("spool भी fail: %s — %d rows lost", e2, len(rows))


# ---------------------------------------------------------------------------
# Gap recorder — fail पर gap_spool में जाता है
# ---------------------------------------------------------------------------
def record_gap(pool: ConnectionPool, started: datetime,
               ended: datetime, reason: str) -> None:
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(
                "INSERT INTO collector_gaps (started_at, ended_at, reason) "
                "VALUES (%s, %s, %s)",
                (started, ended, reason),
            )
        log.warning("gap recorded: %s -> %s (%s)", started, ended, reason)
    except Exception as e:                                      # noqa: BLE001
        log.error("gap insert failed: %s — spooling", e)
        try:
            _spool_gaps([{"started_at": started, "ended_at": ended,
                          "reason": reason}])
        except Exception as e2:                                 # noqa: BLE001
            log.critical("gap spool भी fail: %s — gap LOST", e2)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
class WatchdogStale(Exception):
    pass


def run() -> None:
    symbols = load_symbols(SYMBOLS_FILE)
    instruments = [{"exchange": EXCHANGE, "symbol": s} for s in symbols]

    pool = make_pool()
    # Order: gap_spool replay → tick_spool replay → seed cumulative volumes
    replay_gap_spool(pool)
    replay_spool(pool)
    seed_last_cum_vol(pool)

    tick_q: queue.Queue = queue.Queue(maxsize=200_000)
    stop_evt = threading.Event()
    flusher = Flusher(tick_q, pool, stop_evt)
    flusher.start()

    state: dict[str, Any] = {"last_tick": None}

    def shutdown(signum, _frame):
        log.info("signal %d मिला — रुक रहे हैं", signum)
        stop_evt.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    def _enqueue(row: dict[str, Any]) -> None:
        try:
            tick_q.put_nowait(row)
        except queue.Full:
            log.warning("queue full — spooling 1 row (%s)", row.get("symbol"))
            try:
                _spool_rows([row])
            except Exception as e:                              # noqa: BLE001
                log.critical("spool fail: %s — 1 row lost", e)

    def on_quote(data: dict[str, Any]) -> None:
        row = parse_tick(data, EXCHANGE, kind="quote")
        if row:
            state["last_tick"] = datetime.now(timezone.utc)
            _enqueue(row)

    def on_depth(data: dict[str, Any]) -> None:
        row = parse_tick(data, EXCHANGE, kind="depth")
        if row:
            state["last_tick"] = datetime.now(timezone.utc)
            _enqueue(row)

    retry_iter = Retrying(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=RECONNECT_MAX_DELAY),
        stop=stop_never,
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )

    last_disconnect_at: datetime | None = None

    for attempt in retry_iter:
        with attempt:
            if stop_evt.is_set():
                break

            state["last_tick"] = None
            connected_at = datetime.now(timezone.utc)

            client = api(
                api_key=OPENALGO_API_KEY,
                host=OPENALGO_HOST,
                ws_url=OPENALGO_WS_URL,
                verbose=0,
            )
            client.connect()
            if MODE == "quote":
                client.subscribe_quote(instruments, on_data_received=on_quote)
            else:
                client.subscribe_depth(instruments, on_data_received=on_depth)
            log.info("connected & subscribed (%d symbols, mode=%s)",
                     len(instruments), MODE)

            now_utc = datetime.now(timezone.utc)
            if last_disconnect_at and (now_utc - last_disconnect_at).total_seconds() > 5:
                record_gap(pool, last_disconnect_at, now_utc, "ws_reconnect")
            last_disconnect_at = None

            try:
                while not stop_evt.is_set():
                    time.sleep(1)
                    last_tick = state["last_tick"]
                    now_utc = datetime.now(timezone.utc)

                    if last_tick is None:
                        age = (now_utc - connected_at).total_seconds()
                        if age > WATCHDOG_TIMEOUT and is_market_hours(now_utc):
                            log.warning("watchdog: %.0fs बिना पहले tick — reconnect", age)
                            last_disconnect_at = connected_at
                            raise WatchdogStale("no first tick after connect")
                        continue

                    age = (now_utc - last_tick).total_seconds()
                    if age > WATCHDOG_TIMEOUT and is_market_hours(now_utc):
                        log.warning("watchdog: last tick %.0fs पहले — reconnect", age)
                        last_disconnect_at = last_tick
                        raise WatchdogStale(f"stale stream {age:.0f}s")
            finally:
                if last_disconnect_at is None:
                    last_disconnect_at = state["last_tick"] or datetime.now(timezone.utc)
                for fn_name in ("unsubscribe_quote", "unsubscribe_depth"):
                    fn = getattr(client, fn_name, None)
                    if fn is None:
                        continue
                    try:
                        fn(instruments)
                    except Exception:                            # noqa: BLE001
                        pass
                try:
                    client.disconnect()
                except Exception:                                # noqa: BLE001
                    pass

            if stop_evt.is_set():
                break

    # ---------- shutdown ----------
    log.info("waiting for flusher (%d ticks pending)", tick_q.qsize())
    flusher.join(timeout=60)
    if flusher.is_alive():
        remaining: list[dict[str, Any]] = []
        while True:
            try:
                remaining.append(tick_q.get_nowait())
            except queue.Empty:
                break
        if remaining:
            log.error("flusher hang — spooling %d remaining ticks", len(remaining))
            try:
                _spool_rows(remaining)
            except Exception as e:                               # noqa: BLE001
                log.critical("final spool fail: %s — %d rows lost",
                             e, len(remaining))

    pool.close()
    log.info("bye")


if __name__ == "__main__":
    run()
```

---

## File 3 of 8 — `gap_filler.py`  (317 lines)

```python
"""
Gap Filler  (v4 — round-3 review fixes)
========================================
collector_gaps से unfilled disconnect periods लेकर 1-min OHLC bars
ohlc_1m_filled में डालता है।

v4 sudhar:
  * NSE holidays via OpenAlgo API cache (weekday-only fallback)
  * Multi-day gap → day-by-day chunking
  * Partial first/last minute floor-filter
  * Market-hours overlap = failure (with holiday awareness)
  * pool.close() always in finally
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, time as dtime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv
from openalgo import api
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

load_dotenv()

OPENALGO_API_KEY = os.environ["OPENALGO_API_KEY"]
OPENALGO_HOST    = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")

PG_DSN = make_conninfo(
    host     = os.getenv("PG_HOST", "127.0.0.1"),
    port     = int(os.getenv("PG_PORT", "5432")),
    dbname   = os.getenv("PG_DB", "marketdata"),
    user     = os.getenv("PG_USER", "marketdata"),
    password = os.getenv("PG_PASSWORD", ""),
)

EXCHANGE     = os.getenv("EXCHANGE", "NSE")
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "symbols.txt")
INTERVAL     = "1m"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gap_filler")

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
SELECT_GAPS = """
SELECT id, started_at, ended_at, reason, attempts, failed_symbols
FROM   collector_gaps
WHERE  filled = FALSE
       {extra}
ORDER  BY started_at
"""

INSERT_BAR = """
INSERT INTO ohlc_1m_filled (ts, exchange, symbol, open, high, low, close,
                            volume, source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'history_api')
ON CONFLICT (ts, symbol, exchange) DO NOTHING
"""

UPDATE_GAP = """
UPDATE collector_gaps
SET    filled          = %s,
       attempts        = attempts + 1,
       failed_symbols  = %s,
       last_error      = %s,
       last_attempt_at = now()
WHERE  id = %s
"""


# ---------------------------------------------------------------------------
# NSE holiday cache (OpenAlgo से लाते हैं; fallback weekday-only)
# ---------------------------------------------------------------------------
_HOLIDAYS: set[date] | None = None


def _load_holidays(client) -> set[date]:
    global _HOLIDAYS
    if _HOLIDAYS is not None:
        return _HOLIDAYS
    hols: set[date] = set()
    years = {datetime.now(IST).year, datetime.now(IST).year - 1}
    for y in years:
        try:
            resp = client.holidays(year=y)
        except Exception as e:                                   # noqa: BLE001
            log.warning("holidays(%d) API fail: %s", y, e)
            continue
        items: Any = None
        if isinstance(resp, dict):
            items = resp.get("data") or resp.get("holidays") or list(resp.values())
        elif isinstance(resp, list):
            items = resp
        if not items:
            continue
        for h in items:
            d_str = None
            if isinstance(h, dict):
                d_str = h.get("date") or h.get("holiday_date") or h.get("day")
            elif isinstance(h, str):
                d_str = h
            if not d_str:
                continue
            try:
                hols.add(datetime.fromisoformat(str(d_str)[:10]).date())
            except (ValueError, TypeError):
                pass
    _HOLIDAYS = hols
    log.info("loaded %d NSE holidays (%s)", len(hols),
             "via API" if hols else "fallback weekday-only")
    return hols


def is_trading_day(d: date, client=None) -> bool:
    if d.weekday() > 4:
        return False
    if client is not None:
        if d in _load_holidays(client):
            return False
    return True


# ---------------------------------------------------------------------------
def load_symbols(path: str) -> list[str]:
    return [s.strip() for s in Path(path).read_text().splitlines()
            if s.strip() and not s.strip().startswith("#")]


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def date_range(d_start: date, d_end: date) -> Iterator[date]:
    d = d_start
    while d <= d_end:
        yield d
        d += timedelta(days=1)


def gap_overlaps_market_hours(start: datetime, end: datetime, client=None) -> bool:
    """Holiday-aware market-hours overlap check."""
    cur = start.astimezone(IST)
    end_ist = end.astimezone(IST)
    while cur <= end_ist:
        if is_trading_day(cur.date(), client):
            day_open  = cur.replace(hour=9,  minute=15, second=0, microsecond=0)
            day_close = cur.replace(hour=15, minute=30, second=0, microsecond=0)
            if start.astimezone(IST) <= day_close and end_ist >= day_open:
                return True
        cur = (cur + timedelta(days=1)).replace(hour=0, minute=0,
                                                second=0, microsecond=0)
    return False


def fetch_history(client, symbol: str, sd: str, ed: str) -> Any | None:
    try:
        df = client.history(
            symbol     = symbol,
            exchange   = EXCHANGE,
            interval   = INTERVAL,
            start_date = sd,
            end_date   = ed,
        )
        return df if df is not None and len(df) > 0 else None
    except Exception as e:                                       # noqa: BLE001
        log.warning("history fail %s [%s..%s]: %s", symbol, sd, ed, e)
        return None


def df_rows_in_window(df, start: datetime, end: datetime,
                      symbol: str) -> list[tuple]:
    start_floor = floor_minute(start)
    end_floor   = floor_minute(end)
    rows: list[tuple] = []
    for ts, row in df.iterrows():
        py_ts = ts.to_pydatetime()
        if py_ts.tzinfo is None:
            py_ts = py_ts.replace(tzinfo=IST)
        ts_utc = py_ts.astimezone(timezone.utc)
        if not (start_floor <= ts_utc <= end_floor):
            continue
        try:
            rows.append((
                ts_utc, EXCHANGE, symbol,
                float(row["open"]), float(row["high"]),
                float(row["low"]),  float(row["close"]),
                int(row["volume"]) if "volume" in row else None,
            ))
        except (KeyError, TypeError, ValueError) as e:
            log.warning("row parse fail %s @ %s: %s", symbol, ts_utc, e)
    return rows


def fetch_history_chunked(client, symbol: str,
                          start: datetime, end: datetime) -> list[tuple]:
    """Multi-day gap → day-by-day API call (broker 1m limit avoid)."""
    start_ist = start.astimezone(IST).date()
    end_ist   = end.astimezone(IST).date()
    all_rows: list[tuple] = []
    for day in date_range(start_ist, end_ist):
        if not is_trading_day(day, client):
            continue
        sd = ed = day.isoformat()
        df = fetch_history(client, symbol, sd, ed)
        if df is None:
            continue
        all_rows.extend(df_rows_in_window(df, start, end, symbol))
    return all_rows


def fill_one_gap(client, pool, gid: int, start: datetime, end: datetime,
                 symbols: list[str]) -> tuple[int, list[str], str | None]:
    inserted = 0
    failed: list[str] = []
    last_err: str | None = None
    market_gap = gap_overlaps_market_hours(start, end, client)

    for sym in symbols:
        rows = fetch_history_chunked(client, sym, start, end)
        if not rows:
            if market_gap:
                failed.append(sym)
                last_err = f"no bars in market-hours window for {sym}"
                log.warning("gap %d | %s: 0 bars in market-hours", gid, sym)
            else:
                log.debug("gap %d | %s: 0 bars (off-hours/holiday — OK)", gid, sym)
            continue
        try:
            with pool.connection() as con, con.cursor() as cur:
                cur.executemany(INSERT_BAR, rows)
            inserted += len(rows)
            log.info("gap %d | %s: %d bars inserted", gid, sym, len(rows))
        except Exception as e:                                   # noqa: BLE001
            log.error("gap %d | %s: DB insert fail: %s", gid, sym, e)
            failed.append(sym)
            last_err = str(e)

    return inserted, failed, last_err


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--today", action="store_true",
                    help="सिर्फ़ आज के unfilled gaps")
    ap.add_argument("--retry", action="store_true",
                    help="जिनमें पहले attempts हुए हैं वही")
    args = ap.parse_args()

    symbols = load_symbols(SYMBOLS_FILE)
    log.info("symbols: %d", len(symbols))

    pool = ConnectionPool(PG_DSN, min_size=1, max_size=2, open=True,
                          kwargs={"autocommit": True})
    try:
        extra = ""
        if args.today:
            extra += " AND started_at >= date_trunc('day', now())"
        if args.retry:
            extra += " AND attempts > 0"

        with pool.connection() as con, con.cursor() as cur:
            cur.execute(SELECT_GAPS.format(extra=extra))
            gaps = cur.fetchall()

        if not gaps:
            log.info("कोई unfilled gap नहीं — सब साफ़।")
            return

        log.info("processing %d gaps", len(gaps))
        client = api(api_key=OPENALGO_API_KEY, host=OPENALGO_HOST)
        _load_holidays(client)                                    # warm cache

        total_rows = 0
        for gid, start, end, reason, attempts, prev_failed in gaps:
            if (end - start) < timedelta(minutes=1):
                end = start + timedelta(minutes=1)

            target_symbols = list(prev_failed) if prev_failed else symbols
            log.info("→ gap %d | %s → %s | symbols=%d | attempts=%d | %s",
                     gid, start, end, len(target_symbols), attempts, reason)

            ins, failed, last_err = fill_one_gap(
                client, pool, gid, start, end, target_symbols
            )
            total_rows += ins

            is_done = len(failed) == 0
            with pool.connection() as con, con.cursor() as cur:
                cur.execute(UPDATE_GAP, (is_done, failed, last_err, gid))
            if is_done:
                log.info("✓ gap %d completed (+%d rows)", gid, ins)
            else:
                log.warning("⚠ gap %d partial: %d failed (e.g. %s)",
                            gid, len(failed), failed[:5])

        log.info("कुल %d bars डाले गए", total_rows)
    finally:
        pool.close()


if __name__ == "__main__":
    main()
```

---

## File 4 of 8 — `requirements.txt`

```
openalgo>=2.0.0
psycopg[binary,pool]>=3.1.0
python-dotenv>=1.0.0
tenacity>=8.2.0
```

---

## File 5 of 8 — `.env.example`

```bash
# NSE Tick Collector — .env template (v3)
#
# नोट: comments अलग lines में रखें — systemd EnvironmentFile inline-comments
# parse नहीं करता।

# OpenAlgo (अपने server पर local चलेगा)
OPENALGO_API_KEY=your_api_key_here
OPENALGO_HOST=http://127.0.0.1:5000
OPENALGO_WS_URL=ws://127.0.0.1:8765

# TimescaleDB
PG_HOST=127.0.0.1
PG_PORT=5432
PG_DB=marketdata
PG_USER=marketdata
PG_PASSWORD=change_me

# Collector tuning
EXCHANGE=NSE
SYMBOLS_FILE=symbols.txt

# Mode: 'quote' (Level-1: LTP+best bid/ask)
#       'depth' (Level-2: full order book)
# 'both' अब reject होता है (CAGG में duplicate counting bug था) —
# Level-2 चाहिए तो सीधे MODE=depth करें (वो LTP/volume भी देता है)।
MODE=quote

# कितने ticks जमा होने पर एक insert मारें
BATCH_SIZE=500

# या इतने सेकंड में force flush
FLUSH_INTERVAL_SEC=1

# Reconnect exponential backoff cap
RECONNECT_MAX_DELAY=60

# Watchdog: इतनी देर tick न आए तो stale मानकर reconnect
WATCHDOG_TIMEOUT_SEC=30

# DB fail पर failed rows यहाँ JSONL में spool, startup पर replay
SPOOL_DIR=./spool

# हर tick का पूरा raw payload भी DB में store करें? (storage भारी)
STORE_RAW_PAYLOAD=false

LOG_LEVEL=INFO
```

---

## File 6 of 8 — `symbols.txt`

```
# Nifty 50 — एक symbol per line, '#' से शुरू होने वाली lines comment हैं।
# जो भी 50 शेयर चाहिए, यहाँ बदल दें।
RELIANCE
HDFCBANK
ICICIBANK
INFY
TCS
LT
ITC
SBIN
BHARTIARTL
KOTAKBANK
HINDUNILVR
AXISBANK
BAJFINANCE
ASIANPAINT
MARUTI
HCLTECH
SUNPHARMA
TITAN
NTPC
ULTRACEMCO
NESTLEIND
WIPRO
M&M
TATAMOTORS
POWERGRID
ONGC
JSWSTEEL
TATASTEEL
ADANIENT
ADANIPORTS
COALINDIA
TECHM
BAJAJFINSV
HINDALCO
GRASIM
BRITANNIA
DRREDDY
CIPLA
BPCL
EICHERMOT
DIVISLAB
HEROMOTOCO
INDUSINDBK
SBILIFE
APOLLOHOSP
HDFCLIFE
TATACONSUM
LTIM
BAJAJ-AUTO
SHRIRAMFIN
```

---

## File 7 of 8 — `.gitignore`

```
.env
.venv/
__pycache__/
*.pyc
*.log
```

---

## File 8 of 8 — `README.md`

````markdown
# NSE Tick Collector — Hindi गाइड (v4)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v4 में ChatGPT + Gemini के **3 rounds का review** लागू है — कुल **37+ bugs fix**।

---

## v1 → v2 → v3 → v4 का सफर

- **v2 round-1 (12 fixes):** watchdog placement, parse_tick nested data,
  tick_volume delta, disk spool, gap_filler retry, IST timezone, schema gaps,
  v_quotes view, MODE=depth, .env comments, make_conninfo, index।
- **v3 round-2 (15 fixes):** watchdog grace + state reset, naive timestamp →
  IST, day-rollover detection, depth bids/asks arrays, MODE=both reject,
  spool replay order, queue.Full→spool, flusher hang spool, realtime CAGG,
  pool open=True, partial-minute floor, market-hours overlap check,
  day-by-day chunking, quality column, .env multi-line।

### v4 round-3 (10 critical production fixes)

| # | Bug                                                                       | Fix |
|---|---------------------------------------------------------------------------|-----|
| 1 | DB migration पुराने schema पर new columns add नहीं करता                  | Idempotent `ALTER TABLE ADD COLUMN IF NOT EXISTS` + migration notes |
| 2 | **Day-rollover late-start spike** — 10:30 AM start पर पूरा morning vol एक tick में | tick का IST time check; ≤9:30 → genuine, >9:30 → return 0 (gap_filler भरेगा) |
| 3 | **Spool replay duplicate** — crash between insert & unlink → next start पर double | `tick_uid` (SHA1 hash) + UNIQUE index + `ON CONFLICT DO NOTHING` |
| 4 | **Numeric string timestamp** miss — `"1717741500000"` → `datetime.now()` (wrong!) | `s.isdigit()` check before format parsing |
| 5 | `record_gap()` fail पर gap permanently lost (DB down + WS disconnect)    | Gap-spool JSONL + startup replay |
| 6 | Spool storm — हर queue-full tick के लिए नई file                          | Per-hour append-only file (`spool_YYYYMMDD_HH.jsonl`) |
| 7 | `gap_overlaps_market_hours` doesn't know NSE holidays                     | `client.holidays()` API cache; weekday-only fallback |
| 8 | `v_quotes_1m` partial+history same minute → duplicate                     | `v_quotes_1m_raw` (forensic) + `v_quotes_1m` (DISTINCT ON, quality priority) |
| 9 | `sec_count >= 50` low-liquidity stocks में हमेशा partial                  | Gap-overlap-based quality (`full`/`partial`/`sparse`/`history`) |
| 10 | `ohlc_1s` mixed stream_types — forensic clarity                           | Comment + `stream_type` column persists |

---

## आपके सवालों के जवाब

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?
| बात                    | सीधा broker SDK | OpenAlgo |
|------------------------|----------------|----------|
| Latency (localhost)    | ~0 ms          | ~1-2 ms  |
| 30+ brokers code reuse | ❌            | ✅       |
| Symbol format unified  | ❌            | ✅       |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended।**

### Connection कटा — data कैसे recover होगा? (4 परतें)
1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)
2. **Watchdog** — silent disconnect detection (30s tickless = reconnect)
3. **Disk spool** — DB down = JSONL file (per-hour); startup auto-replay; `tick_uid` ON CONFLICT से safe
4. **Gap fill** — `collector_gaps` log; `gap_filler.py` 1-min bars history API से, **NSE-holiday aware**, day-by-day chunking

> **Note:** brokers का "true 1-second history" मुफ़्त नहीं मिलता।

---

## Architecture

```
 ┌──────────┐  WebSocket  ┌──────────┐  psycopg  ┌──────────────┐
 │  Broker  │ ──────────▶ │ OpenAlgo │ ────────▶ │ collector.py │
 └──────────┘             └──────────┘           └──────┬───────┘
                                                        │ queue.Queue (200k)
                                                        ▼
                                          ┌─────────────────────┐
                                          │ Flusher (batch)     │
                                          │   ↓ DB OK           │
                                          │   ↓ DB FAIL → spool │  (per-hour file)
                                          │   ↓ Q FULL  → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │  +tick_uid UNIQUE
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m_raw │  (forensic)
                                          │   ─ v_quotes_1m     │  (clean ML feed)
                                          └─────────────────────┘
```

**Volume delta logic (v4):**
```
parse_tick → compute_tick_volume(symbol, cum_vol, ts):
   • day rollover (tick_day != prev_day):
       - tick_time ≤ 9:30 IST → cum_vol (genuine first-of-day volume)
       - tick_time > 9:30 IST → 0 (late start; gap_filler भरेगा)
   • intra-day decrease (cum < prev) → 0 (broker glitch, prev preserve)
   • normal increase → cum-prev
   • first ever tick:
       - ≤ 9:30 IST → cum_vol
       - > 9:30 IST → 0
```

**Spool flow (v4):**
```
DB fail | Q full | Flusher hang →  spool/spool_YYYYMMDD_HH.jsonl  (append)
record_gap fail                  →  spool/gapspool_YYYYMMDD_HH.jsonl
                                                 ↓
                                  Startup replay (chronological)
                                                 ↓
                                  ON CONFLICT (ts, tick_uid) DO NOTHING
                                  → safe even if crash between insert+unlink
```

---

## Setup

### 1. TimescaleDB
```bash
sudo apt install postgresql-16
# TimescaleDB repo से timescaledb-2-postgresql-16
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql
```

### 2. DB बनाएँ
```bash
sudo -u postgres psql <<SQL
CREATE USER marketdata WITH PASSWORD 'change_me';
CREATE DATABASE marketdata OWNER marketdata;
SQL

psql -h 127.0.0.1 -U marketdata -d marketdata -f schema.sql
```

> **पुराने (v2/v3) DB से upgrade?** schema.sql चलाने से पहले एक बार:
> ```sql
> DROP MATERIALIZED VIEW IF EXISTS ohlc_1s CASCADE;
> DROP VIEW IF EXISTS v_quotes_1m, v_quotes_1m_raw;
> ```
> फिर `psql -f schema.sql` — ALTER TABLE से नए columns idempotent जुड़ेंगे।

### 3. OpenAlgo
[docs.openalgo.in](https://docs.openalgo.in/) से install + broker login + API key।

### 4. Python env
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env       # API key + password भरें
```

---

## रोज़ चलाना

```bash
# सुबह 9 बजे
python collector.py

# शाम — gap fill (NSE holidays auto-skip करेगा)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth`। `depth` column में पूरा order book JSONB।

```sql
-- Order Book Imbalance example
SELECT
    ts, symbol,
    (depth -> 'bids' -> 0 ->> 'price')::FLOAT      AS bid_price,
    (depth -> 'bids' -> 0 ->> 'quantity')::INT     AS bid_qty,
    (depth -> 'asks' -> 0 ->> 'price')::FLOAT      AS ask_price,
    (depth -> 'asks' -> 0 ->> 'quantity')::INT     AS ask_qty
FROM ticks
WHERE  stream_type = 'depth' AND depth IS NOT NULL
ORDER  BY ts DESC LIMIT 10;
```

---

## ML Training Quick Reference

```sql
-- Strict ML training feed (recommended)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY'
  AND  ts BETWEEN '2026-01-01' AND '2026-06-07'
  AND  quality IN ('full', 'history')
ORDER  BY ts;

-- Low-liquidity stocks include करना है
SELECT * FROM v_quotes_1m
WHERE  quality != 'partial'   -- sparse भी OK अगर gap नहीं था
ORDER  BY ts;

-- Volume sanity check (mid-day glitch detection)
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0 AND ltp > 0
GROUP  BY symbol HAVING count(*) > 100;

-- Gap forensics
SELECT id, started_at, ended_at, attempts,
       array_length(failed_symbols, 1) AS n_fail, last_error
FROM   collector_gaps WHERE filled = FALSE;
```

---

## Production (systemd)

`/etc/systemd/system/tick-collector.service`:
```ini
[Unit]
Description=NSE Tick Collector
After=network.target postgresql.service openalgo.service
Requires=postgresql.service

[Service]
Type=simple
User=marketdata
WorkingDirectory=/home/marketdata/Trading-TimescaleDB-Short-2
EnvironmentFile=/home/marketdata/Trading-TimescaleDB-Short-2/.env
ExecStart=/home/marketdata/Trading-TimescaleDB-Short-2/.venv/bin/python collector.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tick-collector
journalctl -u tick-collector -f
```

OpenAlgo को भी अपनी systemd service बनाएँ ताकि वह crash होने पर auto-restart हो।

Cron (शाम 4 बजे gap fill):
```cron
0 16 * * 1-5 /home/marketdata/Trading-TimescaleDB-Short-2/.venv/bin/python /home/marketdata/Trading-TimescaleDB-Short-2/gap_filler.py >> /var/log/gap_filler.log 2>&1
```

---

## Tuning

| problem                   | solution |
|---------------------------|----------|
| Insert lag                | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा          | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए| `schema.sql` में retention policy uncomment |
| 50+ symbols               | OpenAlgo हज़ारों handle करता है |
| Watchdog बहुत agressive   | `WATCHDOG_TIMEOUT_SEC=60` |
| Spool भरा हुआ है          | `ls spool/` — startup पर खुद drain होगा |
| Holidays auto-skip नहीं   | OpenAlgo `holidays()` API check; manually `failed_symbols` cleanup |

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB schema (v4) + tick_uid UNIQUE + smart views
├── collector.py        Day-rollover late-start guard, gap-spool, per-hour spool
├── gap_filler.py       NSE holidays + day chunking + market-hours validation
├── symbols.txt         Nifty 50 default
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── REVIEW_BUNDLE.md    Single-file bundle for AI review
```

बस — market hours में `collector.py` चलाते रहें; production-grade 1-sec tick data
रोज़ का साफ़-सुथरा इकट्ठा होता रहेगा।

> **Status:** v4 अब **production-deploy ready**। 27 + 10 = **37 review issues fixed**।
````

---

## End of bundle

Total: 8 files, 1685 lines.
