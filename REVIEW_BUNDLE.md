# NSE Tick Collector — Full Code Review Bundle (v6)

> **For ChatGPT / Gemini / Claude / Qwen / Kimi reviewers:**
> यह **v6** है — round-1 (12) + round-2 (15) + round-3 (10) + round-4 (20) + round-5 (10) = **67 review issues fixed**।
>
> **v6 round-5 के prominent changes:**
> - `--today`: filter `ended_at >= today_start_ist` (Friday→Monday gap अब cover होगा)
> - `fetch_history` returns `(df, had_error)` — 0 rows ≠ API error
> - `gap_filler` pool retry (10-attempt) symmetric with collector
> - NaN volume safe cast via `pd.isna()`
> - DataFrame ts column robust (datetime/t/dt + warning log)
> - `_hash_jsonb` `allow_nan=True`
> - `replay_gap_spool` single-transaction per file
> - Per-symbol startup gap: `min(max(ts) per symbol)`
> - Holiday cache `_LOADED_YEARS` tracking
> - Empty `closed_exchanges` semantics fixed
>
> **अब verify करना है:**
> 1. `--today` overlap edge cases — what if gap started AND ended yesterday?
> 2. `fetch_history` empty-result semantics — broker variations?
> 3. Per-symbol min(max_ts) gap query performance on huge tables?
> 4. NaN volume cast — DataFrame edge cases (None vs NaN vs 'N/A' string)?
> 5. Pool retry exponential — DB-down for 30 min scenarios?

---

## Project structure

```
Trading-TimescaleDB-Short-2/
├── schema.sql          UNIQUE indexes + smart views with quality
├── collector.py        Per-symbol startup-gap, allow_nan hash, single-tx replay
├── gap_filler.py       --today overlap, (df, error) tuple, pool retry, NaN-safe
├── symbols.txt
├── requirements.txt    + pandas
├── .env.example
├── .gitignore
└── README.md
```

---

## File 1 of 8 — `schema.sql`

```sql
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
```

---

## File 2 of 8 — `collector.py`  (856 lines)

```python
"""
NSE Tick Collector  (v5 — round-4 review fixes)
================================================
v5 sudhar (round-4 — ChatGPT + Gemini + Qwen + Kimi):

CRITICAL:
  1. tick_uid: 32-char SHA-256, includes depth+raw content hash, stable
     float formatting (f"{x:.6f}"), preserves None vs 0 vs ""
  2. Auto-startup-gap-record: last DB tick से अब तक का gap on first connect
     → gap_filler Monday-morning bars भर देगा (कोई fake spike नहीं)
  3. Pool creation retry (DB startup fragility)
  4. Flusher local buf lock-protected (hang पर recover होता है)
  5. _parse_timestamp range check ('1.5' → 1970 garbage rokta है)

HIGH:
  6. record_gap → ON CONFLICT DO NOTHING (replay duplicate safe)
  7. Watchdog default 60s (low-liquidity false positives)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
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
WATCHDOG_TIMEOUT    = int(os.getenv("WATCHDOG_TIMEOUT_SEC", "60"))   # v5: 30→60
SPOOL_DIR           = Path(os.getenv("SPOOL_DIR", "./spool"))
STORE_RAW_PAYLOAD   = os.getenv("STORE_RAW_PAYLOAD", "false").lower() == "true"
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO").upper()

if MODE not in ("quote", "depth"):
    sys.stderr.write(
        f"FATAL: MODE='{MODE}' invalid. Allowed: 'quote' or 'depth'.\n"
    )
    sys.exit(2)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("collector")

IST                  = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN          = dtime(9, 15)
MARKET_CLOSE         = dtime(15, 30)
MARKET_OPEN_GRACE    = dtime(9, 30)

# Sanity: timestamps within 2000-01-01 .. 2100-01-01 unix-sec range
_TS_MIN_SEC = 946684800
_TS_MAX_SEC = 4102444800


# ---------------------------------------------------------------------------
# DB SQL — v5: ON CONFLICT (ts, tick_uid) safe replay
# ---------------------------------------------------------------------------
INSERT_SQL = """
INSERT INTO ticks (ts, exchange, symbol, stream_type, ltp, volume, tick_volume,
                   bid, ask, open, high, low, close, depth, raw, tick_uid)
VALUES (%(ts)s, %(exchange)s, %(symbol)s, %(stream_type)s, %(ltp)s,
        %(volume)s, %(tick_volume)s, %(bid)s, %(ask)s,
        %(open)s, %(high)s, %(low)s, %(close)s, %(depth)s, %(raw)s, %(tick_uid)s)
ON CONFLICT (ts, tick_uid) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Pool with retry — v5 FIX (Kimi #10): DB startup fragility
# ---------------------------------------------------------------------------
def make_pool() -> ConnectionPool:
    pool = ConnectionPool(
        PG_DSN, min_size=1, max_size=4, open=False,
        kwargs={"autocommit": True},
    )
    last_err: Exception | None = None
    for attempt in range(10):
        try:
            pool.open(wait=True, timeout=10)
            log.info("DB pool ready (attempt %d)", attempt + 1)
            return pool
        except Exception as e:                                  # noqa: BLE001
            last_err = e
            delay = min(30, 2 ** attempt)
            log.warning("DB pool open fail (attempt %d): %s — retry in %ds",
                        attempt + 1, e, delay)
            time.sleep(delay)
    pool.close()
    raise RuntimeError(f"DB unreachable after 10 retries: {last_err}")


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
# Per-symbol cumulative-volume tracker
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


def get_last_db_tick_ts(pool: ConnectionPool) -> datetime | None:
    """
    v6 FIX (ChatGPT #5): per-symbol min(max(ts)) — सबसे stale symbol का
    last-seen time वाला conservative gap रिकॉर्ड करते हैं ताकि कोई
    symbol coverage छूटे न।
    """
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute("""
                WITH per_symbol AS (
                    SELECT symbol, max(ts) AS max_ts
                    FROM   ticks
                    WHERE  ts >= now() - INTERVAL '7 days'
                    GROUP  BY symbol
                )
                SELECT min(max_ts) FROM per_symbol
            """)
            row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:                                      # noqa: BLE001
        log.warning("get_last_db_tick_ts failed: %s", e)
        return None


def compute_tick_volume(symbol: str, cum_vol: int | None,
                        tick_ts: datetime) -> int | None:
    """
    Returns tick की actual quantity (cumulative day-volume का delta)।

    Edge cases:
      • Day rollover (tick_day != prev_day):
          - ≤ 9:30 IST → cum_vol (genuine market-open volume, छोटा spike OK)
          - > 9:30 IST → 0; auto-startup-gap-record से gap_filler भर देगा
      • Mid-day glitch (cum < prev) → 0, prev preserve
      • First-ever tick (no prior data):
          - ≤ 9:30 IST → cum_vol
          - > 9:30 IST → 0
      • Normal increase → cum - prev
    """
    if cum_vol is None or cum_vol < 0:
        return None

    tick_ist = tick_ts.astimezone(IST)
    tick_day = tick_ist.date()
    in_open_window = tick_ist.time() <= MARKET_OPEN_GRACE

    with _VOL_LOCK:
        prev      = _LAST_CUM_VOL.get(symbol)
        prev_day  = _LAST_TICK_DAY.get(symbol)

        # (1) day rollover
        if prev_day is not None and tick_day != prev_day:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            if in_open_window:
                return cum_vol
            log.debug("rollover late-start %s @ %s — return 0 (gap_filler)",
                      symbol, tick_ist.time())
            return 0

        # (2) mid-day glitch
        if prev is not None and cum_vol < prev:
            log.debug("volume glitch %s: cum=%d < prev=%d (same day) — ignore",
                      symbol, cum_vol, prev)
            return 0

        # (3) first-ever tick
        if prev is None:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            if in_open_window:
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


_NUMERIC_TS_RE = re.compile(r"^-?\d+(\.\d+)?$")


def _parse_timestamp(ts_raw: Any) -> datetime:
    """
    String/int/float → tz-aware UTC datetime.
    v5 FIX: range check (2000-2100) — '1.5' जैसे garbage rokte हैं।
    """
    if isinstance(ts_raw, (int, float)):
        ts_val = ts_raw / 1000.0 if abs(ts_raw) > 1e12 else float(ts_raw)
        if _TS_MIN_SEC <= ts_val <= _TS_MAX_SEC:
            try:
                return datetime.fromtimestamp(ts_val, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                pass
        return datetime.now(timezone.utc)

    if isinstance(ts_raw, str):
        s = ts_raw.strip()
        if not s:
            return datetime.now(timezone.utc)

        # numeric string (strict) — v5 FIX
        if _NUMERIC_TS_RE.fullmatch(s):
            try:
                n = float(s)
                ts_val = n / 1000.0 if abs(n) > 1e12 else n
                if _TS_MIN_SEC <= ts_val <= _TS_MAX_SEC:
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


# ---------------------------------------------------------------------------
# tick_uid — v5 FIX: SHA-256 32-char, depth+raw content, stable formatting,
#                     None vs 0 distinction
# ---------------------------------------------------------------------------
def _norm(v: Any) -> str:
    """None → '<N>', else repr() — preserves 0 vs None vs '' distinction."""
    return "<N>" if v is None else repr(v)


def _norm_float(v: Any) -> str:
    if v is None:
        return "<N>"
    try:
        return f"{float(v):.6f}"
    except (TypeError, ValueError):
        return "<NAN>"


def _hash_jsonb(j: Any) -> str:
    """v6 FIX (Qwen): allow_nan=True — broker के NaN/Inf floats crash न करें।"""
    if j is None:
        return ""
    obj = j.obj if isinstance(j, Jsonb) else j
    try:
        return hashlib.sha256(
            json.dumps(obj, sort_keys=True, default=str,
                       allow_nan=True).encode("utf-8")
        ).hexdigest()[:16]
    except (TypeError, ValueError):
        return "<unhashable>"


def make_tick_uid(row: dict[str, Any]) -> str:
    parts = (
        str(row.get("exchange", "")),
        str(row.get("symbol", "")),
        str(row.get("stream_type", "")),
        row["ts"].isoformat() if isinstance(row.get("ts"), datetime)
                              else str(row.get("ts")),
        _norm_float(row.get("ltp")),
        _norm(row.get("volume")),
        _norm_float(row.get("bid")),
        _norm_float(row.get("ask")),
        _hash_jsonb(row.get("depth")),
        _hash_jsonb(row.get("raw")),
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


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
# Disk spool — per-hour file
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
    if not out.get("tick_uid"):
        out["tick_uid"] = make_tick_uid(out)
    return out


def replay_spool(pool: ConnectionPool) -> None:
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
    """v6 FIX (Qwen): single-transaction per file (no N+1 connections)."""
    if not SPOOL_DIR.exists():
        return
    files = sorted(SPOOL_DIR.glob("gapspool_*.jsonl"))
    if not files:
        return
    log.info("replaying %d gap spool files", len(files))
    for fp in files:
        try:
            count = 0
            with pool.connection() as con, con.cursor() as cur:
                for line in fp.read_text().splitlines():
                    if not line.strip():
                        continue
                    d = json.loads(line)
                    started = datetime.fromisoformat(d["started_at"])
                    ended   = datetime.fromisoformat(d["ended_at"])
                    cur.execute(
                        "INSERT INTO collector_gaps "
                        "(started_at, ended_at, reason) VALUES (%s, %s, %s) "
                        "ON CONFLICT (started_at, ended_at, COALESCE(reason, '')) "
                        "DO NOTHING",
                        (started, ended, d.get("reason")),
                    )
                    count += 1
            log.info("replayed %d gaps from %s", count, fp.name)
            fp.unlink()
        except Exception as e:                                  # noqa: BLE001
            log.error("gap replay failed %s: %s", fp.name, e)


# ---------------------------------------------------------------------------
# Flusher — v5 FIX: lock-protected local buf (hang पर recover होता है)
# ---------------------------------------------------------------------------
class Flusher(threading.Thread):
    def __init__(self, q: queue.Queue, pool: ConnectionPool,
                 stop_evt: threading.Event):
        super().__init__(daemon=True, name="flusher")
        self.q, self.pool, self.stop_evt = q, pool, stop_evt
        self.last_flush_ts = time.monotonic()
        self.buf: list[dict[str, Any]] = []
        self.buf_lock = threading.Lock()

    def run(self) -> None:
        while True:
            done = self.stop_evt.is_set()
            if done and self.q.empty() and not self._buf_count():
                break
            try:
                item = self.q.get(timeout=0.2)
                with self.buf_lock:
                    self.buf.append(item)
            except queue.Empty:
                pass

            now = time.monotonic()
            with self.buf_lock:
                buf_len = len(self.buf)
            should_flush = (
                buf_len >= BATCH_SIZE
                or (buf_len > 0 and now - self.last_flush_ts >= FLUSH_INTERVAL_SEC)
                or (done and buf_len > 0)
            )
            if should_flush:
                with self.buf_lock:
                    rows = self.buf
                    self.buf = []
                self._flush(rows)
                self.last_flush_ts = now
        log.info("flusher stopped")

    def _buf_count(self) -> int:
        with self.buf_lock:
            return len(self.buf)

    def take_buffer_snapshot(self) -> list[dict[str, Any]]:
        with self.buf_lock:
            snap = self.buf
            self.buf = []
        return snap

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
# Gap recorder — v5 FIX: ON CONFLICT DO NOTHING
# ---------------------------------------------------------------------------
def record_gap(pool: ConnectionPool, started: datetime,
               ended: datetime, reason: str) -> None:
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(
                "INSERT INTO collector_gaps (started_at, ended_at, reason) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (started_at, ended_at, COALESCE(reason, '')) "
                "DO NOTHING",
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
    replay_gap_spool(pool)
    replay_spool(pool)
    seed_last_cum_vol(pool)

    # v5 FIX: auto-startup-gap recording
    last_disconnect_at: datetime | None = get_last_db_tick_ts(pool)
    if last_disconnect_at:
        log.info("startup: last DB tick %s — gap will be recorded on first connect",
                 last_disconnect_at)

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
                reason = ("startup_gap" if last_disconnect_at < connected_at - timedelta(minutes=5)
                          else "ws_reconnect")
                record_gap(pool, last_disconnect_at, now_utc, reason)
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
        # v5 FIX: drain BOTH flusher's local buf AND queue
        flusher_buf = flusher.take_buffer_snapshot()
        queue_remaining: list[dict[str, Any]] = []
        while True:
            try:
                queue_remaining.append(tick_q.get_nowait())
            except queue.Empty:
                break
        all_remaining = flusher_buf + queue_remaining
        if all_remaining:
            log.error("flusher hang — spooling %d remaining ticks "
                      "(buf=%d, queue=%d)",
                      len(all_remaining), len(flusher_buf), len(queue_remaining))
            try:
                _spool_rows(all_remaining)
            except Exception as e:                               # noqa: BLE001
                log.critical("final spool fail: %s — %d rows lost",
                             e, len(all_remaining))

    pool.close()
    log.info("bye")


if __name__ == "__main__":
    run()
```

---

## File 3 of 8 — `gap_filler.py`  (463 lines)

```python
"""
Gap Filler  (v5 — round-4 review fixes)
========================================
v5 sudhar:
  * --today: IST timezone-aware day boundary (Qwen #2 fix)
  * df_rows_in_window: list/dict response defensive parsing (Gemini, Qwen #4)
  * NSE holidays: closed_exchanges + multi-format date parser + dynamic years
  * fill_one_gap: failure ONLY on API errors (failed_days), not 0-rows
                  (15:25-15:35 type gaps no longer false-fail)
  * SQL composition via psycopg.sql (no string formatting injection)
  * pool.close() in finally
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from dotenv import load_dotenv
from openalgo import api
from psycopg import sql
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
# SQL — v5 FIX: psycopg.sql composition (no string formatting)
# ---------------------------------------------------------------------------
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


def build_gap_query(today: bool, retry: bool) -> sql.SQL:
    conditions: list[sql.Composable] = [sql.SQL("filled = FALSE")]
    if today:
        # v6 FIX (Gemini, Kimi #3): `ended_at` use करो — Friday→Monday वाला
        # startup_gap भी आज के ended_at से match होगा।
        conditions.append(sql.SQL(
            "ended_at >= (now() AT TIME ZONE 'Asia/Kolkata')::date "
            "AT TIME ZONE 'Asia/Kolkata'"
        ))
    if retry:
        conditions.append(sql.SQL("attempts > 0"))
    return sql.SQL(
        "SELECT id, started_at, ended_at, reason, attempts, failed_symbols "
        "FROM collector_gaps WHERE {conds} ORDER BY started_at"
    ).format(conds=sql.SQL(" AND ").join(conditions))


# ---------------------------------------------------------------------------
# NSE holiday cache
# v6 FIX (Qwen): cached years track करो — repeat API calls avoid
# ---------------------------------------------------------------------------
_HOLIDAYS:    set[date] | None = None
_LOADED_YEARS: set[int]        = set()


def _parse_holiday_date(s: Any) -> date | None:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s[:10]).date()
    except ValueError:
        return None


def _load_holidays(client, years: set[int] | None = None) -> set[date]:
    global _HOLIDAYS, _LOADED_YEARS
    if _HOLIDAYS is None:
        _HOLIDAYS = set()

    target_years = years or {datetime.now(IST).year}
    # v6 FIX (Qwen): सिर्फ़ missing years के लिए API call
    years_to_fetch = target_years - _LOADED_YEARS
    if not years_to_fetch:
        return _HOLIDAYS

    for y in years_to_fetch:
        try:
            resp = client.holidays(year=y)
        except Exception as e:                                   # noqa: BLE001
            log.warning("holidays(%d) API fail: %s", y, e)
            _LOADED_YEARS.add(y)   # don't retry this run
            continue

        items: list = []
        if isinstance(resp, dict):
            for k in ("data", "holidays", "result"):
                v = resp.get(k)
                if isinstance(v, list):
                    items = v
                    break
        elif isinstance(resp, list):
            items = resp

        for h in items:
            if not isinstance(h, dict):
                continue

            # v6 FIX (Kimi #9): empty list = "no exchanges closed" = NSE open
            closed = h.get("closed_exchanges")
            if closed is None:
                closed = h.get("closed")
            if isinstance(closed, list):
                if not closed:
                    continue                          # NSE open
                if EXCHANGE not in closed:
                    continue

            htype = (h.get("holiday_type") or h.get("type") or "").upper()
            if htype and htype not in ("TRADING_HOLIDAY", "TRADING", ""):
                continue

            d_str = h.get("date") or h.get("holiday_date") or h.get("day")
            d = _parse_holiday_date(d_str)
            if d:
                _HOLIDAYS.add(d)

        _LOADED_YEARS.add(y)

    log.info("loaded NSE holidays for years %s (cache=%d)",
             sorted(years_to_fetch), len(_HOLIDAYS))
    return _HOLIDAYS


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
    start_ist = start.astimezone(IST)
    end_ist   = end.astimezone(IST)
    cur_day = start_ist.date()
    end_day = end_ist.date()
    while cur_day <= end_day:
        if is_trading_day(cur_day, client):
            day_open  = datetime.combine(cur_day, MARKET_OPEN,  tzinfo=IST)
            day_close = datetime.combine(cur_day, MARKET_CLOSE, tzinfo=IST)
            if start_ist <= day_close and end_ist >= day_open:
                return True
        cur_day += timedelta(days=1)
    return False


def fetch_history(client, symbol: str, sd: str, ed: str
                  ) -> tuple[Any | None, bool]:
    """
    v6 FIX (Kimi #4): tuple (df, had_api_error) return करो ताकि
    "0 rows" को "API failure" से distinguish कर सकें।

    Returns:
      (df, False)   — successful, df has rows
      (None, False) — successful, but no rows (illiquid / no-trade day)
      (None, True)  — API exception thrown
    """
    try:
        df = client.history(
            symbol     = symbol,
            exchange   = EXCHANGE,
            interval   = INTERVAL,
            start_date = sd,
            end_date   = ed,
        )
    except Exception as e:                                       # noqa: BLE001
        log.warning("history fail %s [%s..%s]: %s", symbol, sd, ed, e)
        return None, True                                        # API error

    if df is None:
        return None, False                                       # no error, no data

    # if not a DataFrame, try to convert
    if not hasattr(df, "iterrows"):
        try:
            if isinstance(df, dict):
                df = (df.get("data") or df.get("candles") or
                      df.get("history") or [])
            df = pd.DataFrame(df)
            # v6 FIX (Kimi #6): broker-specific column names cover करो
            for col in ("timestamp", "time", "ts", "date",
                        "datetime", "t", "dt"):
                if col in df.columns:
                    df = df.set_index(col)
                    break
            else:
                # कोई recognized ts column नहीं — index check करो
                if not (hasattr(df.index, "to_pydatetime")
                        or (len(df.index) > 0
                            and isinstance(df.index[0],
                                           (datetime, pd.Timestamp)))):
                    log.warning("history df %s: no ts column. cols=%s",
                                symbol, list(df.columns)[:8])
                    return None, False
        except Exception as e:                                   # noqa: BLE001
            log.warning("history df conversion fail %s: %s", symbol, e)
            return None, True
        if not hasattr(df, "iterrows"):
            return None, False

    if len(df) == 0:
        return None, False                                       # successfully empty

    return df, False


def df_rows_in_window(df, start: datetime, end: datetime,
                      symbol: str) -> list[tuple]:
    start_floor = floor_minute(start)
    end_floor   = floor_minute(end)
    rows: list[tuple] = []
    for ts, row in df.iterrows():
        # v5 FIX: defensive ts conversion
        if hasattr(ts, "to_pydatetime"):
            py_ts = ts.to_pydatetime()
        elif isinstance(ts, str):
            try:
                py_ts = datetime.fromisoformat(ts)
            except ValueError:
                continue
        elif isinstance(ts, datetime):
            py_ts = ts
        else:
            continue
        if py_ts.tzinfo is None:
            py_ts = py_ts.replace(tzinfo=IST)
        ts_utc = py_ts.astimezone(timezone.utc)
        if not (start_floor <= ts_utc <= end_floor):
            continue
        try:
            # v6 FIX (Gemini): NaN volume safe cast
            vol_raw = row["volume"] if "volume" in row else None
            if vol_raw is None or pd.isna(vol_raw):
                vol_clean: int | None = None
            else:
                vol_clean = int(float(vol_raw))
            rows.append((
                ts_utc, EXCHANGE, symbol,
                float(row["open"]), float(row["high"]),
                float(row["low"]),  float(row["close"]),
                vol_clean,
            ))
        except (KeyError, TypeError, ValueError) as e:
            log.warning("row parse fail %s @ %s: %s", symbol, ts_utc, e)
    return rows


def fetch_history_chunked(client, symbol: str,
                          start: datetime, end: datetime
                          ) -> tuple[list[tuple], list[date]]:
    """
    v6 FIX (Kimi #4): failed_days अब सिर्फ़ true API errors के लिए;
    successful empty results (illiquid stock, no-trade day) failure नहीं।
    """
    start_ist = start.astimezone(IST).date()
    end_ist   = end.astimezone(IST).date()
    all_rows: list[tuple] = []
    failed_days: list[date] = []
    for day in date_range(start_ist, end_ist):
        if not is_trading_day(day, client):
            continue
        sd = ed = day.isoformat()
        df, had_error = fetch_history(client, symbol, sd, ed)
        if had_error:
            failed_days.append(day)
            continue
        if df is None:
            # API succeeded but no rows — not a failure
            continue
        all_rows.extend(df_rows_in_window(df, start, end, symbol))
    return all_rows, failed_days


def fill_one_gap(client, pool, gid: int, start: datetime, end: datetime,
                 symbols: list[str]) -> tuple[int, list[str], str | None]:
    """
    v5 FIX: failure ONLY when API actually errored (failed_days non-empty)。
    0-rows + no API error = success (e.g. 15:25-15:35 — after-close, no data
    expected; market holiday gap; illiquid stock).
    """
    inserted = 0
    failed: list[str] = []
    last_err: str | None = None

    for sym in symbols:
        rows, failed_days = fetch_history_chunked(client, sym, start, end)

        if failed_days:
            failed.append(sym)
            last_err = f"API failed for {len(failed_days)} day(s) on {sym}"
            log.warning("gap %d | %s: API fail on %d days", gid, sym, len(failed_days))
            continue

        if not rows:
            log.debug("gap %d | %s: 0 bars (no API error — likely after-hours/holiday)",
                      gid, sym)
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


def make_pool_with_retry() -> ConnectionPool:
    """v6 FIX (Kimi #5): symmetric with collector — DB startup retry।"""
    pool = ConnectionPool(PG_DSN, min_size=1, max_size=2, open=False,
                          kwargs={"autocommit": True})
    last_err: Exception | None = None
    for attempt in range(10):
        try:
            pool.open(wait=True, timeout=10)
            log.info("DB pool ready (attempt %d)", attempt + 1)
            return pool
        except Exception as e:                                  # noqa: BLE001
            last_err = e
            delay = min(30, 2 ** attempt)
            log.warning("DB pool open fail (attempt %d): %s — retry in %ds",
                        attempt + 1, e, delay)
            time.sleep(delay)
    pool.close()
    raise RuntimeError(f"DB unreachable after 10 retries: {last_err}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--today", action="store_true",
                    help="आज (IST) ended_at वाले unfilled gaps")
    ap.add_argument("--retry", action="store_true",
                    help="जिनमें पहले attempts हुए हैं वही")
    args = ap.parse_args()

    symbols = load_symbols(SYMBOLS_FILE)
    log.info("symbols: %d", len(symbols))

    pool = make_pool_with_retry()
    try:
        query = build_gap_query(today=args.today, retry=args.retry)
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(query)
            gaps = cur.fetchall()

        if not gaps:
            log.info("कोई unfilled gap नहीं — सब साफ़।")
            return

        log.info("processing %d gaps", len(gaps))
        client = api(api_key=OPENALGO_API_KEY, host=OPENALGO_HOST)

        # v5 FIX: dynamic year range from gap dates
        gap_years = {g[1].astimezone(IST).year for g in gaps} | \
                    {g[2].astimezone(IST).year for g in gaps}
        _load_holidays(client, years=gap_years)

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
psycopg[binary,pool]>=3.2.0
python-dotenv>=1.0.0
tenacity>=8.2.0
pandas>=2.0.0
```

---

## File 5 of 8 — `.env.example`

```bash
# NSE Tick Collector — .env template (v5)
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

# Mode: 'quote' (Level-1) | 'depth' (Level-2 order book)
# 'both' rejected (CAGG duplicate counting)
MODE=quote

BATCH_SIZE=500
FLUSH_INTERVAL_SEC=1
RECONNECT_MAX_DELAY=60

# Watchdog: इतनी देर tick न आए तो reconnect
# v5: default 60 (पहले 30 था — low-liquidity false positives)
WATCHDOG_TIMEOUT_SEC=60

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
# NSE Tick Collector — Hindi गाइड (v6)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v6 में ChatGPT + Gemini + Qwen + Kimi के **5 rounds का review** लागू है —
> कुल **67+ bugs fix**।

---

## v1 → v6 का सफर

- **v2 (round-1: 12 fixes):** watchdog, parse_tick, tick_volume, spool, IST
- **v3 (round-2: 15 fixes):** day-rollover, depth arrays, MODE=both reject
- **v4 (round-3: 10 fixes):** ALTER migration, late-start guard, tick_uid
- **v5 (round-4: 20 fixes):** SHA-256, auto-startup-gap, holiday API, quality
- **v6 (round-5: 10 fixes):** see below

### v6 round-5 fixes

| # | Bug                                                                    | Fix |
|---|------------------------------------------------------------------------|-----|
| 1 | **`--today` Friday→Monday gap miss** — startup_gap के `started_at` Friday था, today filter skip कर देता था | `ended_at >= today_start_ist` (Monday morning का startup_gap match होगा) |
| 2 | **`fetch_history` 0-rows = API error** — illiquid stocks falsely marked failed | `(df, had_error)` tuple — empty result success, exception failure |
| 3 | **`gap_filler` no pool retry** — DB temporary down पर cron crash | `make_pool_with_retry()` (10-attempt exponential, symmetric with collector) |
| 4 | **`pandas` NaN volume → int(NaN) ValueError** — valid row drop | `pd.isna()` check + safe casting |
| 5 | **DataFrame ts column missing** ("datetime"/"t"/"dt") → silent ALL ROWS skip | Extended column names + warning log if no ts found |
| 6 | **`_hash_jsonb` NaN/Inf → ValueError** | `allow_nan=True` |
| 7 | **`replay_gap_spool` N+1 connections** | Single transaction per file |
| 8 | **Per-symbol startup gap** — global max(ts) misses symbols with older data | `min(max(ts) per symbol)` (conservative coverage) |
| 9 | **Holiday cache redundant API calls** — cached years still re-fetched | `_LOADED_YEARS` set tracks which years done |
| 10 | **Empty `closed_exchanges` list semantics** | Empty list = NSE open (not a holiday) |

### v5 round-4 fixes (20 critical issues)

| # | Bug                                                                    | Fix |
|---|------------------------------------------------------------------------|-----|
| 1 | `tick_uid` hash incomplete — depth/raw missing, unstable float, 0/None collapse | 32-char SHA-256 + content hash for depth/raw + `_norm()` (None vs 0) + `f"{x:.6f}"` |
| 2 | **Monday-restart volume loss** — day rollover after 9:30 lost data    | Auto-startup-gap recording: last DB tick से अब तक का gap on first connect |
| 3 | `collector_gaps` no UNIQUE → spool replay duplicates                   | UNIQUE(started_at, ended_at, COALESCE(reason,'')) + ON CONFLICT DO NOTHING |
| 4 | `gap_minutes` SQL last-partial-minute miss                             | `time_bucket` floor on BOTH ends of generate_series |
| 5 | `v_quotes_1m_raw` history filter missing (always included)             | `WHERE lm.ts IS NULL OR gm.ts IS NOT NULL` |
| 6 | Quality priority wrong (`full > history > sparse > partial`)            | New: `full > sparse > history > partial` (live real > history fill) |
| 7 | `--today` IST timezone bug (UTC truncation)                            | `(now() AT TIME ZONE 'Asia/Kolkata')::date AT TIME ZONE 'Asia/Kolkata'` |
| 8 | `df_rows_in_window` crashes on list/dict response                      | Defensive: convert to DataFrame; handle `to_pydatetime`/string/datetime |
| 9 | NSE holidays parsing fragile (settlement holidays counted, date format breaks) | `closed_exchanges`/`holiday_type` filter + multi-format parser + dynamic year range |
| 10 | Multi-day partial gap success                                          | `failed_days` tracking; failure ONLY on API errors |
| 11 | SQL injection pattern in gap_filler                                    | `psycopg.sql` composition |
| 12 | `_parse_timestamp` matches non-timestamps ("1.5" → 1970)               | Strict regex + range check (2000-2100) |
| 13 | Pool startup fragility (DB down at startup = crash)                    | Retry loop with exponential backoff (10 attempts) |
| 14 | Flusher buffer loss on hang                                            | Lock-protected `buf` + `take_buffer_snapshot()` on shutdown |
| 15 | `requirements.txt` missing pandas, version mismatch                    | `pandas>=2.0.0`, `psycopg-pool>=3.2` |
| 16 | `.env.example` v3 header                                               | v5 |
| 17 | Watchdog 30s false positives in low liquidity                          | Default 60s |
| 18 | `fill_one_gap` over-strict (15:25-15:35 false-fail)                    | Failure only on API errors, not 0-rows |
| 19 | Holiday cache static years                                             | Dynamic from gap dates |
| 20 | Tick_uid 16-char (~10 days at scale possibly)                          | 32 chars (128-bit safe for billions) |

---

## आपके सवालों के जवाब (एक नज़र)

### Connection कटा — data कैसे recover होगा? (5 परतें)
1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)
2. **Watchdog** — silent disconnect detection (60s tickless = reconnect)
3. **Disk spool** — DB down = JSONL per-hour file; startup auto-replay; `tick_uid` ON CONFLICT safe
4. **Gap fill** — `collector_gaps` log + UNIQUE constraint; `gap_filler.py` 1-min bars from history (NSE-holiday aware, day-by-day chunked, partial-failure tracked)
5. **Auto-startup-gap** (v5 NEW) — restart पर `last DB tick → now` gap automatic record होती है, gap_filler morning bars भर देगा

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
                                          │  with lock-protected│
                                          │  buf for hang safety│
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (tick_uid)│
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │  UNIQUE
                                          │   ─ v_quotes_1m_raw │
                                          │   ─ v_quotes_1m     │  full > sparse > history > partial
                                          └─────────────────────┘
```

**Volume delta logic (v5):**
```
compute_tick_volume:
   day rollover (tick_day != prev_day):
       ≤ 9:30 IST → cum_vol (genuine market-open volume)
       > 9:30 IST → 0 (auto-startup-gap covers it via gap_filler)
   intra-day decrease → 0 (broker glitch, prev preserve)
   normal → cum-prev
```

**Auto-startup-gap (v5 NEW):**
```
collector startup:
   last_db_ts = max(ts in ticks WHERE ts >= 7 days ago)
   last_disconnect_at = last_db_ts
   on first connect:
       record_gap(last_disconnect_at, now, "startup_gap")
   gap_filler --today fills these morning bars from history API
```

---

## Setup

### 1. TimescaleDB
```bash
sudo apt install postgresql-16
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

> **पुराने (v2/v3/v4) DB से upgrade?** सिर्फ़ views drop करें (CAGG को नहीं!):
> ```sql
> DROP VIEW IF EXISTS v_quotes_1m, v_quotes_1m_raw;
> ```
> फिर `psql -f schema.sql` — ALTER TABLE से नए columns idempotent जुड़ेंगे।

### 3. OpenAlgo + Python env
```bash
# OpenAlgo install से अपने broker से login + API key copy
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env       # API key + password भरें
```

---

## रोज़ चलाना

```bash
# सुबह 9 बजे
python collector.py

# शाम — gap fill (NSE holidays auto-skip; day-by-day chunking)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book)
`.env` में `MODE=depth`। `depth` JSONB column में पूरा order book।

```sql
SELECT
    ts, symbol,
    (depth -> 'bids' -> 0 ->> 'price')::FLOAT      AS bid_price,
    (depth -> 'bids' -> 0 ->> 'quantity')::INT     AS bid_qty,
    (depth -> 'asks' -> 0 ->> 'price')::FLOAT      AS ask_price,
    (depth -> 'asks' -> 0 ->> 'quantity')::INT     AS ask_qty
FROM ticks WHERE depth IS NOT NULL ORDER BY ts DESC LIMIT 10;
```

---

## ML Training Quick Reference

```sql
-- Strict ML feed (recommended) — full live OR history fill
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY' AND ts >= '2026-01-01'
  AND  quality IN ('full', 'history')
ORDER  BY ts;

-- Low-liquidity stocks include
SELECT * FROM v_quotes_1m
WHERE  quality != 'partial'   -- sparse OK अगर gap नहीं था
ORDER  BY ts;

-- Volume sanity check
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0 AND ltp > 0
GROUP  BY symbol HAVING count(*) > 100;

-- Gap forensics
SELECT id, started_at, ended_at, attempts, reason,
       array_length(failed_symbols, 1) AS n_fail
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

Cron (शाम 4 बजे gap fill):
```cron
0 16 * * 1-5 /home/marketdata/Trading-TimescaleDB-Short-2/.venv/bin/python /home/marketdata/Trading-TimescaleDB-Short-2/gap_filler.py --today >> /var/log/gap_filler.log 2>&1
```

OpenAlgo को भी अपनी systemd service बनाएँ ताकि वह crash होने पर auto-restart हो।

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          v5: tick_uid UNIQUE, gaps UNIQUE, smart views with quality
├── collector.py        v5: 32-char SHA-256 uid, auto-startup-gap, pool retry,
│                            flusher buf protection, _parse_timestamp range check
├── gap_filler.py       v5: psycopg.sql composition, IST --today, dataframe defensive,
│                            failed_days tracking, holiday closed_exchanges check
├── symbols.txt         Nifty 50 default
├── requirements.txt    + pandas
├── .env.example
├── .gitignore
├── README.md           यह file
└── REVIEW_BUNDLE.md    Single-file bundle for AI review
```

> **Status:** v6 — **67+ review issues fixed across 5 rounds**। Production-deploy ready।

बस — market hours में `collector.py` चलाते रहें; production-grade tick data रोज़ का इकट्ठा होता रहेगा।
````

---

## End of bundle — Total: 8 files, 1897 lines.
