# NSE Tick Collector — Full Code Review Bundle (v3)

> **For ChatGPT / Gemini / Claude reviewers:**
> यह **v3** है — round-1 (12 fixes) + round-2 (15 fixes) — कुल **27 review issues fix** हो चुके।
>
> **v3 round-2 के prominent changes:**
> - Watchdog: connect → no-first-tick grace timeout + state reset on reconnect
> - Timestamp: naive ISO/string → IST localize → UTC (5:30hr shift bug fix)
> - Volume: day-based rollover detection (intra-day decrease = glitch, prev preserve)
> - Depth: bids/asks arrays से top-of-book extract; `MODE=both` reject
> - Spool: replay BEFORE seed; queue.Full → spool; flusher hang → final spool
> - Gap filler: minute-floor filter, market-hours overlap = failure, day-by-day chunking
> - Schema: `stream_type`, `materialized_only=false`, smart `v_quotes_1m` with quality
> - Pool: `open=True` (psycopg-pool 3.2+)
>
> **अब verify करना है:**
> 1. क्या day-rollover detection में कोई edge case छूटा? (मार्केट छुट्टी का दिन, आधी रात restart...)
> 2. `parse_tick` की thread-safety? `_VOL_LOCK` enough है?
> 3. Spool replay sequence guarantee — duplicate insert होगा क्या? (no UNIQUE constraint by design)
> 4. Watchdog: market hours boundary पर (15:30:00 ठीक) reconnect-loop?
> 5. `v_quotes_1m` quality threshold (50 sec) — low-liquidity stocks में partial-flag गलत?
> 6. Gap filler `gap_overlaps_market_hours` weekend logic correct?
> 7. Performance — 50 symbols + depth mode (heavy payload) में queue saturate होगा?

---

## Project structure

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB tables + smart CAGG (sum tick_volume) + quality view
├── collector.py        Watchdog + day-rollover volume + spool fallback (Q-full + flusher-hang)
├── gap_filler.py       Per-symbol retry, day-by-day chunking, market-hours validation
├── symbols.txt         Nifty 50
├── requirements.txt
├── .env.example        Multi-line comments
├── .gitignore
└── README.md           Hindi guide
```

## Tech stack

- Python 3.12+
- OpenAlgo Python SDK v2.0+ (subscribe_quote / subscribe_depth / history)
- TimescaleDB (hypertable + realtime continuous aggregate + compression)
- psycopg 3 + psycopg_pool 3.2+ (open=True)
- tenacity (exponential-backoff + WatchdogStale)
- python-dotenv

---

## File 1 of 8 — `schema.sql`

```sql
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
```

---

## File 2 of 8 — `collector.py`  (651 lines)

```python
"""
NSE Tick Collector  (v3 — review fixes round 2)
================================================
OpenAlgo WebSocket -> Buffer -> TimescaleDB (batch insert)

v3 में लागू सुधार (ChatGPT + Gemini round-2 review से):
  A. Watchdog: 'first tick after connect' grace timeout + state reset on reconnect
  B. Timestamp: naive ISO/string → IST मानकर UTC convert (5:30hr shift bug fix)
  C. Volume glitch: day-based rollover detection (intra-day decrease = glitch,
     prev preserve, return 0 — fake spike नहीं बनेगा)
  D. Depth payload: bids/asks arrays से top-of-book bid/ask extract
  E. MODE=both अब reject (duplicate counting bug)
  F. Spool replay BEFORE seed_last_cum_vol (correct ordering)
  G. queue.Full → drop नहीं, spool करो
  H. Flusher join timeout के बाद remaining queue भी spool
  I. ConnectionPool open=True (psycopg-pool 3.2+ default change)
"""
from __future__ import annotations

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
MODE                = os.getenv("MODE", "quote").lower()       # quote | depth
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
        "('both' caused duplicate counting in CAGG — removed in v3).\n"
        "Use MODE=depth if you want LTP+volume+orderbook all together.\n"
    )
    sys.exit(2)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("collector")

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


# ---------------------------------------------------------------------------
# DB SQL
# ---------------------------------------------------------------------------
INSERT_SQL = """
INSERT INTO ticks (ts, exchange, symbol, stream_type, ltp, volume, tick_volume,
                   bid, ask, open, high, low, close, depth, raw)
VALUES (%(ts)s, %(exchange)s, %(symbol)s, %(stream_type)s, %(ltp)s,
        %(volume)s, %(tick_volume)s, %(bid)s, %(ask)s,
        %(open)s, %(high)s, %(low)s, %(close)s, %(depth)s, %(raw)s)
"""


def make_pool() -> ConnectionPool:
    # open=True: psycopg-pool 3.2+ में explicit चाहिए
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
# Per-symbol cumulative-volume tracker — day-based rollover detection
# ---------------------------------------------------------------------------
_LAST_CUM_VOL:  dict[str, int]  = {}
_LAST_TICK_DAY: dict[str, date] = {}
_VOL_LOCK = threading.Lock()


def seed_last_cum_vol(pool: ConnectionPool) -> None:
    """
    Restart पर: हर symbol के लिए DB का latest tick का cumulative volume
    और उसका IST date load कर लें ताकि:
      * delta correct बने (न कि 0 first tick पर)
      * day rollover detection सही चले
    """
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
    Robust tick_volume calculation:
      1. day rollover (tick का IST-date != prev IST-date)  → return cum_vol,
         update prev (नये दिन का first volume = cum_vol)
      2. mid-day decrease (cum < prev, same day)  → broker glitch,
         return 0 और prev preserve करो (fake spike avoid)
      3. normal increase  → return cum - prev, update prev
      4. first tick ever  → return 0 (single tick की volume lose, acceptable)
    """
    if cum_vol is None or cum_vol < 0:
        return None

    tick_day = tick_ts.astimezone(IST).date()

    with _VOL_LOCK:
        prev      = _LAST_CUM_VOL.get(symbol)
        prev_day  = _LAST_TICK_DAY.get(symbol)

        # (1) day rollover — सबसे reliable detection
        if prev_day is not None and tick_day != prev_day:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            log.debug("rollover %s: prev_day=%s, new_day=%s, cum=%d",
                      symbol, prev_day, tick_day, cum_vol)
            return cum_vol           # नये दिन का first volume

        _LAST_TICK_DAY[symbol] = tick_day

        # (2) mid-day glitch — same day में volume गिरा (impossible normally)
        if prev is not None and cum_vol < prev:
            log.debug("volume glitch %s: cum=%d < prev=%d (same day) — ignore",
                      symbol, cum_vol, prev)
            return 0                  # prev MUST NOT be updated

        _LAST_CUM_VOL[symbol] = cum_vol

        # (4) first tick ever (no prev)
        if prev is None:
            return 0

        # (3) normal increase
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
    """
    Multi-source lookup. Usage: _pick(outer, inner, "k1", "k2")
    जो पहले मिले (None नहीं), वह return।
    """
    sources = []
    keys = []
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
    String/int/float timestamps को tz-aware UTC datetime में बदलता है।
    Naive string हो तो IST मानकर UTC में convert। (5:30hr shift bug fix)
    """
    if isinstance(ts_raw, (int, float)):
        ts_val = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
        try:
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return datetime.now(timezone.utc)

    if isinstance(ts_raw, str):
        candidates = (
            None,                              # ISO 8601
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%d-%b-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        )
        for fmt in candidates:
            try:
                if fmt is None:
                    dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(ts_raw, fmt)
            except ValueError:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)            # broker = IST
            return dt.astimezone(timezone.utc)

    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Tick parser (kind = 'quote' | 'depth')
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

        # ---- depth (Level-2) extraction ----
        depth_obj = None
        bid_val   = _to_float(_pick(payload, inner, "bid", "best_bid_price",
                                    "buy_price"))
        ask_val   = _to_float(_pick(payload, inner, "ask", "best_ask_price",
                                    "sell_price"))

        if kind == "depth":
            depth_obj = _pick(payload, inner, "depth", "market_depth")
            # OpenAlgo native shape: bids/asks arrays at inner level
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

            # arrays से top-of-book निकाल लो अगर scalar field नहीं था
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

        return {
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
    except Exception as e:                                      # noqa: BLE001
        log.warning("parse_tick failed: %s | keys=%s",
                    e, list(payload.keys())[:8])
        return None


# ---------------------------------------------------------------------------
# Disk spool
# ---------------------------------------------------------------------------
def _spool_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    fname = SPOOL_DIR / (
        f"spool_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
    )
    with fname.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(_serialize_row(r), default=str) + "\n")
    log.error("spooled %d rows -> %s", len(rows), fname)


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
    return out


def replay_spool(pool: ConnectionPool) -> None:
    """Spool files को chronological order में DB में push करता है।"""
    if not SPOOL_DIR.exists():
        return
    files = sorted(SPOOL_DIR.glob("spool_*.jsonl"))
    if not files:
        return
    log.info("replaying %d spool files", len(files))
    for fp in files:
        try:
            rows = [_deserialize_row(json.loads(line))
                    for line in fp.read_text().splitlines() if line.strip()]
            if rows:
                with pool.connection() as con, con.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
                log.info("replayed %d rows from %s", len(rows), fp.name)
            fp.unlink()
        except Exception as e:                                  # noqa: BLE001
            log.error("replay failed %s: %s — छोड़ रहे हैं", fp.name, e)


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
# Gap recorder
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
        log.error("gap insert failed: %s", e)


# ---------------------------------------------------------------------------
# Main loop with reconnect + watchdog
# ---------------------------------------------------------------------------
class WatchdogStale(Exception):
    pass


def run() -> None:
    symbols = load_symbols(SYMBOLS_FILE)
    instruments = [{"exchange": EXCHANGE, "symbol": s} for s in symbols]

    pool = make_pool()
    # Order matters: pending writes पहले, फिर latest state DB से seed
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
            # drop नहीं — disk पर spool करो
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

            # ---- नया attempt — state reset (stale value carry न हो) ----
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
            else:                                                # depth
                client.subscribe_depth(instruments, on_data_received=on_depth)
            log.info("connected & subscribed (%d symbols, mode=%s)",
                     len(instruments), MODE)

            # पिछले disconnect का gap log
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
                        # connect के बाद पहला tick अभी तक नहीं
                        age = (now_utc - connected_at).total_seconds()
                        if age > WATCHDOG_TIMEOUT and is_market_hours(now_utc):
                            log.warning("watchdog: connect को %.0fs हो गए "
                                        "बिना पहले tick — reconnect", age)
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
                # cleanup — हर error suppress
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
        # Flusher shut नहीं हुआ — pending queue spool करो ताकि data न जाए
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

## File 3 of 8 — `gap_filler.py`  (282 lines)

```python
"""
Gap Filler  (v3 — review fixes round 2)
========================================
collector_gaps से unfilled disconnect periods लेकर 1-min OHLC bars
ohlc_1m_filled में डालता है।

v3 sudhar:
  H. Partial minute fix: filter minute-floor par (पहली partial minute miss नहीं)
  I. No-rows-in-market-hours = failure (अब "data नहीं आया" को skip नहीं करते)
  J. Multi-day gap → day-by-day API chunking (broker 1m limit hit न हो)
  + pool.close() हमेशा finally में
  + unused defaultdict import हटाया
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, time as dtime, timedelta, timezone
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


def gap_overlaps_market_hours(start: datetime, end: datetime) -> bool:
    """
    Gap window में कहीं भी कोई market-hours minute है? (कम-से-कम एक trading day
    और उस day में 9:15-15:30 का कोई हिस्सा।)
    """
    cur = start.astimezone(IST)
    end_ist = end.astimezone(IST)
    while cur <= end_ist:
        # weekend skip
        if cur.weekday() <= 4:
            day_open  = cur.replace(hour=9,  minute=15, second=0, microsecond=0)
            day_close = cur.replace(hour=15, minute=30, second=0, microsecond=0)
            # any overlap
            if start.astimezone(IST) <= day_close and end_ist >= day_open:
                return True
        cur = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
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
    """
    DataFrame से gap window के bars निकालो।
    Window minute-boundary पर floor किया जाता है ताकि partial first/last minute
    drop न हो। Naive timestamps IST मानकर UTC में।
    """
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
    """
    Multi-day gap → day-by-day API call (broker 1m limit avoid).
    हर day का API result merge करके gap-window-filtered tuples लौटाओ।
    """
    start_ist = start.astimezone(IST).date()
    end_ist   = end.astimezone(IST).date()
    all_rows: list[tuple] = []
    for day in date_range(start_ist, end_ist):
        if day.weekday() > 4:                # Sat/Sun skip
            continue
        sd = ed = day.isoformat()
        df = fetch_history(client, symbol, sd, ed)
        if df is None:
            continue
        all_rows.extend(df_rows_in_window(df, start, end, symbol))
    return all_rows


def fill_one_gap(client, pool, gid: int, start: datetime, end: datetime,
                 symbols: list[str]) -> tuple[int, list[str], str | None]:
    """
    Returns: (inserted_rows, failed_symbol_list, last_error_or_None)
    """
    inserted = 0
    failed: list[str] = []
    last_err: str | None = None
    market_gap = gap_overlaps_market_hours(start, end)

    for sym in symbols:
        rows = fetch_history_chunked(client, sym, start, end)

        if not rows:
            # API fail OR window में data नहीं
            if market_gap:
                # market hours में 0 rows = data नहीं मिला → failure
                failed.append(sym)
                last_err = f"no bars in market-hours window for {sym}"
                log.warning("gap %d | %s: 0 bars in market-hours gap", gid, sym)
            else:
                log.debug("gap %d | %s: 0 bars (off-hours gap, OK)", gid, sym)
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
                    help="सिर्फ़ वो gaps जिनमें पहले attempts हुए हैं")
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
# NSE Tick Collector — Hindi गाइड (v3)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v3 में ChatGPT + Gemini के दो rounds के सारे critical fixes लागू हैं।

---

## v1 → v2 → v3 का सफर

### v2 round-1 fixes (12)
Watchdog placement, parse_tick nested data, tick_volume delta, disk spool,
gap_filler retry, IST timezone in gap_filler, schema gaps table,
v_quotes view, MODE=depth optional, .env comments, make_conninfo, index।

### v3 round-2 fixes (15)

| # | Bug                                                              | Fix |
|---|------------------------------------------------------------------|-----|
| A | Watchdog: connect के बाद कोई tick नहीं आया → infinite `pass`     | `connected_at` + grace timeout, no-first-tick पर भी reconnect |
| B | `state["last_tick"]` reconnect पर carry होता था                  | हर attempt के शुरू में explicit `None` reset |
| C | **Timestamp 5:30hr shift bug** (naive ISO → UTC नहीं)            | naive datetime को IST मानकर UTC convert |
| D | **Mid-day volume glitch** (broker से 0 → fake spike अगले tick)    | day-based rollover detection; intra-day decrease = glitch, prev preserve |
| E | Depth payload — `bids`/`asks` arrays में होते हैं                | defensive depth parsing + top-of-book bid/ask arrays से extract |
| F | `MODE=both` में duplicate counting                               | `both` reject; depth alone सब देता है |
| G | Spool replay BEFORE seed                                          | order बदला (latest data से seed) |
| H | Gap filler partial first/last minute drop                        | minute-floor filter |
| I | Gap filler "no rows in market hours" को success मानता था         | market-hours overlap check → failure |
| J | Multi-day gap → broker 1m API limit                               | day-by-day chunking |
| K | `v_quotes_1m` partial live minute = full मान लेता था             | `quality` column (full / partial / history) |
| L | `queue.Full` पर tick drop                                         | spool करो |
| M | Flusher join timeout के बाद pending lost                          | timeout पर remaining queue spool |
| N | Realtime CAGG default disabled (TimescaleDB 2.13+)                | `materialized_only = false` explicit |
| O | `ConnectionPool` बिना `open=True` (psycopg-pool 3.2+)             | `open=True` |

---

## आपके सवालों के जवाब

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → आपकी Python script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?

| बात                    | सीधा broker SDK           | OpenAlgo |
|------------------------|---------------------------|----------|
| Latency (localhost)    | ~0 ms                     | ~1-2 ms  |
| 30+ brokers code reuse | ❌ हर broker अलग         | ✅       |
| Symbol format unified  | ❌                        | ✅       |
| Reliability layers     | broker only               | broker + OpenAlgo |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended।**

### Connection कटा — data कैसे recover होगा? (4 परतें)

1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)।
2. **Watchdog** — silent disconnect detection (30s बिना tick = reconnect)।
3. **Disk spool** — DB down हो तो rows JSONL file में; अगले startup पर auto-replay।
4. **Gap fill** — disconnect window `collector_gaps` में log; `gap_filler.py` 1-min
   bars history API से `ohlc_1m_filled` में डालता है (per-symbol retry, day-by-day
   chunking)।

> **Note:** brokers का "true 1-second history" मुफ़्त नहीं मिलता। Live stream 1-sec है;
> gap recovery 1-min granularity पर।

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
                                          │   ↓ DB FAIL → spool │
                                          │   ↓ Q FULL  → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m     │
                                          └─────────────────────┘
```

**Watchdog flow:**
```
on_quote() → state["last_tick"] = now
main loop  → if (no first tick AND age > 30s AND market hours) → reconnect
             if (last_tick AND age > 30s AND market hours)     → reconnect
```

**Volume delta logic:**
```
parse_tick → compute_tick_volume(symbol, cum_vol, ts):
   • day rollover (tick_day != prev_day)        → return cum_vol, update prev
   • intra-day decrease (cum < prev, same day)  → glitch! return 0, prev unchanged
   • normal increase (cum >= prev)              → return cum-prev, update prev
   • first ever tick                            → return 0
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

# शाम (gap fill, optional)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth`। `depth` column में पूरा order book JSONB।

```sql
-- Order Book Imbalance (OBI) example
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

> ⚠️ depth JSONB का exact shape broker-specific है। पहला live tick देखकर adjust करें।

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

## Sample queries

```sql
-- आज RELIANCE का 1-second OHLCV
SELECT bucket, open, high, low, close, volume, tick_count
FROM   ohlc_1s
WHERE  symbol = 'RELIANCE'
  AND  bucket > now() - interval '5 minutes'
ORDER  BY bucket DESC LIMIT 100;

-- ML training feed (clean — full live + history-filled, no partial)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY'
  AND  ts >= '2026-06-07'
  AND  quality IN ('full', 'history')
ORDER  BY ts;

-- Pending gaps
SELECT id, started_at, ended_at, attempts,
       array_length(failed_symbols, 1) AS n_fail
FROM   collector_gaps WHERE filled = FALSE;

-- आज सबसे active symbols
SELECT symbol, count(*), max(ltp), min(ltp)
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  stream_type = 'quote'
GROUP  BY symbol ORDER BY 2 DESC LIMIT 10;

-- Volume glitch detection (sanity check)
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0
  AND  ltp > 0
GROUP  BY symbol HAVING count(*) > 100;
```

---

## Tuning

| problem                   | solution |
|---------------------------|----------|
| Insert lag                | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा          | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए| `schema.sql` में retention policy uncomment |
| 50+ symbols               | OpenAlgo हज़ारों handle करता है — `symbols.txt` बढ़ाएँ |
| Watchdog बहुत agressive   | `WATCHDOG_TIMEOUT_SEC=60` |
| Spool भरा हुआ है          | `ls spool/` — startup पर खुद drain होगा |

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB schema (v3)
├── collector.py        Live collector — watchdog, day-rollover vol, spool
├── gap_filler.py       Day-chunked retry + market-hours validation
├── symbols.txt         Nifty 50 default
├── requirements.txt    Python deps
├── .env.example        Config template
├── .gitignore
├── README.md           यह file
└── REVIEW_BUNDLE.md    सब code single file में (ChatGPT/Gemini review के लिए)
```

बस — market hours में `collector.py` चलाते रहें; production-grade 1-sec tick data
रोज़ का साफ़-सुथरा इकट्ठा होता रहेगा।
````

---

## End of bundle

Total: 8 files, 1490 lines.
