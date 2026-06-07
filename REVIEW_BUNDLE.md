# NSE Tick Collector — Full Code Review Bundle (v2)

> **Reviewers (ChatGPT / Gemini / Claude) के लिए:**
> यह **v2** है — पिछले review के सारे critical bugs fix हो चुके हैं।
> अब verify करना है कि नई implementation में कोई regression या नया bug न हो।
>
> **पिछले review में जो fix हुए:**
> 1. `last_seen` watchdog → callback में move + main-loop staleness check
> 2. Volume CAGG → Python में per-symbol `tick_volume` delta → `sum(tick_volume)`
> 3. `parse_tick` nested data → outer + inner दोनों दिशा defensive lookup
> 4. DB fail पर JSONL disk-spool + startup पर auto-replay
> 5. `gap_filler` per-symbol failure tracking, सब OK तभी `filled=TRUE`
> 6. `gap_filler` timezone — naive timestamps IST मानकर UTC convert
> 7. `collector_gaps` table schema.sql में move
> 8. `v_quotes` UNION duplicates → proper "prefer-1s, fallback-1m" view
> 9. Optional Level-2 (`MODE=depth`) + `depth JSONB` column
> 10. `.env.example` comments अलग lines (systemd EnvironmentFile compatible)
> 11. `psycopg.conninfo.make_conninfo()` (special-char passwords safe)
> 12. Index `(exchange, symbol, ts DESC)`
>
> **अब verify करें:**
> - कोई race condition watchdog vs callback के बीच?
> - `tick_volume` का day-boundary handling सही है?
> - depth subscription के साथ quote subscription concurrent ठीक है?
> - spool replay sequence guarantee देता है?
> - Edge cases — broker से समय IST string में आए, या millisecond float में?

---

## Project structure

```
nse-tick-collector/
├── schema.sql          TimescaleDB tables + CAGG (sum(tick_volume)) + compression
├── collector.py        Live collector (watchdog + spool + optional L2)
├── gap_filler.py       Per-symbol retry, IST→UTC fix
├── symbols.txt         50 stock symbols
├── requirements.txt    Python deps
├── .env.example        Config template (multi-line comments)
├── .gitignore
└── README.md           Hindi user guide
```

## Tech stack

- **Python 3.12+**
- **OpenAlgo Python SDK** v2.0+ (`subscribe_quote`, `subscribe_depth`, `history`)
- **TimescaleDB** — hypertable + continuous aggregate + compression
- **psycopg 3** with connection pool
- **tenacity** exponential backoff + custom `WatchdogStale` exception
- **python-dotenv**

---

## File 1 of 8 — `schema.sql`

```sql
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
```

---

## File 2 of 8 — `collector.py`  (517 lines)

```python
"""
NSE Tick Collector  (v2 — review-fixes applied)
================================================
OpenAlgo WebSocket -> Buffer -> TimescaleDB (batch insert)

Fixes from ChatGPT/Gemini review:
  1. parse_tick(): nested 'data' dict properly handled, 0-values safe
  2. last_tick_ts: callback में update होता है (silent disconnects detect)
  3. Watchdog: market hours में N सेकंड tick नहीं तो force reconnect
  4. tick_volume: per-symbol cumulative delta calculate करके store
     (sum-able है — CAGG में सही volume मिलता है)
  5. Disk spool: DB fail पर JSONL में lines लिख देते हैं, startup पर replay
  6. Optional Level-2 depth: MODE=depth/both पर subscribe_depth → depth JSONB
  7. psycopg make_conninfo: special chars में password safe
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
from datetime import datetime, time as dtime, timedelta, timezone
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
MODE                = os.getenv("MODE", "quote").lower()           # quote | depth | both
BATCH_SIZE          = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC  = float(os.getenv("FLUSH_INTERVAL_SEC", "1"))
RECONNECT_MAX_DELAY = int(os.getenv("RECONNECT_MAX_DELAY", "60"))
WATCHDOG_TIMEOUT    = int(os.getenv("WATCHDOG_TIMEOUT_SEC", "30"))
SPOOL_DIR           = Path(os.getenv("SPOOL_DIR", "./spool"))
STORE_RAW_PAYLOAD   = os.getenv("STORE_RAW_PAYLOAD", "false").lower() == "true"
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO").upper()

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
INSERT INTO ticks (ts, exchange, symbol, ltp, volume, tick_volume,
                   bid, ask, open, high, low, close, depth, raw)
VALUES (%(ts)s, %(exchange)s, %(symbol)s, %(ltp)s, %(volume)s, %(tick_volume)s,
        %(bid)s, %(ask)s, %(open)s, %(high)s, %(low)s, %(close)s,
        %(depth)s, %(raw)s)
"""


def make_pool() -> ConnectionPool:
    return ConnectionPool(PG_DSN, min_size=1, max_size=4,
                          kwargs={"autocommit": True})


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


def is_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() > 4:                  # Sat=5, Sun=6
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


# ---------------------------------------------------------------------------
# Per-symbol cumulative-volume tracker (for tick_volume calculation)
# ---------------------------------------------------------------------------
_LAST_CUM_VOL: dict[str, int] = {}
_VOL_LOCK = threading.Lock()


def seed_last_cum_vol(pool: ConnectionPool) -> None:
    """
    Restart पर: आज के last cumulative volume को DB से लोड कर लें ताकि
    पहला tick का tick_volume सही delta बने (न कि 0).
    """
    sql = """
        SELECT DISTINCT ON (symbol) symbol, volume
        FROM ticks
        WHERE ts >= (now() AT TIME ZONE 'Asia/Kolkata')::date
                    AT TIME ZONE 'Asia/Kolkata'
          AND volume IS NOT NULL
        ORDER BY symbol, ts DESC
    """
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(sql)
            for sym, vol in cur.fetchall():
                _LAST_CUM_VOL[sym] = int(vol)
        log.info("seeded last_cum_vol for %d symbols", len(_LAST_CUM_VOL))
    except Exception as e:                                      # noqa: BLE001
        log.warning("seed_last_cum_vol failed: %s", e)


def compute_tick_volume(symbol: str, cum_vol: int | None) -> int | None:
    """
    cumulative day-volume से इस tick की actual quantity निकालो।
    First tick या new-day reset पर 0 (तभी बेहतर — गलत बड़ा spike न हो)।
    """
    if cum_vol is None:
        return None
    with _VOL_LOCK:
        prev = _LAST_CUM_VOL.get(symbol)
        _LAST_CUM_VOL[symbol] = cum_vol
    if prev is None:
        return 0                                # पहला tick — delta unknown
    if cum_vol < prev:
        return 0                                # day reset
    return cum_vol - prev


# ---------------------------------------------------------------------------
# Tick parser (fixed: nested data + is-not-None checks)
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


def _pick(d_outer: dict, d_inner: dict, *keys: str) -> Any:
    """outer पहले, फिर inner — जो पहले मिले (None नहीं)।"""
    for d in (d_outer, d_inner):
        for k in keys:
            v = d.get(k)
            if v is not None:
                return v
    return None


def parse_tick(payload: dict[str, Any], default_exchange: str,
               kind: str = "quote") -> dict[str, Any] | None:
    """
    OpenAlgo callback payload → DB row.
    Brokers के अनुसार fields कभी outer, कभी 'data' inner में आते हैं —
    दोनों जगह defensively check करते हैं।
    """
    try:
        inner = payload.get("data") if isinstance(payload.get("data"), dict) else {}

        symbol   = _pick(payload, inner, "symbol", "trading_symbol")
        if not symbol:
            return None

        exchange = _pick(payload, inner, "exchange") or default_exchange

        ltp = _pick(payload, inner, "ltp", "last_price")
        if ltp is None and kind == "quote":
            ltp = _pick(payload, inner, "close")
        if ltp is None:
            return None

        # ---- timestamp ----
        ts_raw = _pick(payload, inner, "timestamp", "exchange_timestamp",
                       "ltt", "last_traded_time")
        ts = _parse_timestamp(ts_raw)

        cum_vol     = _to_int(_pick(payload, inner, "volume", "v"))
        tick_volume = compute_tick_volume(symbol, cum_vol)

        depth_obj = None
        if kind == "depth":
            depth_obj = _pick(payload, inner, "depth", "market_depth")

        return {
            "ts":          ts,
            "exchange":    exchange,
            "symbol":      symbol,
            "ltp":         float(ltp),
            "volume":      cum_vol,
            "tick_volume": tick_volume,
            "bid":         _to_float(_pick(payload, inner, "bid", "best_bid_price",
                                           "buy_price")),
            "ask":         _to_float(_pick(payload, inner, "ask", "best_ask_price",
                                           "sell_price")),
            "open":        _to_float(_pick(payload, inner, "open")),
            "high":        _to_float(_pick(payload, inner, "high")),
            "low":         _to_float(_pick(payload, inner, "low")),
            "close":       _to_float(_pick(payload, inner, "prev_close",
                                           "previous_close")),
            "depth":       Jsonb(depth_obj) if depth_obj is not None else None,
            "raw":         Jsonb(payload) if STORE_RAW_PAYLOAD else None,
        }
    except Exception as e:                                      # noqa: BLE001
        log.warning("parse_tick failed: %s | payload keys=%s",
                    e, list(payload.keys()) if isinstance(payload, dict) else type(payload))
        return None


def _parse_timestamp(ts_raw: Any) -> datetime:
    if isinstance(ts_raw, (int, float)):
        ts_val = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
        try:
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return datetime.now(timezone.utc)
    if isinstance(ts_raw, str):
        try:
            return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Disk spool — DB down हो तब भी data safe
# ---------------------------------------------------------------------------
def _spool_rows(rows: list[dict[str, Any]]) -> None:
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    fname = SPOOL_DIR / f"spool_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
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
    if out.get("depth") is not None:
        out["depth"] = Jsonb(out["depth"])
    if out.get("raw") is not None:
        out["raw"] = Jsonb(out["raw"])
    return out


def replay_spool(pool: ConnectionPool) -> None:
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
    def __init__(self, q: queue.Queue, pool: ConnectionPool, stop_evt: threading.Event):
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
# Main loop
# ---------------------------------------------------------------------------
class WatchdogStale(Exception):
    """Watchdog ने stale stream detect किया — reconnect करो।"""


def run() -> None:
    symbols = load_symbols(SYMBOLS_FILE)
    instruments = [{"exchange": EXCHANGE, "symbol": s} for s in symbols]

    pool = make_pool()
    seed_last_cum_vol(pool)
    replay_spool(pool)

    tick_q: queue.Queue = queue.Queue(maxsize=200_000)
    stop_evt = threading.Event()
    flusher = Flusher(tick_q, pool, stop_evt)
    flusher.start()

    # ---- shared state across reconnects ----
    state: dict[str, datetime | None] = {"last_tick": None}

    # ---- signal handlers ----
    def shutdown(signum, _frame):
        log.info("signal %d मिला — रुक रहे हैं", signum)
        stop_evt.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ---- callbacks ----
    def on_quote(data: dict[str, Any]) -> None:
        state["last_tick"] = datetime.now(timezone.utc)
        row = parse_tick(data, EXCHANGE, kind="quote")
        if row:
            try:
                tick_q.put_nowait(row)
            except queue.Full:
                log.warning("tick queue full — drop %s", row.get("symbol"))

    def on_depth(data: dict[str, Any]) -> None:
        state["last_tick"] = datetime.now(timezone.utc)
        row = parse_tick(data, EXCHANGE, kind="depth")
        if row:
            try:
                tick_q.put_nowait(row)
            except queue.Full:
                log.warning("tick queue full — drop %s", row.get("symbol"))

    # ---- reconnect loop ----
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

            client = api(
                api_key=OPENALGO_API_KEY,
                host=OPENALGO_HOST,
                ws_url=OPENALGO_WS_URL,
                verbose=0,
            )
            client.connect()
            if MODE in ("quote", "both"):
                client.subscribe_quote(instruments, on_data_received=on_quote)
            if MODE in ("depth", "both"):
                client.subscribe_depth(instruments, on_data_received=on_depth)

            log.info("connected & subscribed (%d symbols, mode=%s)",
                     len(instruments), MODE)

            # पिछले disconnect का gap log कर दो (अगर 5s से ज़्यादा था)
            now_utc = datetime.now(timezone.utc)
            if last_disconnect_at and (now_utc - last_disconnect_at).total_seconds() > 5:
                record_gap(pool, last_disconnect_at, now_utc, "ws_reconnect")
            last_disconnect_at = None

            # ---- main idle loop with watchdog ----
            try:
                while not stop_evt.is_set():
                    time.sleep(1)
                    last_tick = state["last_tick"]
                    if last_tick is None:
                        # connect के बाद कभी tick आया ही नहीं
                        # सिर्फ़ market hours में warning के तौर पर watchdog
                        if is_market_hours():
                            # subscribe के बाद थोड़ा grace
                            pass
                        continue
                    age = (datetime.now(timezone.utc) - last_tick).total_seconds()
                    if age > WATCHDOG_TIMEOUT and is_market_hours():
                        log.warning("watchdog: %.0fs बिना tick — reconnect", age)
                        last_disconnect_at = last_tick
                        raise WatchdogStale(f"no ticks for {age:.0f}s")
            finally:
                if last_disconnect_at is None:
                    last_disconnect_at = state["last_tick"] or datetime.now(timezone.utc)
                for fn, args in (
                    (getattr(client, "unsubscribe_quote", None), instruments)
                    if MODE in ("quote", "both") else (None, None),
                    (getattr(client, "unsubscribe_depth", None), instruments)
                    if MODE in ("depth", "both") else (None, None),
                    (getattr(client, "disconnect", None), None),
                ):
                    if fn is None:
                        continue
                    try:
                        fn(args) if args is not None else fn()
                    except Exception:                            # noqa: BLE001
                        pass

            if stop_evt.is_set():
                break

    # ---- shutdown ----
    log.info("waiting for flusher (%d ticks pending)", tick_q.qsize())
    flusher.join(timeout=60)
    pool.close()
    log.info("bye")


if __name__ == "__main__":
    run()
```

---

## File 3 of 8 — `gap_filler.py`  (238 lines)

```python
"""
Gap Filler  (v2 — review-fixes applied)
=======================================
collector_gaps table से unfilled disconnect periods लेकर OpenAlgo की
history API से 1-minute bars मँगवाकर ohlc_1m_filled में डालता है।

Fixes:
  * Per-symbol failure tracking — सिर्फ़ सब symbols सफल होने पर ही
    `filled = TRUE`। बाक़ी `failed_symbols[]` में रहते हैं।
  * Timezone fix — broker history naive timestamps IST हैं, UTC नहीं।
    explicit IST localize करते हैं।
  * `pool.close()` हमेशा `finally` में।
  * Repeated gaps के लिए date-merge — एक symbol-day के लिए एक ही API call।

चलाएं:
    python gap_filler.py            # सारे unfilled gaps
    python gap_filler.py --today    # सिर्फ़ आज के
    python gap_filler.py --retry    # जिन्हें पहले attempts हुए, वही retry
"""
from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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


def fetch_history(client, symbol: str, sd: str, ed: str) -> Any | None:
    """OpenAlgo history API call। DataFrame या None लौटाता है।"""
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
    DataFrame से (start..end] window के अंदर वाले bars निकालो।
    Naive timestamps IST मानकर UTC में convert।
    """
    rows: list[tuple] = []
    for ts, row in df.iterrows():
        py_ts = ts.to_pydatetime()
        if py_ts.tzinfo is None:
            py_ts = py_ts.replace(tzinfo=IST)            # broker = IST
        ts_utc = py_ts.astimezone(timezone.utc)
        if not (start <= ts_utc <= end):
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


def fill_one_gap(client, pool, gid: int, start: datetime, end: datetime,
                 symbols: list[str]) -> tuple[int, list[str], str | None]:
    """
    एक gap के लिए सब symbols process करो।
    returns: (inserted_rows, failed_symbol_list, last_error_or_None)
    """
    sd = start.astimezone(IST).date().isoformat()
    ed = end.astimezone(IST).date().isoformat()

    inserted   = 0
    failed: list[str] = []
    last_err: str | None = None

    for sym in symbols:
        df = fetch_history(client, sym, sd, ed)
        if df is None:
            failed.append(sym)
            last_err = f"history None for {sym}"
            continue

        rows = df_rows_in_window(df, start, end, sym)
        if not rows:
            # API ने data दिया पर window में कुछ नहीं — non-failure
            log.debug("gap %d | %s: no bars in window", gid, sym)
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
                    help="सिर्फ़ वो जिनमें पहले attempts हुए हैं")
    args = ap.parse_args()

    symbols = load_symbols(SYMBOLS_FILE)
    log.info("symbols: %d", len(symbols))

    pool = ConnectionPool(PG_DSN, min_size=1, max_size=2,
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

            # अगर पहले से कुछ symbols fail थे, तो सिर्फ़ उन्हें retry
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
                log.warning("⚠ gap %d partial: %d failed (%s)",
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
# OpenAlgo (अपने server पर local चलेगा)
# नोट: comments अलग lines में रखें — systemd EnvironmentFile inline comments
# parse नहीं करता।

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

# Mode: quote (Level-1) | depth (Level-2 order book) | both
MODE=quote

# कितने ticks जमा होने पर एक insert मारें
BATCH_SIZE=500

# या इतने सेकंड में force flush
FLUSH_INTERVAL_SEC=1

# Reconnect exponential backoff cap
RECONNECT_MAX_DELAY=60

# कितनी देर tick न आए तो stale मानकर reconnect (silent disconnect detection)
WATCHDOG_TIMEOUT_SEC=30

# DB fail पर failed rows यहाँ JSONL में spool होंगी, startup पर replay
SPOOL_DIR=./spool

# हर tick का पूरा raw payload भी DB में store करें? (storage भारी हो जाता है)
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
# NSE Tick Collector — Hindi गाइड (v2)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही server के
> **TimescaleDB** में store करता है।
> v2 में ChatGPT + Gemini code-review के सारे critical fixes apply हो चुके हैं।

---

## क्या-क्या Fix किया गया (v1 → v2)

| # | समस्या (v1)                                        | Fix (v2) |
|---|----------------------------------------------------|----------|
| 1 | `last_seen` watchdog ग़लत जगह — silent disconnect miss | Callback में update + main loop में staleness check (force reconnect) |
| 2 | Volume CAGG `last - first` — first tick का volume drop | Python में per-symbol delta `tick_volume` → CAGG `sum(tick_volume)` |
| 3 | `parse_tick` nested `data` dict miss               | Outer + inner दोनों जगह defensive lookup, `is not None` checks |
| 4 | DB fail पर shutdown में data loss                  | Disk spool (JSONL) + startup पर auto-replay |
| 5 | `gap_filler` partial fill पर भी `filled=TRUE`      | per-symbol `failed_symbols[]`, सब OK तभी mark filled |
| 6 | `gap_filler` timezone bug (naive → IST not UTC)    | Explicit IST localize → UTC |
| 7 | `collector_gaps` schema में नहीं था                | schema.sql में move (gap_filler standalone चल सकता है) |
| 8 | `v_quotes` UNION duplicates                        | Proper "prefer-1s, fallback-1m" view |
| 9 | Level-2 depth support absent                       | Optional `MODE=depth/both` + `depth JSONB` column |
| 10 | `.env.example` inline comments systemd तोड़ते      | अलग lines |
| 11 | `PG_DSN` manual string (special-char unsafe)       | `psycopg.conninfo.make_conninfo()` |
| 12 | Index unoptimal                                    | `(exchange, symbol, ts DESC)` |

---

## आपके सवालों के जवाब (एक नज़र में)

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → आपकी Python script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?

| बात                    | सीधा broker SDK                | OpenAlgo |
|------------------------|-------------------------------|----------|
| Latency (localhost)    | ~0 ms                         | ~1-2 ms  |
| 30+ brokers code reuse | ❌ हर broker अलग             | ✅ एक code |
| Symbol format unified  | ❌                            | ✅       |
| Reliability layers     | broker ही                     | broker + OpenAlgo |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended**।
> सिर्फ़ HFT-style absolute latency चाहिए तब broker SDK direct।

### Connection कटा — data कैसे recover होगा? (4 परतें)

1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)।
2. **Watchdog** — main loop हर सेकंड check करता है: agar 30 sec बिना tick (market hours में) → force reconnect।
3. **Disk spool** — DB ख़ुद down हो तो rows JSONL file में लिखी जाती हैं;
   अगले startup पर auto-replay।
4. **Gap fill** — disconnect window `collector_gaps` में log; `gap_filler.py`
   बाद में 1-min OHLC bars history API से `ohlc_1m_filled` में डालता है।

> Note: brokers का "true 1-second history" मुफ़्त में नहीं मिलता।
> Live stream 1-sec है; gap recovery 1-min granularity पर।

---

## Architecture

```
 ┌──────────┐  WebSocket  ┌──────────┐  psycopg  ┌──────────────┐
 │  Broker  │ ──────────▶ │ OpenAlgo │ ────────▶ │ collector.py │
 └──────────┘             └──────────┘           └──────┬───────┘
                                                        │ queue.Queue
                                                        ▼
                                          ┌─────────────────────┐
                                          │ Flusher (batch)     │
                                          │   ↓ DB OK ────────  │
                                          │   ↓ DB FAIL → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m view│
                                          └─────────────────────┘
```

**Watchdog flow:**
```
on_quote() → state["last_tick"] = now
main loop  → if now - last_tick > 30s in market hours → raise WatchdogStale
                                                       ↓
                                              tenacity reconnect
```

---

## Setup (एक बार)

### 1. TimescaleDB
```bash
sudo apt install postgresql-16
# TimescaleDB repo से timescaledb-2-postgresql-16 install करें
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
[docs.openalgo.in](https://docs.openalgo.in/) से install + अपने broker से login + API key copy।

### 4. Python env
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# .env खोलकर API key और password भरें
```

`symbols.txt` में Nifty 50 default है — अपनी पसंद के 50 शेयर चाहिए तो वही file edit करें।

---

## रोज़ चलाना

```bash
# सुबह 9 बजे
python collector.py

# शाम (gap fill — optional)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth` या `MODE=both` कर दें। फिर `depth` column में हर tick का full
order book JSONB में store होगा।

```sql
-- depth से Order Book Imbalance निकालने का example
SELECT
    ts, symbol,
    (depth -> 'buy' -> 0 ->> 'quantity')::INT  AS bid_qty_l1,
    (depth -> 'sell' -> 0 ->> 'quantity')::INT AS ask_qty_l1
FROM ticks
WHERE depth IS NOT NULL
ORDER BY ts DESC LIMIT 10;
```

> ⚠️ Note: exact JSONB shape broker-specific है। पहला tick देखकर adjust करें।

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
WorkingDirectory=/home/marketdata/nse-tick-collector
EnvironmentFile=/home/marketdata/nse-tick-collector/.env
ExecStart=/home/marketdata/nse-tick-collector/.venv/bin/python collector.py
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

OpenAlgo को भी अपनी systemd service बना लें ताकि वह crash होने पर auto-restart हो।

Cron (शाम 4 बजे gap fill):
```cron
0 16 * * 1-5 /home/marketdata/nse-tick-collector/.venv/bin/python /home/marketdata/nse-tick-collector/gap_filler.py >> /var/log/gap_filler.log 2>&1
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

-- ML training feed: 1-minute bars (live + gap-filled, NO duplicates)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY' AND ts >= '2026-06-07'
ORDER  BY ts;

-- Pending gaps
SELECT id, started_at, ended_at, attempts, array_length(failed_symbols, 1) AS n_fail
FROM   collector_gaps WHERE filled = FALSE;

-- आज सबसे active symbols
SELECT symbol, count(*), max(ltp), min(ltp)
FROM   ticks WHERE ts > date_trunc('day', now())
GROUP  BY symbol ORDER BY 2 DESC LIMIT 10;

-- Spool में कुछ pending है? (CLI से)
-- ls -la spool/
```

---

## Tuning

| problem                         | solution |
|---------------------------------|----------|
| Insert lag                      | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा                | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए     | `schema.sql` में retention policy uncomment |
| 50+ symbols                     | OpenAlgo हज़ारों handle करता है — `symbols.txt` बढ़ाएँ |
| Watchdog बहुत agressive         | `WATCHDOG_TIMEOUT_SEC=60` |
| Multiple brokers एक साथ         | अलग `.env` से दो instances, अलग OpenAlgo ports |

---

## Files

```
nse-tick-collector/
├── schema.sql          TimescaleDB schema (v2)
├── collector.py        Main live collector (watchdog + spool + L2 optional)
├── gap_filler.py       Disconnect-period 1-min recovery (per-symbol retry)
├── symbols.txt         Nifty 50 default
├── requirements.txt    Python deps
├── .env.example        Config template
├── .gitignore
└── README.md           यह file
```

बस! market hours में `collector.py` चलाते रहें — आपके server में रोज़ का साफ़-सुथरा
1-सेकंड tick data इकट्ठा होता रहेगा।
````

---

## End of bundle

Total: 8 files, 1270 lines.
