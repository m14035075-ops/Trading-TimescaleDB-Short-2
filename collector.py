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
