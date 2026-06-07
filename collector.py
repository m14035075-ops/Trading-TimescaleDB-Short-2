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
