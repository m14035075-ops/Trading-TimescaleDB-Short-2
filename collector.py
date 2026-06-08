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
_LAST_CUM_VOL:  dict[str, int]      = {}
_LAST_TICK_DAY: dict[str, date]     = {}
_LAST_TICK_TS:  dict[str, datetime] = {}    # v7: per-symbol last-tick time
_VOL_LOCK = threading.Lock()

# v7 FIX (ChatGPT #2): same-day reconnect detection — इतने seconds बिना tick के
# बाद आया tick = reconnect; cumulative delta को एक tick में credit नहीं करते
# (gap_filler उसे history bars से भरेगा)।
RECONNECT_GAP_SEC = int(os.getenv("RECONNECT_GAP_THRESHOLD_SEC", "60"))


def seed_last_cum_vol(pool: ConnectionPool) -> None:
    """
    v7 FIX (Gemini): exchange filter + DISTINCT ON (exchange, symbol)
                     ताकि idx_ticks_ex_sym_ts का proper use हो (no seq-scan)।
    v8 SELF-AUDIT FIX (CRITICAL): _LAST_TICK_TS भी seed करो — पहले reconnect
    detection collector-restart पर काम ही नहीं कर रहा था (prev_ts=None →
    branch skip → cumulative spike on first post-restart tick)।
    """
    sql = """
        SELECT DISTINCT ON (exchange, symbol) symbol, volume, ts
        FROM   ticks
        WHERE  exchange = %s
          AND  volume IS NOT NULL
          AND  ts >= now() - INTERVAL '7 days'
        ORDER  BY exchange, symbol, ts DESC
    """
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(sql, (EXCHANGE,))
            for sym, vol, ts in cur.fetchall():
                _LAST_CUM_VOL[sym]  = int(vol)
                _LAST_TICK_DAY[sym] = ts.astimezone(IST).date()
                _LAST_TICK_TS[sym]  = ts        # v8 FIX: seed prev_ts
        log.info("seeded last_cum_vol+day+ts for %d symbols", len(_LAST_CUM_VOL))
    except Exception as e:                                      # noqa: BLE001
        log.warning("seed_last_cum_vol failed: %s", e)


def get_last_db_tick_ts(pool: ConnectionPool) -> datetime | None:
    """
    v7 FIXES:
      (Gemini): exchange filter + GROUP BY (index use)
      (Qwen #1): WHERE max_ts IS NOT NULL — कोई नया symbol हो तो min()=NULL
                 ट्रैप से बचो; अन्यथा startup gap recording skip हो जाता था।
    """
    try:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute("""
                WITH per_symbol AS (
                    SELECT exchange, symbol, max(ts) AS max_ts
                    FROM   ticks
                    WHERE  exchange = %s
                      AND  ts >= now() - INTERVAL '7 days'
                    GROUP  BY exchange, symbol
                )
                SELECT min(max_ts)
                FROM   per_symbol
                WHERE  max_ts IS NOT NULL
            """, (EXCHANGE,))
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

    v9 SELF-AUDIT FIX (CRITICAL): cum_vol=None ticks (volumeless updates)
    पर भी _LAST_TICK_TS update करते हैं, ताकि बाद के ticks पर reconnect
    detection गलत trigger न हो (stream अभी भी live था)।
    """
    if cum_vol is None or cum_vol < 0:
        # v9: liveness track करो — broker कभी-कभी volume-less LTP updates भेजता है
        with _VOL_LOCK:
            _LAST_TICK_TS[symbol] = tick_ts
        return None

    tick_ist = tick_ts.astimezone(IST)
    tick_day = tick_ist.date()
    in_open_window = tick_ist.time() <= MARKET_OPEN_GRACE

    with _VOL_LOCK:
        prev      = _LAST_CUM_VOL.get(symbol)
        prev_day  = _LAST_TICK_DAY.get(symbol)
        prev_ts   = _LAST_TICK_TS.get(symbol)

        # (1) day rollover
        if prev_day is not None and tick_day != prev_day:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            _LAST_TICK_TS[symbol]  = tick_ts
            if in_open_window:
                return cum_vol
            return 0

        # (2) mid-day glitch
        if prev is not None and cum_vol < prev:
            log.debug("volume glitch %s: cum=%d < prev=%d", symbol, cum_vol, prev)
            # v9: glitch पर भी _LAST_TICK_TS refresh (stream live है)
            _LAST_TICK_TS[symbol] = tick_ts
            return 0

        # (2b) v7 FIX (ChatGPT #2): same-day reconnect detection
        if prev_ts is not None:
            gap_sec = (tick_ts - prev_ts).total_seconds()
            if gap_sec > RECONNECT_GAP_SEC:
                log.debug("reconnect gap %s: %.0fs since prev tick — return 0",
                          symbol, gap_sec)
                _LAST_CUM_VOL[symbol]  = cum_vol
                _LAST_TICK_DAY[symbol] = tick_day
                _LAST_TICK_TS[symbol]  = tick_ts
                return 0

        # (3) first-ever tick
        if prev is None:
            _LAST_CUM_VOL[symbol]  = cum_vol
            _LAST_TICK_DAY[symbol] = tick_day
            _LAST_TICK_TS[symbol]  = tick_ts
            if in_open_window:
                return cum_vol
            return 0

        # (4) normal increase
        _LAST_CUM_VOL[symbol]  = cum_vol
        _LAST_TICK_DAY[symbol] = tick_day
        _LAST_TICK_TS[symbol]  = tick_ts
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


import math


def _clean_json(x: Any) -> Any:
    """
    v7 FIX (ChatGPT #8): NaN/Inf floats को None replace — JSONB insert safe।
    PostgreSQL JSONB raw NaN/Infinity reject करता है।
    """
    if isinstance(x, float):
        return None if not math.isfinite(x) else x
    if isinstance(x, dict):
        return {k: _clean_json(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_clean_json(v) for v in x]
    return x


def _hash_jsonb(j: Any) -> str:
    """v6 FIX (Qwen): allow_nan=True — hash safe। JSONB insert के लिए
       _clean_json अलग से इस्तेमाल होता है।"""
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

        # v9 SELF-AUDIT: NaN/Inf LTP को CAGG-pollution से रोको
        try:
            ltp_f = float(ltp)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(ltp_f):
            log.debug("non-finite LTP for %s: %r — drop tick", symbol, ltp)
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
            "ltp":         ltp_f,
            "volume":      cum_vol,
            "tick_volume": tick_volume,
            "bid":         bid_val,
            "ask":         ask_val,
            "open":        _to_float(_pick(payload, inner, "open")),
            "high":        _to_float(_pick(payload, inner, "high")),
            "low":         _to_float(_pick(payload, inner, "low")),
            "close":       _to_float(_pick(payload, inner, "prev_close",
                                           "previous_close")),
            # v7 FIX (ChatGPT #8): NaN/Inf sanitize before JSONB insert
            "depth":       Jsonb(_clean_json(depth_obj)) if depth_obj is not None else None,
            "raw":         Jsonb(_clean_json(payload)) if STORE_RAW_PAYLOAD else None,
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
