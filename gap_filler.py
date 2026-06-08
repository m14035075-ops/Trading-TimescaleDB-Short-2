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
        else:
            # v7 FIX (Qwen): unexpected type — debugging के लिए warning
            log.warning("unexpected holidays(%d) response type: %s",
                        y, type(resp).__name__)

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
                # v7 FIX (Gemini #2): explicit API JSON error detection
                # broker कभी-कभी 200 OK में भी {"status":"error", ...} भेजता है
                status_str = str(df.get("status", "")).lower()
                if (status_str in ("error", "failure", "fail")
                        or df.get("error")
                        or df.get("errorMessage")):
                    log.warning(
                        "history JSON error %s: %s",
                        symbol,
                        df.get("message") or df.get("error") or df,
                    )
                    return None, True                            # treat as API error

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
        # v5/v7 FIX: defensive ts conversion (numeric epoch भी handle)
        if hasattr(ts, "to_pydatetime"):
            py_ts = ts.to_pydatetime()
        elif isinstance(ts, (int, float)):
            # v7 FIX (ChatGPT #3): broker कभी-कभी unix epoch भेजता है
            ts_val = ts / 1000.0 if abs(ts) > 1e12 else float(ts)
            try:
                py_ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                continue
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

        # v7 FIX (Gemini #4 / Qwen #2): NaN/string-volume row drop न करे —
        # सिर्फ़ volume = None, OHLC अब भी valid → row preserve
        try:
            ohlc = (
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            log.warning("OHLC parse fail %s @ %s: %s", symbol, ts_utc, e)
            continue

        vol_raw = row.get("volume") if hasattr(row, "get") else (
            row["volume"] if "volume" in row else None
        )
        vol_clean: int | None = None
        if vol_raw is not None and not pd.isna(vol_raw):
            try:
                vol_clean = int(float(vol_raw))
            except (TypeError, ValueError):
                log.debug("invalid volume %s @ %s: %r", symbol, ts_utc, vol_raw)

        rows.append((
            ts_utc, EXCHANGE, symbol,
            ohlc[0], ohlc[1], ohlc[2], ohlc[3],
            vol_clean,
        ))
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
