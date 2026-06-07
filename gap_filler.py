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
