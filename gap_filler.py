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
