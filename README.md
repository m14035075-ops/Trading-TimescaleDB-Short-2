# NSE Tick Collector — Hindi गाइड (v9)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v9 = 6 review rounds + **2 rounds of self-audit** = total **80 fixes**।

---

## v9 self-audit fixes (round-2 self-recheck — 2 critical bugs)

| # | Bug                                                               | Severity | Fix |
|---|-------------------------------------------------------------------|----------|-----|
| 1 | **`compute_tick_volume` early-return पर `_LAST_TICK_TS` update नहीं** — volumeless ticks (LTP-only updates) के बाद real tick पर false reconnect detection trigger → tick_volume=0, volume permanently lost | 🔴 CRITICAL | Early-return से पहले `_LAST_TICK_TS` update; mid-day glitch branch में भी refresh |
| 2 | **NaN/Inf LTP CAGG pollution** — `float("nan")` `parse_tick` से pass through → SQL `max/min/first/last` NaN propagate | 🟡 MEDIUM | `math.isfinite(ltp_f)` check, return None on non-finite |

---

## v8 self-audit fixes (round-1 self-recheck — 2 bugs)

| # | Bug                                                               | Severity | Fix |
|---|-------------------------------------------------------------------|----------|-----|
| 1 | **`_LAST_TICK_TS` startup पर seed नहीं होता** — v7 reconnect-spike fix collector-restart पर bypass हो जाता था (prev_ts=None → branch skip → cumulative spike) | 🔴 CRITICAL | `seed_last_cum_vol` अब `_LAST_TICK_TS[sym] = ts` भी set करता है |
| 2 | **`gap_overlaps_market_hours` dead code** — defined but unused since v6, ML data quality concern | 🟡 MEDIUM | Revived in `fill_one_gap`: market-hours overlap में 0-rows = failure (Nifty 50 active stocks में broker mismatch detect करता है) |

---

## v1 → v7 का सफर

- **v2 (round-1: 12 fixes):** watchdog, parse_tick, tick_volume, spool, IST
- **v3 (round-2: 15 fixes):** day-rollover, depth arrays, MODE=both reject
- **v4 (round-3: 10 fixes):** ALTER migration, late-start guard, tick_uid
- **v5 (round-4: 20 fixes):** SHA-256, auto-startup-gap, holiday API, quality
- **v6 (round-5: 10 fixes):** --today, fetch_history tuple, pool retry, NaN safe
- **v7 (round-6: 9 fixes):** see below

### v7 round-6 fixes

| # | Bug                                                                    | Fix |
|---|------------------------------------------------------------------------|-----|
| 1 | **`fetch_history` JSON-error swallow** — broker `{"status":"error"}` को empty data treat कर रहा था → gap permanently lost | Explicit error detection: `status`, `error`, `errorMessage` keys check |
| 2 | **`get_last_db_tick_ts` NULL trap** — कोई नया symbol = `min(NULL,...)`=NULL → startup gap skip | `WHERE max_ts IS NOT NULL` filter |
| 3 | **NaN volume drops whole OHLC row** — `int(float(NaN))` ValueError पूरे row को drop करता था | OHLC parse और volume parse अलग — सिर्फ़ volume = None, OHLC valid रहता है |
| 4 | **SQL queries miss `exchange` in GROUP BY** — index `(exchange,symbol,ts DESC)` use नहीं हो रहा था → seq scan | `WHERE exchange = %s` + `DISTINCT ON (exchange, symbol)` |
| 5 | **`df_rows_in_window` numeric ts skip** — broker epoch int/float index → silent data loss | `isinstance(ts, (int, float))` branch |
| 6 | **`compute_tick_volume` reconnect spike** — same-day reconnect: huge cum-prev एक tick में | `_LAST_TICK_TS` track + `RECONNECT_GAP_THRESHOLD_SEC` (60s default) → return 0 |
| 7 | **JSONB NaN/Inf insert fail** — hash safe था, DB insert नहीं | `_clean_json()` recursive sanitizer (NaN→None) before `Jsonb()` |
| 8 | **Schema `UPDATE` table lock on prod** | DO block conditional — सिर्फ़ तभी जब NULL rows मिलें |
| 9 | **Holiday API unexpected type silent skip** | `else: log.warning("unexpected type")` |

### v5 round-4 fixes (20 critical issues, summary)

tick_uid 32-char SHA-256 + depth/raw hash, auto-startup-gap recording,
collector_gaps UNIQUE constraint, gap_minutes time_bucket floor,
v_quotes_1m_raw history filter, quality priority full>sparse>history>partial,
--today IST fix, df_rows_in_window defensive, NSE holidays robust parsing,
multi-day failed_days tracking, psycopg.sql composition, _parse_timestamp
range check, pool startup retry, flusher buf lock, requirements pandas+pool,
.env v5 header, watchdog 60s default, fill_one_gap lenient (no false-fail
on 0-rows), dynamic holiday year cache, tick_uid 32-char (128-bit safe).

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
