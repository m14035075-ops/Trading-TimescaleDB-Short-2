# NSE Tick Collector — Hindi गाइड (v5)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v5 में ChatGPT + Gemini + Qwen + Kimi के **4 rounds का review** लागू है —
> कुल **57+ bugs fix**।

---

## v1 → v2 → v3 → v4 → v5 का सफर

- **v2 (round-1: 12 fixes):** watchdog placement, parse_tick nested data,
  tick_volume delta, disk spool, gap_filler retry, IST timezone
- **v3 (round-2: 15 fixes):** watchdog grace, naive timestamp, day-rollover,
  depth arrays, MODE=both reject, partial-minute floor, etc.
- **v4 (round-3: 10 fixes):** ALTER migration, late-start guard, tick_uid,
  numeric string ts, gap_spool, holiday API
- **v5 (round-4: 20 fixes):** **see below**

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

> **Status:** v5 — **57+ review issues fixed across 4 rounds**। Production-deploy ready।

बस — market hours में `collector.py` चलाते रहें; production-grade tick data रोज़ का इकट्ठा होता रहेगा।
