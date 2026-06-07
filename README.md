# NSE Tick Collector — Hindi गाइड (v4)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v4 में ChatGPT + Gemini के **3 rounds का review** लागू है — कुल **37+ bugs fix**।

---

## v1 → v2 → v3 → v4 का सफर

- **v2 round-1 (12 fixes):** watchdog placement, parse_tick nested data,
  tick_volume delta, disk spool, gap_filler retry, IST timezone, schema gaps,
  v_quotes view, MODE=depth, .env comments, make_conninfo, index।
- **v3 round-2 (15 fixes):** watchdog grace + state reset, naive timestamp →
  IST, day-rollover detection, depth bids/asks arrays, MODE=both reject,
  spool replay order, queue.Full→spool, flusher hang spool, realtime CAGG,
  pool open=True, partial-minute floor, market-hours overlap check,
  day-by-day chunking, quality column, .env multi-line।

### v4 round-3 (10 critical production fixes)

| # | Bug                                                                       | Fix |
|---|---------------------------------------------------------------------------|-----|
| 1 | DB migration पुराने schema पर new columns add नहीं करता                  | Idempotent `ALTER TABLE ADD COLUMN IF NOT EXISTS` + migration notes |
| 2 | **Day-rollover late-start spike** — 10:30 AM start पर पूरा morning vol एक tick में | tick का IST time check; ≤9:30 → genuine, >9:30 → return 0 (gap_filler भरेगा) |
| 3 | **Spool replay duplicate** — crash between insert & unlink → next start पर double | `tick_uid` (SHA1 hash) + UNIQUE index + `ON CONFLICT DO NOTHING` |
| 4 | **Numeric string timestamp** miss — `"1717741500000"` → `datetime.now()` (wrong!) | `s.isdigit()` check before format parsing |
| 5 | `record_gap()` fail पर gap permanently lost (DB down + WS disconnect)    | Gap-spool JSONL + startup replay |
| 6 | Spool storm — हर queue-full tick के लिए नई file                          | Per-hour append-only file (`spool_YYYYMMDD_HH.jsonl`) |
| 7 | `gap_overlaps_market_hours` doesn't know NSE holidays                     | `client.holidays()` API cache; weekday-only fallback |
| 8 | `v_quotes_1m` partial+history same minute → duplicate                     | `v_quotes_1m_raw` (forensic) + `v_quotes_1m` (DISTINCT ON, quality priority) |
| 9 | `sec_count >= 50` low-liquidity stocks में हमेशा partial                  | Gap-overlap-based quality (`full`/`partial`/`sparse`/`history`) |
| 10 | `ohlc_1s` mixed stream_types — forensic clarity                           | Comment + `stream_type` column persists |

---

## आपके सवालों के जवाब

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?
| बात                    | सीधा broker SDK | OpenAlgo |
|------------------------|----------------|----------|
| Latency (localhost)    | ~0 ms          | ~1-2 ms  |
| 30+ brokers code reuse | ❌            | ✅       |
| Symbol format unified  | ❌            | ✅       |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended।**

### Connection कटा — data कैसे recover होगा? (4 परतें)
1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)
2. **Watchdog** — silent disconnect detection (30s tickless = reconnect)
3. **Disk spool** — DB down = JSONL file (per-hour); startup auto-replay; `tick_uid` ON CONFLICT से safe
4. **Gap fill** — `collector_gaps` log; `gap_filler.py` 1-min bars history API से, **NSE-holiday aware**, day-by-day chunking

> **Note:** brokers का "true 1-second history" मुफ़्त नहीं मिलता।

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
                                          │   ↓ DB FAIL → spool │  (per-hour file)
                                          │   ↓ Q FULL  → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │  +tick_uid UNIQUE
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m_raw │  (forensic)
                                          │   ─ v_quotes_1m     │  (clean ML feed)
                                          └─────────────────────┘
```

**Volume delta logic (v4):**
```
parse_tick → compute_tick_volume(symbol, cum_vol, ts):
   • day rollover (tick_day != prev_day):
       - tick_time ≤ 9:30 IST → cum_vol (genuine first-of-day volume)
       - tick_time > 9:30 IST → 0 (late start; gap_filler भरेगा)
   • intra-day decrease (cum < prev) → 0 (broker glitch, prev preserve)
   • normal increase → cum-prev
   • first ever tick:
       - ≤ 9:30 IST → cum_vol
       - > 9:30 IST → 0
```

**Spool flow (v4):**
```
DB fail | Q full | Flusher hang →  spool/spool_YYYYMMDD_HH.jsonl  (append)
record_gap fail                  →  spool/gapspool_YYYYMMDD_HH.jsonl
                                                 ↓
                                  Startup replay (chronological)
                                                 ↓
                                  ON CONFLICT (ts, tick_uid) DO NOTHING
                                  → safe even if crash between insert+unlink
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

> **पुराने (v2/v3) DB से upgrade?** schema.sql चलाने से पहले एक बार:
> ```sql
> DROP MATERIALIZED VIEW IF EXISTS ohlc_1s CASCADE;
> DROP VIEW IF EXISTS v_quotes_1m, v_quotes_1m_raw;
> ```
> फिर `psql -f schema.sql` — ALTER TABLE से नए columns idempotent जुड़ेंगे।

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

# शाम — gap fill (NSE holidays auto-skip करेगा)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth`। `depth` column में पूरा order book JSONB।

```sql
-- Order Book Imbalance example
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

---

## ML Training Quick Reference

```sql
-- Strict ML training feed (recommended)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY'
  AND  ts BETWEEN '2026-01-01' AND '2026-06-07'
  AND  quality IN ('full', 'history')
ORDER  BY ts;

-- Low-liquidity stocks include करना है
SELECT * FROM v_quotes_1m
WHERE  quality != 'partial'   -- sparse भी OK अगर gap नहीं था
ORDER  BY ts;

-- Volume sanity check (mid-day glitch detection)
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0 AND ltp > 0
GROUP  BY symbol HAVING count(*) > 100;

-- Gap forensics
SELECT id, started_at, ended_at, attempts,
       array_length(failed_symbols, 1) AS n_fail, last_error
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

OpenAlgo को भी अपनी systemd service बनाएँ ताकि वह crash होने पर auto-restart हो।

Cron (शाम 4 बजे gap fill):
```cron
0 16 * * 1-5 /home/marketdata/Trading-TimescaleDB-Short-2/.venv/bin/python /home/marketdata/Trading-TimescaleDB-Short-2/gap_filler.py >> /var/log/gap_filler.log 2>&1
```

---

## Tuning

| problem                   | solution |
|---------------------------|----------|
| Insert lag                | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा          | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए| `schema.sql` में retention policy uncomment |
| 50+ symbols               | OpenAlgo हज़ारों handle करता है |
| Watchdog बहुत agressive   | `WATCHDOG_TIMEOUT_SEC=60` |
| Spool भरा हुआ है          | `ls spool/` — startup पर खुद drain होगा |
| Holidays auto-skip नहीं   | OpenAlgo `holidays()` API check; manually `failed_symbols` cleanup |

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB schema (v4) + tick_uid UNIQUE + smart views
├── collector.py        Day-rollover late-start guard, gap-spool, per-hour spool
├── gap_filler.py       NSE holidays + day chunking + market-hours validation
├── symbols.txt         Nifty 50 default
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── REVIEW_BUNDLE.md    Single-file bundle for AI review
```

बस — market hours में `collector.py` चलाते रहें; production-grade 1-sec tick data
रोज़ का साफ़-सुथरा इकट्ठा होता रहेगा।

> **Status:** v4 अब **production-deploy ready**। 27 + 10 = **37 review issues fixed**।
