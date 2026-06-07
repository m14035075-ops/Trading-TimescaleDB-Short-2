# NSE Tick Collector — Hindi गाइड (v3)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही
> server के **TimescaleDB** में store करने वाला production-grade project।
> v3 में ChatGPT + Gemini के दो rounds के सारे critical fixes लागू हैं।

---

## v1 → v2 → v3 का सफर

### v2 round-1 fixes (12)
Watchdog placement, parse_tick nested data, tick_volume delta, disk spool,
gap_filler retry, IST timezone in gap_filler, schema gaps table,
v_quotes view, MODE=depth optional, .env comments, make_conninfo, index।

### v3 round-2 fixes (15)

| # | Bug                                                              | Fix |
|---|------------------------------------------------------------------|-----|
| A | Watchdog: connect के बाद कोई tick नहीं आया → infinite `pass`     | `connected_at` + grace timeout, no-first-tick पर भी reconnect |
| B | `state["last_tick"]` reconnect पर carry होता था                  | हर attempt के शुरू में explicit `None` reset |
| C | **Timestamp 5:30hr shift bug** (naive ISO → UTC नहीं)            | naive datetime को IST मानकर UTC convert |
| D | **Mid-day volume glitch** (broker से 0 → fake spike अगले tick)    | day-based rollover detection; intra-day decrease = glitch, prev preserve |
| E | Depth payload — `bids`/`asks` arrays में होते हैं                | defensive depth parsing + top-of-book bid/ask arrays से extract |
| F | `MODE=both` में duplicate counting                               | `both` reject; depth alone सब देता है |
| G | Spool replay BEFORE seed                                          | order बदला (latest data से seed) |
| H | Gap filler partial first/last minute drop                        | minute-floor filter |
| I | Gap filler "no rows in market hours" को success मानता था         | market-hours overlap check → failure |
| J | Multi-day gap → broker 1m API limit                               | day-by-day chunking |
| K | `v_quotes_1m` partial live minute = full मान लेता था             | `quality` column (full / partial / history) |
| L | `queue.Full` पर tick drop                                         | spool करो |
| M | Flusher join timeout के बाद pending lost                          | timeout पर remaining queue spool |
| N | Realtime CAGG default disabled (TimescaleDB 2.13+)                | `materialized_only = false` explicit |
| O | `ConnectionPool` बिना `open=True` (psycopg-pool 3.2+)             | `open=True` |

---

## आपके सवालों के जवाब

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → आपकी Python script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?

| बात                    | सीधा broker SDK           | OpenAlgo |
|------------------------|---------------------------|----------|
| Latency (localhost)    | ~0 ms                     | ~1-2 ms  |
| 30+ brokers code reuse | ❌ हर broker अलग         | ✅       |
| Symbol format unified  | ❌                        | ✅       |
| Reliability layers     | broker only               | broker + OpenAlgo |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended।**

### Connection कटा — data कैसे recover होगा? (4 परतें)

1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)।
2. **Watchdog** — silent disconnect detection (30s बिना tick = reconnect)।
3. **Disk spool** — DB down हो तो rows JSONL file में; अगले startup पर auto-replay।
4. **Gap fill** — disconnect window `collector_gaps` में log; `gap_filler.py` 1-min
   bars history API से `ohlc_1m_filled` में डालता है (per-symbol retry, day-by-day
   chunking)।

> **Note:** brokers का "true 1-second history" मुफ़्त नहीं मिलता। Live stream 1-sec है;
> gap recovery 1-min granularity पर।

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
                                          │   ↓ DB FAIL → spool │
                                          │   ↓ Q FULL  → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m     │
                                          └─────────────────────┘
```

**Watchdog flow:**
```
on_quote() → state["last_tick"] = now
main loop  → if (no first tick AND age > 30s AND market hours) → reconnect
             if (last_tick AND age > 30s AND market hours)     → reconnect
```

**Volume delta logic:**
```
parse_tick → compute_tick_volume(symbol, cum_vol, ts):
   • day rollover (tick_day != prev_day)        → return cum_vol, update prev
   • intra-day decrease (cum < prev, same day)  → glitch! return 0, prev unchanged
   • normal increase (cum >= prev)              → return cum-prev, update prev
   • first ever tick                            → return 0
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

# शाम (gap fill, optional)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth`। `depth` column में पूरा order book JSONB।

```sql
-- Order Book Imbalance (OBI) example
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

> ⚠️ depth JSONB का exact shape broker-specific है। पहला live tick देखकर adjust करें।

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

## Sample queries

```sql
-- आज RELIANCE का 1-second OHLCV
SELECT bucket, open, high, low, close, volume, tick_count
FROM   ohlc_1s
WHERE  symbol = 'RELIANCE'
  AND  bucket > now() - interval '5 minutes'
ORDER  BY bucket DESC LIMIT 100;

-- ML training feed (clean — full live + history-filled, no partial)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY'
  AND  ts >= '2026-06-07'
  AND  quality IN ('full', 'history')
ORDER  BY ts;

-- Pending gaps
SELECT id, started_at, ended_at, attempts,
       array_length(failed_symbols, 1) AS n_fail
FROM   collector_gaps WHERE filled = FALSE;

-- आज सबसे active symbols
SELECT symbol, count(*), max(ltp), min(ltp)
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  stream_type = 'quote'
GROUP  BY symbol ORDER BY 2 DESC LIMIT 10;

-- Volume glitch detection (sanity check)
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0
  AND  ltp > 0
GROUP  BY symbol HAVING count(*) > 100;
```

---

## Tuning

| problem                   | solution |
|---------------------------|----------|
| Insert lag                | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा          | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए| `schema.sql` में retention policy uncomment |
| 50+ symbols               | OpenAlgo हज़ारों handle करता है — `symbols.txt` बढ़ाएँ |
| Watchdog बहुत agressive   | `WATCHDOG_TIMEOUT_SEC=60` |
| Spool भरा हुआ है          | `ls spool/` — startup पर खुद drain होगा |

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB schema (v3)
├── collector.py        Live collector — watchdog, day-rollover vol, spool
├── gap_filler.py       Day-chunked retry + market-hours validation
├── symbols.txt         Nifty 50 default
├── requirements.txt    Python deps
├── .env.example        Config template
├── .gitignore
├── README.md           यह file
└── REVIEW_BUNDLE.md    सब code single file में (ChatGPT/Gemini review के लिए)
```

बस — market hours में `collector.py` चलाते रहें; production-grade 1-sec tick data
रोज़ का साफ़-सुथरा इकट्ठा होता रहेगा।
