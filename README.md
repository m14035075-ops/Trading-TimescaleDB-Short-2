# NSE Tick Collector — Hindi गाइड (v2)

> 50 भारतीय शेयरों का **live tick data** OpenAlgo WebSocket से उठाकर अपने ही server के
> **TimescaleDB** में store करता है।
> v2 में ChatGPT + Gemini code-review के सारे critical fixes apply हो चुके हैं।

---

## क्या-क्या Fix किया गया (v1 → v2)

| # | समस्या (v1)                                        | Fix (v2) |
|---|----------------------------------------------------|----------|
| 1 | `last_seen` watchdog ग़लत जगह — silent disconnect miss | Callback में update + main loop में staleness check (force reconnect) |
| 2 | Volume CAGG `last - first` — first tick का volume drop | Python में per-symbol delta `tick_volume` → CAGG `sum(tick_volume)` |
| 3 | `parse_tick` nested `data` dict miss               | Outer + inner दोनों जगह defensive lookup, `is not None` checks |
| 4 | DB fail पर shutdown में data loss                  | Disk spool (JSONL) + startup पर auto-replay |
| 5 | `gap_filler` partial fill पर भी `filled=TRUE`      | per-symbol `failed_symbols[]`, सब OK तभी mark filled |
| 6 | `gap_filler` timezone bug (naive → IST not UTC)    | Explicit IST localize → UTC |
| 7 | `collector_gaps` schema में नहीं था                | schema.sql में move (gap_filler standalone चल सकता है) |
| 8 | `v_quotes` UNION duplicates                        | Proper "prefer-1s, fallback-1m" view |
| 9 | Level-2 depth support absent                       | Optional `MODE=depth/both` + `depth JSONB` column |
| 10 | `.env.example` inline comments systemd तोड़ते      | अलग lines |
| 11 | `PG_DSN` manual string (special-char unsafe)       | `psycopg.conninfo.make_conninfo()` |
| 12 | Index unoptimal                                    | `(exchange, symbol, ts DESC)` |

---

## आपके सवालों के जवाब (एक नज़र में)

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।** broker → WS → आपकी Python script → TimescaleDB।

### OpenAlgo से जाएँ या सीधा broker से?

| बात                    | सीधा broker SDK                | OpenAlgo |
|------------------------|-------------------------------|----------|
| Latency (localhost)    | ~0 ms                         | ~1-2 ms  |
| 30+ brokers code reuse | ❌ हर broker अलग             | ✅ एक code |
| Symbol format unified  | ❌                            | ✅       |
| Reliability layers     | broker ही                     | broker + OpenAlgo |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended**।
> सिर्फ़ HFT-style absolute latency चाहिए तब broker SDK direct।

### Connection कटा — data कैसे recover होगा? (4 परतें)

1. **Auto-reconnect** — tenacity exponential backoff (1s → 60s)।
2. **Watchdog** — main loop हर सेकंड check करता है: agar 30 sec बिना tick (market hours में) → force reconnect।
3. **Disk spool** — DB ख़ुद down हो तो rows JSONL file में लिखी जाती हैं;
   अगले startup पर auto-replay।
4. **Gap fill** — disconnect window `collector_gaps` में log; `gap_filler.py`
   बाद में 1-min OHLC bars history API से `ohlc_1m_filled` में डालता है।

> Note: brokers का "true 1-second history" मुफ़्त में नहीं मिलता।
> Live stream 1-sec है; gap recovery 1-min granularity पर।

---

## Architecture

```
 ┌──────────┐  WebSocket  ┌──────────┐  psycopg  ┌──────────────┐
 │  Broker  │ ──────────▶ │ OpenAlgo │ ────────▶ │ collector.py │
 └──────────┘             └──────────┘           └──────┬───────┘
                                                        │ queue.Queue
                                                        ▼
                                          ┌─────────────────────┐
                                          │ Flusher (batch)     │
                                          │   ↓ DB OK ────────  │
                                          │   ↓ DB FAIL → spool │
                                          └──────────┬──────────┘
                                                     ▼
                                          ┌─────────────────────┐
                                          │   TimescaleDB       │
                                          │   ─ ticks (raw)     │
                                          │   ─ ohlc_1s (CAGG)  │
                                          │   ─ ohlc_1m_filled  │
                                          │   ─ collector_gaps  │
                                          │   ─ v_quotes_1m view│
                                          └─────────────────────┘
```

**Watchdog flow:**
```
on_quote() → state["last_tick"] = now
main loop  → if now - last_tick > 30s in market hours → raise WatchdogStale
                                                       ↓
                                              tenacity reconnect
```

---

## Setup (एक बार)

### 1. TimescaleDB
```bash
sudo apt install postgresql-16
# TimescaleDB repo से timescaledb-2-postgresql-16 install करें
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
[docs.openalgo.in](https://docs.openalgo.in/) से install + अपने broker से login + API key copy।

### 4. Python env
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# .env खोलकर API key और password भरें
```

`symbols.txt` में Nifty 50 default है — अपनी पसंद के 50 शेयर चाहिए तो वही file edit करें।

---

## रोज़ चलाना

```bash
# सुबह 9 बजे
python collector.py

# शाम (gap fill — optional)
python gap_filler.py --today

# कुछ symbols fail हुए तो retry
python gap_filler.py --retry
```

### Level-2 (order book) चाहिए?
`.env` में `MODE=depth` या `MODE=both` कर दें। फिर `depth` column में हर tick का full
order book JSONB में store होगा।

```sql
-- depth से Order Book Imbalance निकालने का example
SELECT
    ts, symbol,
    (depth -> 'buy' -> 0 ->> 'quantity')::INT  AS bid_qty_l1,
    (depth -> 'sell' -> 0 ->> 'quantity')::INT AS ask_qty_l1
FROM ticks
WHERE depth IS NOT NULL
ORDER BY ts DESC LIMIT 10;
```

> ⚠️ Note: exact JSONB shape broker-specific है। पहला tick देखकर adjust करें।

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
WorkingDirectory=/home/marketdata/nse-tick-collector
EnvironmentFile=/home/marketdata/nse-tick-collector/.env
ExecStart=/home/marketdata/nse-tick-collector/.venv/bin/python collector.py
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

OpenAlgo को भी अपनी systemd service बना लें ताकि वह crash होने पर auto-restart हो।

Cron (शाम 4 बजे gap fill):
```cron
0 16 * * 1-5 /home/marketdata/nse-tick-collector/.venv/bin/python /home/marketdata/nse-tick-collector/gap_filler.py >> /var/log/gap_filler.log 2>&1
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

-- ML training feed: 1-minute bars (live + gap-filled, NO duplicates)
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY' AND ts >= '2026-06-07'
ORDER  BY ts;

-- Pending gaps
SELECT id, started_at, ended_at, attempts, array_length(failed_symbols, 1) AS n_fail
FROM   collector_gaps WHERE filled = FALSE;

-- आज सबसे active symbols
SELECT symbol, count(*), max(ltp), min(ltp)
FROM   ticks WHERE ts > date_trunc('day', now())
GROUP  BY symbol ORDER BY 2 DESC LIMIT 10;

-- Spool में कुछ pending है? (CLI से)
-- ls -la spool/
```

---

## Tuning

| problem                         | solution |
|---------------------------------|----------|
| Insert lag                      | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा                | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए     | `schema.sql` में retention policy uncomment |
| 50+ symbols                     | OpenAlgo हज़ारों handle करता है — `symbols.txt` बढ़ाएँ |
| Watchdog बहुत agressive         | `WATCHDOG_TIMEOUT_SEC=60` |
| Multiple brokers एक साथ         | अलग `.env` से दो instances, अलग OpenAlgo ports |

---

## Files

```
nse-tick-collector/
├── schema.sql          TimescaleDB schema (v2)
├── collector.py        Main live collector (watchdog + spool + L2 optional)
├── gap_filler.py       Disconnect-period 1-min recovery (per-symbol retry)
├── symbols.txt         Nifty 50 default
├── requirements.txt    Python deps
├── .env.example        Config template
├── .gitignore
└── README.md           यह file
```

बस! market hours में `collector.py` चलाते रहें — आपके server में रोज़ का साफ़-सुथरा
1-सेकंड tick data इकट्ठा होता रहेगा।
