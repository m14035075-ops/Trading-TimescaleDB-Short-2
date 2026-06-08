# NSE Tick Collector

> 50 भारतीय शेयरों का **live 1-second tick data** OpenAlgo WebSocket से उठाकर
> अपने ही server के **TimescaleDB** में store करने वाला production-grade project।
>
> Auto-reconnect, watchdog, batch insert, disk spool fallback, gap recovery — सब शामिल।

---

## आपके सवालों के जवाब

### क्या script से अपने server के TimescaleDB में data रख सकते हैं?
**हाँ — यही standard तरीक़ा है।**
broker → WebSocket → आपकी Python script → TimescaleDB (आपका server)।

### OpenAlgo से जाएँ या सीधा broker से?

| बात                    | सीधा broker SDK              | OpenAlgo |
|------------------------|------------------------------|----------|
| Latency (localhost)    | ~0 ms                        | ~1-2 ms (negligible) |
| 30+ brokers code reuse | ❌ हर broker के लिए अलग code | ✅ एक ही code |
| Symbol/exchange format | broker-specific              | unified  |
| Broker बदलना           | पूरा rewrite                 | बस config बदलो |

> दोनों एक ही server पर — फ़र्क़ practically zero। **OpenAlgo recommended।**
> सिर्फ़ HFT-style absolute lowest latency चाहिए तब broker SDK direct।
>
> ⚠️ OpenAlgo crash हो तो script भी disconnect होगी। इसलिए OpenAlgo को भी
> **systemd service `Restart=always`** के साथ चलाएँ।

### Connection कटा — data कैसे recover होगा? (4 परतें)

1. **Auto-reconnect** — exponential backoff (1s → 60s) से retry।
2. **Watchdog** — 60s तक tick न आए (market hours में) तो force reconnect।
3. **Disk spool** — DB ख़ुद down हो तो ticks JSONL files में लिखी जाती हैं;
   अगले startup पर auto-replay (duplicate-safe ON CONFLICT)।
4. **Gap fill** — disconnect window `collector_gaps` में log; `gap_filler.py`
   बाद में 1-minute OHLC bars history API से `ohlc_1m_filled` में डालता है।

> ⚠️ **असली 1-second history कोई broker मुफ़्त नहीं देता।**
> Live stream 1-second है; gap recovery 1-minute granularity पर है।
> सब brokers की universal limitation है।

---

## Architecture

```
 ┌──────────┐  WebSocket  ┌──────────┐  psycopg  ┌──────────────┐
 │  Broker  │ ──────────▶ │ OpenAlgo │ ────────▶ │ collector.py │
 └──────────┘             └──────────┘           └──────┬───────┘
                                                        │ queue.Queue (200k)
                                                        ▼
                                          ┌─────────────────────┐
                                          │ Flusher (batch 500) │
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
                                          │   ─ v_quotes_1m     │ (ML-ready)
                                          └─────────────────────┘
```

---

## Setup (एक बार)

### 1. TimescaleDB install (Ubuntu)

```bash
sudo apt install postgresql-16
# TimescaleDB repo से timescaledb-2-postgresql-16 install करें
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql
```

### 2. Database बनाएँ

```bash
sudo -u postgres psql <<SQL
CREATE USER marketdata WITH PASSWORD 'change_me';
CREATE DATABASE marketdata OWNER marketdata;
SQL

psql -h 127.0.0.1 -U marketdata -d marketdata -f schema.sql
```

### 3. OpenAlgo चलाएँ

[docs.openalgo.in](https://docs.openalgo.in/) से install करें, अपने broker से
login करें, dashboard से API key copy करें।
Default REST: `127.0.0.1:5000`, WebSocket: `127.0.0.1:8765`।

### 4. Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env       # API key और password भरें
```

### 5. Symbols की list

`symbols.txt` में Nifty 50 default है। चाहें तो अपने 50 शेयर भर लें (एक per line)।

---

## रोज़ चलाना

### सुबह 9 बजे — Live collector

```bash
python collector.py
```

Terminal पर ऐसा दिखेगा:
```
[INFO] कुल symbols: 50  (mode=quote)
[INFO] DB pool ready (attempt 1)
[INFO] seeded state for 50 symbols
[INFO] connected & subscribed (50 symbols, mode=quote)
[DEBUG] flushed 487 ticks
[DEBUG] flushed 502 ticks
...
```

### Market बंद होने पर — `Ctrl+C`

Script pending buffer flush करके साफ़-सुथरा निकलेगा।

### शाम 4 बजे — Gap fill (optional)

```bash
python gap_filler.py --today
```

Disconnect periods के 1-minute bars history API से fill होंगे।

### कुछ symbols fail हुए तो retry

```bash
python gap_filler.py --retry
```

---

## Level-2 (Order Book) चाहिए?

`.env` में `MODE=depth` कर दें। `depth` JSONB column में पूरा order book store होगा।

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

> ⚠️ Depth JSONB का exact shape broker-specific है। पहला live tick देखकर
> JSONB path adjust करें।

---

## Production Setup (systemd)

### Service file: `/etc/systemd/system/tick-collector.service`

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

### Enable & start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tick-collector
journalctl -u tick-collector -f         # live logs
```

OpenAlgo को भी अपनी systemd service बनाएँ ताकि वह crash होने पर auto-restart हो।

### Cron — रोज़ शाम 4 बजे gap fill

```cron
0 16 * * 1-5 /home/marketdata/Trading-TimescaleDB-Short-2/.venv/bin/python /home/marketdata/Trading-TimescaleDB-Short-2/gap_filler.py --today >> /var/log/gap_filler.log 2>&1
```

---

## Sample Queries

### आज RELIANCE का 1-second OHLCV

```sql
SELECT bucket, open, high, low, close, volume, tick_count
FROM   ohlc_1s
WHERE  symbol = 'RELIANCE'
  AND  bucket > now() - interval '5 minutes'
ORDER  BY bucket DESC LIMIT 100;
```

### ML training feed (clean — full live OR history-filled)

```sql
SELECT * FROM v_quotes_1m
WHERE  symbol = 'INFY'
  AND  ts BETWEEN '2026-01-01' AND '2026-06-07'
  AND  quality IN ('full', 'history')
ORDER  BY ts;
```

### Pending gaps देखें

```sql
SELECT id, started_at, ended_at, attempts,
       array_length(failed_symbols, 1) AS n_fail, last_error
FROM   collector_gaps WHERE filled = FALSE;
```

### Volume sanity check (mid-day glitch detection)

```sql
SELECT symbol, count(*) AS suspicious_zeros
FROM   ticks
WHERE  ts > date_trunc('day', now())
  AND  tick_volume = 0 AND ltp > 0
GROUP  BY symbol HAVING count(*) > 100;
```

### आज सबसे active symbols

```sql
SELECT symbol, count(*) AS ticks, max(ltp), min(ltp)
FROM   ticks
WHERE  ts > date_trunc('day', now())
GROUP  BY symbol ORDER BY ticks DESC LIMIT 10;
```

---

## Quality Labels (v_quotes_1m)

`v_quotes_1m` view में हर 1-minute bar पर एक `quality` label है:

| Quality   | अर्थ                                                    |
|-----------|---------------------------------------------------------|
| `full`    | Live data, 60 में से ≥ 10 seconds में ticks आए, gap नहीं |
| `sparse`  | Live data, कम trades (low-liquidity), लेकिन gap नहीं    |
| `history` | Live data नहीं था, gap_filler ने history API से भरा     |
| `partial` | Live data था पर gap भी overlap हुआ — सबसे कम trustworthy |

ML training के लिए:

```sql
WHERE quality IN ('full', 'history')        -- strict
WHERE quality != 'partial'                  -- low-liquidity OK
```

---

## Tuning

| Problem                     | Solution |
|-----------------------------|----------|
| Insert lag                  | `BATCH_SIZE` 1000-2000, `FLUSH_INTERVAL_SEC=0.5` |
| Disk fast भर रहा            | `STORE_RAW_PAYLOAD=false` (default), compression policy active |
| बहुत पुराना data नहीं चाहिए | `schema.sql` में retention policy uncomment |
| 50+ symbols                 | OpenAlgo हज़ारों handle करता है |
| Watchdog बहुत agressive     | `WATCHDOG_TIMEOUT_SEC=120` |
| Spool भरा हुआ है            | `ls spool/` — startup पर auto-drain होगा |

---

## Files

```
Trading-TimescaleDB-Short-2/
├── schema.sql          TimescaleDB tables, CAGG, compression, smart views
├── collector.py        Live collector (watchdog + spool + reconnect)
├── gap_filler.py       1-min bar recovery from history API
├── symbols.txt         Nifty 50 default
├── requirements.txt    Python dependencies
├── .env.example        Configuration template
├── .gitignore
└── README.md           यह file
```

---

बस — market hours में `collector.py` चलाते रहें; production-grade 1-sec tick
data रोज़ का साफ़-सुथरा इकट्ठा होता रहेगा।
