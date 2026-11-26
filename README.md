# Trading Volume vs. Price Volatility - AAPL, MSFT, NVDA, AMZN, GOOGL

SIS2 project (Data Collection & Preparation): an end-to-end pipeline

```
FMP API -> api_client -> data/raw_data.csv -> Kafka producer -> [Kafka topic] -> Kafka consumer
        -> Pandera validation -> pandas/NumPy cleaning -> SQLite3 (idempotent load) -> EDA + figures
```

**Research question:** is higher-than-usual trading volume associated with higher price volatility?
Volatility is measured as |daily log return|, intraday range `(high-low)/close` and 20-day realised volatility; volume is measured
relative to each stock's own previous-20-day average (so the five stocks and stock splits are comparable).

## 1. Requirements

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10 - 3.12 recommended | check with `python --version` |
| Docker Desktop | any recent | runs the Kafka broker (a real broker is mandatory) |
| FMP API key | free plan is enough | https://site.financialmodelingprep.com/developer/docs -> sign up -> *Dashboard* -> API key |

## 2. Installation

```bash
# 1) get the code
git clone <your-repo-url> project && cd project

# 2) virtual environment
python -m venv .venv
source .venv/bin/activate            # macOS / Linux
# .venv\Scripts\activate             # Windows (PowerShell: .venv\Scripts\Activate.ps1)

# 3) Python dependencies
pip install -r requirements.txt

# 4) API key
cp .env.example .env                 # Windows: copy .env.example .env
# open .env and replace put_your_key_here with your FMP key
```

## 3. Start Kafka

```bash
docker compose up -d                 # starts a single-node Kafka (KRaft) on localhost:9092
docker compose ps                    # wait until the status says "healthy" (~20-30 s)
```
The topic `stock_prices` (3 partitions) is created automatically by the producer. Stop the broker later with `docker compose down`.

## 4. Run the pipeline

```bash
python main.py                       # all stages: collect -> produce -> consume -> validate -> clean -> load -> analyze -> verify
```

Useful variants:

```bash
python main.py --skip-collect        # reuse data/raw_data.csv (saves API calls)
python main.py --stage validate      # run a single stage (collect|produce|consume|validate|clean|load|analyze|verify)
python main.py --no-test-records     # do not inject the 3 invalid test records
```

Kafka producer and consumer are separate scripts and can be run by hand (run the producer first, then the consumer; the producer writes `data/kafka_manifest.json` with the run id and message count that the consumer uses to know when to stop):

```bash
python -m src.api_client             # stage 1 (needs the API key)
python -m src.producer
python -m src.consumer
python -m src.validator && python -m src.cleaner && python -m src.loader && python -m src.analysis
```

## 5. What you get

| Output | Content |
|---|---|
| `data/raw_data.csv` | untouched API data (kept before any cleaning) |
| `data/consumed_data.jsonl` | what the consumer received from Kafka |
| `data/valid_data.csv`, `data/invalid_records.csv` | Pandera result: accepted records / rejected records with the broken rules |
| `data/clean_data.csv`, `summary_by_symbol.csv`, `pivot_volume_by_month.csv`, `pivot_volume_long.csv`, `high_activity_days.csv` | cleaning, GroupBy, pivot_table / melt, multi-condition filter |
| `data/output.db` | SQLite database (`symbols`, `daily_prices`) |
| `data/pipeline_report.json`, `docs/verification_report.md` | record counts of every stage |
| `figures/fig1..fig5*.png`, `docs/eda_findings.md` | 5 visualisations + generated findings |
| `docs/data_dictionary.md` | description of every field |

At the end `main.py` prints the reconciliation table required in section 7 of the assignment (collected / published / consumed /
valid / invalid / cleaned / stored). Expected relations: `published = collected + 3 test records`, `consumed = published`,
`invalid = 3 test records (+ any real bad rows)`, `cleaned = valid - duplicates - filtered`, `stored = cleaned`.

## 6. Module overview (`src/`)

| Module | Requirement | Role |
|---|---|---|
| `config.py` | - | settings (env vars / `.env`), paths, ticker -> company map |
| `api_client.py` | 1 | FMP requests with retries, timeouts, HTTP/JSON error handling, API key masked in logs |
| `producer.py` | 2 | JSON messages to Kafka (`acks=all`, idempotent producer, delivery callbacks, delivered/failed counters) + 3 invalid test records |
| `consumer.py` | 2 | JSON deserialisation, malformed-message handling; stops when all expected messages are read, or after 30 s idle, or 300 s max |
| `validator.py` | 3 | Pandera schema, rules R1-R7, valid/invalid split |
| `cleaner.py` | 4 | missing values, duplicates, dtypes, text normalisation, NumPy features, filtering, GroupBy, pivot_table + melt |
| `loader.py` | 5 | SQLite schema with PK + CHECKs, upsert, SQL verification queries, double-load idempotency test with checksum |
| `analysis.py` | 6 | EDA from the database, Pearson/Spearman, lead/lag, 5 figures |
| `verification.py` | 7 | compares counts across stages |

Pandera rules: **R1** numeric types / whole-number volume, **R2** no missing required fields, **R3** symbol in allowed list,
**R4** valid calendar date not in the future, **R5** prices > 0 and volume >= 0, **R6** high >= low and open/close inside [low, high],
**R7** unique (symbol, date). Invalid records are quarantined in `data/invalid_records.csv` and never reach cleaning.

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `FMP_API_KEY is not set` | create `.env` from `.env.example` and add the key |
| `HTTP 402/403 ... not in your FMP plan` | key is wrong, or the endpoint/date range is outside your plan; shorten the range with `START_DATE` in `.env` |
| `Cannot reach Kafka broker at localhost:9092` | run `docker compose up -d`, wait for `healthy`, check `docker compose logs kafka` |
| Port 9092 already in use | stop the other Kafka, or change the port mapping **and** `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` plus `KAFKA_BOOTSTRAP_SERVERS` in `.env` |
| `pip install confluent-kafka` fails to build | use Python 3.10-3.12 (prebuilt wheels exist), upgrade pip: `python -m pip install -U pip` |
| Consumer stops with fewer messages than published | increase `CONSUMER_IDLE_TIMEOUT` in `.env`; the manifest and the report show the exact counts |

## 8. Limitations

* Free FMP plan limits history length and daily request count; the default range is the last 3 years.
* Raw volume is not always split-adjusted. Relative volume (vs. the stock's own previous 20 days) reduces this effect, and daily moves
  above 35 % are treated as data artefacts (their return is set to NULL, counted in the report).
* Volume and volatility are both driven by news (earnings, macro releases); the analysis shows association, not causation.
