# FinTrade Data Warehouse

A production-style, end-to-end data pipeline for financial market data — built to demonstrate modern Data Engineering practices.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-green)
![dbt](https://img.shields.io/badge/dbt-1.7-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Sources                                │
│                                                                     │
│   Alpha Vantage API          Faker (synthetic OLTP)                 │
│   (real market prices)       (accounts, orders, trades)             │
└──────────────┬───────────────────────────┬──────────────────────────┘
               │                           │
               ▼                           ▼
┌──────────────────────────┐   ┌───────────────────────────────────┐
│     AWS S3  (Data Lake)  │   │       PostgreSQL  raw schema       │
│                          │   │                                    │
│  raw/json/{symbol}/      │   │  raw.stocks                        │
│  raw/parquet/{symbol}/   │   │  raw.accounts                      │
│                          │   │  raw.orders                        │
│  • Immutable archive     │   │  raw.trades                        │
│  • Columnar (Parquet)    │   │  raw.daily_prices  ◄───────────────┤
│  • Source of truth       │   │                                    │
└──────────────────────────┘   └───────────────────┬───────────────┘
                                                   │ dbt
               ┌───────────────────────────────────┤
               │                                   │
               ▼                                   ▼
┌──────────────────────────┐   ┌───────────────────────────────────┐
│    staging schema        │   │         core schema               │
│    (dbt views)           │   │         (dbt views)               │
│                          │   │                                   │
│  stg_stocks              │   │  dim_stocks                       │
│  stg_accounts            │   │  dim_accounts                     │
│  stg_orders         ───► │   │  fct_orders                       │
│  stg_trades              │   │  fct_trades                       │
│  stg_daily_prices        │   │  fct_daily_prices (incremental)   │
└──────────────────────────┘   └───────────────────┬───────────────┘
                                                   │ dbt
                                                   ▼
                               ┌───────────────────────────────────┐
                               │          mart schema              │
                               │          (dbt tables)             │
                               │                                   │
                               │  mart_account_summary             │
                               │  mart_daily_trading               │
                               │  mart_stock_performance           │
                               │  mart_sector_performance          │
                               └───────────────────────────────────┘
                                          ▲
                               ┌──────────┴────────────────────────┐
                               │   Apache Airflow  (Orchestration)  │
                               │                                    │
                               │  fetch_data → dbt staging          │
                               │  → dbt test → dbt core → dbt mart  │
                               │  Schedule: @daily                  │
                               └───────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Orchestration | Apache Airflow 2.8.1 | Daily pipeline scheduling & monitoring |
| Ingestion | Python + requests | Alpha Vantage API with retry/backoff |
| Data Lake | AWS S3 (LocalStack for local dev) | Immutable raw archive in Parquet format |
| Data Warehouse | PostgreSQL 15 | Serving layer for dbt transformations |
| Transformation | dbt 1.7 | Staging → Core → Mart with tests |
| Containerization | Docker Compose | Fully reproducible local environment |
| CI/CD | GitHub Actions | Automated dbt test on every push |

---

## Data Model

### Source Data
- **15 stocks** — AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, JPM, BAC, JNJ, PFE, XOM, CVX, WMT, KO
- **50 trading accounts** — individual & institutional
- **500+ orders** with corresponding executed trades
- **365 days of OHLCV prices** per stock (~5,400 daily price records)

### dbt Layers

| Layer | Materialization | Description |
|-------|----------------|-------------|
| `staging` | View | Clean & rename raw fields, compute derived columns |
| `core` | View / Incremental | Dimensional model (star schema) |
| `mart` | Table | Business-facing aggregations |

### Key Design: Star Schema
```
          dim_stocks ──┐
                       ├── fct_trades ──► mart_account_summary
          dim_accounts ─┘                mart_daily_trading
                                         mart_stock_performance
          fct_daily_prices ──────────► mart_sector_performance
```

---

## Architecture Decisions

### Why S3 + PostgreSQL (not just PostgreSQL)?

S3 and PostgreSQL serve two distinct roles:

| | AWS S3 | PostgreSQL |
|--|--------|-----------|
| Role | Data Lake — raw archive | Data Warehouse — compute layer |
| Data | Immutable JSON + Parquet | Transformed, queryable tables |
| Purpose | Source of truth, re-runnable | dbt transformations & serving |
| Cost | Near-zero for small data | Compute on demand |

If the raw API call ever fails mid-pipeline, we can **re-load from S3 without calling the API again** — saving API quota and ensuring idempotency.

In production this PostgreSQL layer would migrate to Snowflake or BigQuery (dbt profiles change, SQL stays identical).

### Why Parquet?
- **Columnar format** — efficient for analytical queries (reading only relevant columns)
- **Compression** — ~75% smaller than CSV for numeric time-series data
- **Schema enforcement** — catches type mismatches before they reach the warehouse
- **Ecosystem** — native support in Spark, Athena, BigQuery, Databricks

### Why Incremental on `fct_daily_prices`?
Daily prices are append-only time-series data. Running a full refresh on 5,400+ rows daily is wasteful. The incremental model only processes rows where `price_date > max(price_date)` in the existing table — typical pattern for production pipelines.

### Idempotency
Every load step uses `ON CONFLICT DO NOTHING` (PostgreSQL upsert). Re-running the pipeline on the same day produces identical results — no duplicates, no data corruption.

### API Rate Limit Handling
Alpha Vantage free tier: 25 requests/day. The ingestion script implements:
- **Exponential backoff** — retries on HTTP 429 with increasing delay
- **Incremental fetch** — only pulls dates not yet in S3, conserving quota
- **Fail-safe** — on API failure, pipeline loads from existing S3 Parquet files

---

## Project Structure

```
fintrade-dw/
├── airflow/
│   └── dags/
│       └── fintrade_pipeline.py     # Airflow DAG (daily schedule)
├── data_generator/
│   ├── generate_data.py             # Main ingestion: API → S3 → PostgreSQL
│   └── requirements.txt
├── dbt/
│   └── fintrade/
│       ├── models/
│       │   ├── staging/             # stg_* views + schema.yml (tests)
│       │   ├── core/                # dim_* + fct_* + schema.yml (tests)
│       │   └── mart/                # mart_* tables + schema.yml (tests)
│       ├── macros/
│       │   ├── generate_schema_name.sql
│       │   └── test_positive_value.sql
│       └── dbt_project.yml
├── sql/
│   └── init.sql                     # Schema + raw table creation
├── scripts/
│   └── generate_fake_data.py        # Standalone script (accounts/orders/trades)
├── docker-compose.yml               # Airflow + PostgreSQL + LocalStack (S3)
├── .github/
│   └── workflows/
│       └── dbt-ci.yml               # GitHub Actions: dbt test on push
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker Desktop
- Alpha Vantage API key ([free tier](https://www.alphavantage.co/support/#api-key))

### 1. Clone & Configure

```bash
git clone https://github.com/Kellertsn/fintrade-dw.git
cd fintrade-dw
cp .env.example .env
# Edit .env and add your ALPHA_VANTAGE_API_KEY
```

### 2. Start All Services

```bash
docker-compose up -d
```

Services started:
- Airflow UI: http://localhost:8080 (admin / admin)
- PostgreSQL warehouse: localhost:5432
- LocalStack (S3): http://localhost:4566

### 3. Load Initial Data

```bash
# Generate accounts, orders, trades (synthetic OLTP data)
docker exec fintrade_airflow_scheduler \
  python /opt/airflow/data_generator/generate_data.py --init

# Or trigger via Airflow UI: enable fintrade_pipeline DAG
```

### 4. Run dbt Manually (optional)

```bash
cd dbt/fintrade
dbt run --profiles-dir . --select staging
dbt test --profiles-dir . --select staging
dbt run --profiles-dir . --select core
dbt run --profiles-dir . --select mart
```

---

## Data Quality

dbt tests are defined in `schema.yml` for every layer:

| Test Type | Examples |
|-----------|---------|
| `unique` | `trade_id`, `order_id`, `(symbol, price_date)` |
| `not_null` | All primary keys and critical columns |
| `accepted_values` | `order_type` ∈ {buy, sell}, `status` ∈ {filled, cancelled, partial} |
| `relationships` | `fct_trades.account_id` → `dim_accounts` |
| Custom: `positive_value` | `price > 0`, `quantity > 0`, `total_amount > 0` |
| Custom: `ohlc_valid` | `high_price >= low_price`, `high_price >= open_price` |

Run all tests:
```bash
dbt test --profiles-dir .
```

---

## CI/CD

GitHub Actions runs on every push to `main`:
1. Spin up PostgreSQL service
2. Run `dbt build` (compile + run + test all models)
3. Fail the PR if any test fails

See `.github/workflows/dbt-ci.yml`

---

## Background

Built as a portfolio project combining a finance industry background with modern Data Engineering tooling. The domain (stock trading, OHLCV prices, order flow) reflects real financial data patterns seen in production systems.

**Skills demonstrated:**
`ETL/ELT` · `Data Modeling (Star Schema)` · `AWS S3` · `Parquet` · `Apache Airflow` · `dbt` · `PostgreSQL` · `Docker` · `CI/CD` · `Data Quality Testing` · `API Integration` · `Incremental Loading` · `Idempotency`
