#!/usr/bin/env python3
"""
FinTrade Data Generator
-----------------------
Daily ingestion pipeline: Alpha Vantage API → AWS S3 (JSON + Parquet) → PostgreSQL

Architecture:
    API response (JSON)  → s3://fintrade-raw/raw/json/symbol={sym}/date={date}.json
    Converted (Parquet)  → s3://fintrade-raw/raw/parquet/symbol={sym}/date={date}.parquet
    Loaded (idempotent)  → PostgreSQL raw.daily_prices

Design decisions:
    - S3 is the immutable source of truth. If the DB is corrupted, we can replay from S3.
    - ON CONFLICT DO NOTHING ensures idempotency: re-runs are safe.
    - Exponential backoff (tenacity) handles Alpha Vantage rate limits gracefully.
    - 12-second sleep between symbols respects the free-tier limit (25 req/day).

Usage:
    python generate_data.py           # Daily run: fetch latest prices
    python generate_data.py --init    # First run: generate static data + all prices
"""

import os
import json
import time
import logging
import argparse
import random
from datetime import date

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from faker import Faker
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuration (all values from environment)
# ─────────────────────────────────────────────

API_KEY      = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
S3_BUCKET    = os.getenv("S3_BUCKET", "fintrade-raw")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")           # None = real AWS | set for LocalStack
AWS_REGION   = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_KEY      = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
DB_CONN      = os.getenv(
    "FINTRADE_DB_CONN",
    "postgresql://fintrade:fintrade123@postgres-warehouse:5432/fintrade",
).replace("postgresql+psycopg2://", "postgresql://")

STOCKS = [
    ("AAPL",  "Apple Inc.",                  "Technology",             "NASDAQ"),
    ("MSFT",  "Microsoft Corporation",        "Technology",             "NASDAQ"),
    ("GOOGL", "Alphabet Inc.",                "Technology",             "NASDAQ"),
    ("AMZN",  "Amazon.com Inc.",              "Consumer Discretionary", "NASDAQ"),
    ("META",  "Meta Platforms Inc.",          "Technology",             "NASDAQ"),
    ("TSLA",  "Tesla Inc.",                   "Consumer Discretionary", "NASDAQ"),
    ("NVDA",  "NVIDIA Corporation",           "Technology",             "NASDAQ"),
    ("JPM",   "JPMorgan Chase & Co.",         "Finance",                "NYSE"),
    ("BAC",   "Bank of America Corporation",  "Finance",                "NYSE"),
    ("JNJ",   "Johnson & Johnson",            "Healthcare",             "NYSE"),
    ("PFE",   "Pfizer Inc.",                  "Healthcare",             "NYSE"),
    ("XOM",   "Exxon Mobil Corporation",      "Energy",                 "NYSE"),
    ("CVX",   "Chevron Corporation",          "Energy",                 "NYSE"),
    ("WMT",   "Walmart Inc.",                 "Consumer Staples",       "NYSE"),
    ("KO",    "The Coca-Cola Company",        "Consumer Staples",       "NYSE"),
]
SYMBOLS = [s[0] for s in STOCKS]


# ─────────────────────────────────────────────
# S3 helpers
# ─────────────────────────────────────────────

def get_s3_client():
    """
    Returns a boto3 S3 client.
    Setting AWS_ENDPOINT_URL=http://localstack:4566 points to LocalStack
    for local development; omitting it uses real AWS.
    """
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        log.info(f"Bucket '{S3_BUCKET}' not found — creating...")
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        log.info(f"Bucket '{S3_BUCKET}' created.")


# ─────────────────────────────────────────────
# Alpha Vantage ingestion
# ─────────────────────────────────────────────

@retry(
    retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError)),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(3),
    before_sleep=lambda state: log.warning(
        f"Retrying after failure (attempt {state.attempt_number})..."
    ),
)
def fetch_daily_prices(symbol: str) -> dict:
    """
    Fetch daily OHLCV data from Alpha Vantage TIME_SERIES_DAILY.

    Retry strategy (tenacity):
        - Exponential backoff: 4s → 8s → 16s → ... up to 60s
        - Max 3 attempts before propagating the exception to Airflow

    Alpha Vantage free tier quirk:
        - Rate limit returns HTTP 200 with a "Note" key (not a 429 status).
        - We detect this and sleep 65 seconds to reset the per-minute window.
    """
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",    # last 100 trading days
        "apikey":     API_KEY,
    }
    resp = requests.get(
        "https://www.alphavantage.co/query",
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Detect soft rate-limit: "Note" = per-minute burst limit, always retryable
    if "Note" in data:
        log.warning(f"[{symbol}] Per-minute rate limit hit. Sleeping 65s before retry...")
        time.sleep(65)
        raise requests.HTTPError("Rate limited — will retry")

    # "Information" = daily quota exhausted OR invalid/demo API key
    if "Information" in data:
        msg = data["Information"]
        if "demo" in msg.lower() or "api key" in msg.lower():
            # Invalid or demo key — retrying wastes quota and time
            raise ValueError(f"[{symbol}] Invalid or demo API key: {msg}")
        log.warning(f"[{symbol}] Daily API quota exhausted. Sleeping 65s before retry...")
        time.sleep(65)
        raise requests.HTTPError("Daily quota exceeded — will retry")

    if "Time Series (Daily)" not in data:
        raise ValueError(f"Unexpected API response for {symbol}: {list(data.keys())}")

    return data


def save_json_to_s3(s3, symbol: str, data: dict) -> str:
    """
    Persist the raw API JSON to S3.
    This is the immutable source of truth — allows full pipeline replay
    without re-consuming API quota.
    """
    key = f"raw/json/symbol={symbol}/date={date.today().isoformat()}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    log.info(f"  [S3] Raw JSON  → s3://{S3_BUCKET}/{key}")
    return key


def convert_to_parquet_and_upload(s3, symbol: str, data: dict) -> pd.DataFrame:
    """
    Parse the API JSON → DataFrame → Parquet → S3.

    Why Parquet?
        - Columnar format: downstream reads only scan needed columns.
        - Compression: ~75% smaller than CSV for numeric time-series.
        - Schema enforcement: type errors surface before touching the DB.
    """
    rows = [
        {
            "symbol":      symbol,
            "price_date":  dt,
            "open_price":  float(v["1. open"]),
            "high_price":  float(v["2. high"]),
            "low_price":   float(v["3. low"]),
            "close_price": float(v["4. close"]),
            "volume":      int(v["5. volume"]),
        }
        for dt, v in data["Time Series (Daily)"].items()
    ]
    df = pd.DataFrame(rows)
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date

    key = f"raw/parquet/symbol={symbol}/date={date.today().isoformat()}.parquet"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=df.to_parquet(index=False, engine="pyarrow"),
        ContentType="application/octet-stream",
    )
    log.info(f"  [S3] Parquet   → s3://{S3_BUCKET}/{key} ({len(df)} rows)")
    return df


def load_prices_to_postgres(conn, df: pd.DataFrame):
    """
    Upsert price rows into raw.daily_prices.

    Idempotency: ON CONFLICT (symbol, price_date) DO NOTHING
    Re-running this function with the same data has no effect.

    Uses execute_values() to send all rows in a single multi-value INSERT,
    avoiding N round-trips to the database.
    """
    rows = list(
        df[["symbol", "price_date", "open_price", "high_price",
            "low_price", "close_price", "volume"]].itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw.daily_prices
                (symbol, price_date, open_price, high_price,
                 low_price, close_price, volume)
            VALUES %s
            ON CONFLICT (symbol, price_date) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    log.info(f"  [DB] Batch-inserted {len(rows)} rows into raw.daily_prices")


# ─────────────────────────────────────────────
# Static data (init only)
# ─────────────────────────────────────────────

def load_stocks(conn):
    with conn.cursor() as cur:
        for row in STOCKS:
            cur.execute(
                """
                INSERT INTO raw.stocks (symbol, company_name, sector, exchange)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
                """,
                row,
            )
    conn.commit()
    log.info(f"[init] Stocks loaded: {len(STOCKS)}")


def load_accounts(conn, n: int = 50):
    fake = Faker()
    random.seed(42)
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute(
                """
                INSERT INTO raw.accounts
                    (account_id, user_name, email, account_type, balance)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO NOTHING
                """,
                (
                    f"ACC{str(i + 1).zfill(5)}",
                    fake.name(),
                    fake.email(),
                    random.choice(["individual", "institutional"]),
                    round(random.uniform(10_000, 1_000_000), 2),
                ),
            )
    conn.commit()
    log.info(f"[init] Accounts loaded: {n}")


def load_orders_and_trades(conn, n_orders: int = 500):
    fake = Faker()
    random.seed(42)
    account_ids = [f"ACC{str(i + 1).zfill(5)}" for i in range(50)]
    trade_count = 0

    with conn.cursor() as cur:
        for i in range(n_orders):
            symbol     = random.choice(SYMBOLS)
            account_id = random.choice(account_ids)
            order_type = random.choice(["buy", "sell"])
            quantity   = random.randint(1, 1000)
            price      = round(random.uniform(10, 500), 2)
            status     = random.choice(["filled", "cancelled", "partial"])
            created_at = fake.date_time_between(start_date="-1y", end_date="now")

            cur.execute(
                """
                INSERT INTO raw.orders
                    (order_id, account_id, symbol, order_type,
                     quantity, price, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    f"ORD{str(i + 1).zfill(6)}", account_id, symbol,
                    order_type, quantity, price, status, created_at,
                ),
            )

            if status in ("filled", "partial"):
                trade_qty = quantity if status == "filled" else random.randint(1, quantity)
                cur.execute(
                    """
                    INSERT INTO raw.trades
                        (trade_id, order_id, account_id, symbol, trade_type,
                         quantity, price, total_amount, traded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id) DO NOTHING
                    """,
                    (
                        f"TRD{str(i + 1).zfill(6)}",
                        f"ORD{str(i + 1).zfill(6)}",
                        account_id, symbol, order_type,
                        trade_qty, price,
                        round(trade_qty * price, 2),
                        created_at,
                    ),
                )
                trade_count += 1

    conn.commit()
    log.info(f"[init] Orders loaded: {n_orders} | Trades loaded: {trade_count}")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main(init: bool = False):
    log.info("=" * 55)
    log.info("FinTrade ingestion pipeline starting")
    log.info("=" * 55)

    s3   = get_s3_client()
    conn = psycopg2.connect(DB_CONN)

    try:
        ensure_bucket(s3)

        if init:
            log.info("--- [INIT] Loading static reference data ---")
            load_stocks(conn)
            load_accounts(conn)
            load_orders_and_trades(conn)

        log.info("--- Fetching market prices from Alpha Vantage ---")
        failed = []
        for symbol in SYMBOLS:
            log.info(f"[{symbol}] Starting...")
            try:
                raw = fetch_daily_prices(symbol)
                save_json_to_s3(s3, symbol, raw)
                df  = convert_to_parquet_and_upload(s3, symbol, raw)
            except Exception as exc:
                log.error(f"[{symbol}] S3 ingestion failed: {exc}")
                failed.append(symbol)
                time.sleep(12)
                continue

            try:
                load_prices_to_postgres(conn, df)
            except Exception as exc:
                # S3 already has the data — the DB can be replayed from S3
                # without consuming any Alpha Vantage API quota.
                log.error(
                    f"[{symbol}] PostgreSQL load failed: {exc}. "
                    "S3 data is intact and can be replayed into the DB without consuming API quota."
                )
                failed.append(symbol)

            # Respect Alpha Vantage free-tier limit (25 req/day, ~1 req per minute burst)
            time.sleep(12)
    finally:
        conn.close()

    if failed:
        log.warning(f"Symbols with errors: {failed}")
        raise RuntimeError(f"Ingestion incomplete — failed symbols: {failed}")

    log.info("=" * 55)
    log.info("Pipeline complete.")
    log.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FinTrade data ingestion pipeline")
    parser.add_argument(
        "--init",
        action="store_true",
        help="Run initial data load (stocks, accounts, orders, trades). Safe to re-run.",
    )
    main(init=parser.parse_args().init)
