# Databricks notebook source
dbutils.widgets.text("catalog", "main", "Unity Catalog catalog name")
dbutils.widgets.text("schema", "default", "Unity Catalog schema name")

dbutils.widgets.text(
    "watchlist_table_name",
    "watchlist",
    "Source watchlist table"
)

dbutils.widgets.text(
    "stock_price_table_name",
    "stock_price_history",
    "Destination Lakebase table"
)

dbutils.widgets.text(
    "massive_secret_scope",
    "massive",
    "Massive API secret scope"
)

dbutils.widgets.text(
    "massive_secret_key",
    "api-key",
    "Massive API secret key"
)

dbutils.widgets.text(
    "massive_api_base_url",
    "https://api.massive.com",
    "Massive API base URL"
)

dbutils.widgets.text(
    "history_days",
    "30",
    "Number of calendar days of history"
)

dbutils.widgets.text(
    "max_requests_per_minute",
    "5",
    "Massive API rate limit"
)


CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

WATCHLIST_TABLE_NAME = (
    f"{CATALOG}.{SCHEMA}."
    f"{dbutils.widgets.get('watchlist_table_name')}"
)

STOCK_PRICE_TABLE_NAME = dbutils.widgets.get(
    "stock_price_table_name"
)

MASSIVE_SECRET_SCOPE = dbutils.widgets.get(
    "massive_secret_scope"
)

MASSIVE_SECRET_KEY = dbutils.widgets.get(
    "massive_secret_key"
)

MASSIVE_API_BASE_URL = dbutils.widgets.get(
    "massive_api_base_url"
)

HISTORY_DAYS = int(
    dbutils.widgets.get("history_days")
)

MAX_REQUESTS_PER_MINUTE = int(
    dbutils.widgets.get("max_requests_per_minute")
)

print("Watchlist:", WATCHLIST_TABLE_NAME)
print("Stock price table:", STOCK_PRICE_TABLE_NAME)
print("History days:", HISTORY_DAYS)
print("API rate limit:", MAX_REQUESTS_PER_MINUTE)

# COMMAND ----------

import base64
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def get_lakebase_url() -> str:
    secret = w.secrets.get_secret(
        scope="database",
        key="lakebase-url"
    )
    return base64.b64decode(secret.value).decode("utf-8")


lakebase_connection_string = get_lakebase_url()

parsed = urlparse(lakebase_connection_string)

print(
    f"Lakebase host: "
    f"{parsed.hostname}:{parsed.port or 5432}"
)

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(lakebase_connection_string)

try:
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT symbol
        FROM watchlist
        WHERE symbol IS NOT NULL
        ORDER BY symbol
    """)

    rows = cur.fetchall()

    tickers = [
        row[0].strip().upper()
        for row in rows
        if row[0]
    ]

    print(f"Found {len(tickers)} tickers:")
    print(tickers)

finally:
    cur.close()
    conn.close()

# COMMAND ----------

import base64
import requests
from datetime import date, timedelta

def get_massive_api_key() -> str:
    secret = w.secrets.get_secret(
        scope=MASSIVE_SECRET_SCOPE,
        key=MASSIVE_SECRET_KEY
    )
    return base64.b64decode(secret.value).decode("utf-8")


ticker = tickers[0]

to_date = date.today()
from_date = to_date - timedelta(days=30)

url = (
    f"{MASSIVE_API_BASE_URL}/v2/aggs/ticker/"
    f"{ticker}/range/1/day/{from_date}/{to_date}"
)

headers = {
    "Authorization": f"Bearer {get_massive_api_key()}",
    "Content-Type": "application/json"
}

response = requests.get(
    url,
    headers=headers,
    params={
        "adjusted": "true",
        "sort": "asc",
        "limit": 5000,
    },
    timeout=30,
)

response.raise_for_status()

data = response.json()
prices = data.get("results", [])

print(f"Ticker: {ticker}")
print(f"Period: {from_date} to {to_date}")
print(f"Records returned: {len(prices)}")
print("\nFirst record:")
print(prices[0] if prices else "No data")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    LongType,
)
from datetime import datetime, timezone


price_rows = []

for record in prices:
    trade_date = datetime.fromtimestamp(
        record["t"] / 1000,
        tz=timezone.utc
    ).date()

    price_rows.append({
        "ticker": ticker,
        "trade_date": trade_date,
        "open_price": float(record["o"]) if record.get("o") is not None else None,
        "high_price": float(record["h"]) if record.get("h") is not None else None,
        "low_price": float(record["l"]) if record.get("l") is not None else None,
        "close_price": float(record["c"]) if record.get("c") is not None else None,
        "volume": float(record["v"]) if record.get("v") is not None else None,
        "vwap": float(record["vw"]) if record.get("vw") is not None else None,
    })


price_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
])


price_df = spark.createDataFrame(
    price_rows,
    schema=price_schema
)

print(f"Spark DataFrame records: {price_df.count()}")

display(
    price_df.orderBy("trade_date")
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

price_df = price_df.withColumn(
    "updated_at",
    current_timestamp()
)

display(price_df)
price_df.printSchema()

# COMMAND ----------

[v for v in dir() if any(x in v.lower() for x in ["url", "user", "password", "host", "lakebase"])]

# COMMAND ----------

import psycopg2

# Convert Spark DataFrame to Python rows
price_rows = price_df.collect()

print(f"Rows ready to insert: {len(price_rows)}")
print(f"First row: {price_rows[0]}")

# COMMAND ----------

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

cur.execute("""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE tablename = 'stock_price_history'
""")

for row in cur.fetchall():
    print(row)

cur.close()
conn.close()

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

insert_sql = """
    INSERT INTO stock_price_history (
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        vwap,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, trade_date)
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        vwap = EXCLUDED.vwap,
        updated_at = EXCLUDED.updated_at
"""

for row in price_rows:
    cur.execute(
        insert_sql,
        (
            row.ticker,
            row.trade_date,
            row.open_price,
            row.high_price,
            row.low_price,
            row.close_price,
            row.volume,
            row.vwap,
            row.updated_at
        )
    )

conn.commit()

print(f"Successfully upserted {len(price_rows)} rows for AAPL")

cur.close()
conn.close()

# COMMAND ----------

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

cur.execute("""
    SELECT symbol
    FROM watchlist
    ORDER BY symbol
""")

watchlist_tickers = [row[0] for row in cur.fetchall()]

cur.close()
conn.close()

print(f"Watchlist tickers: {watchlist_tickers}")
print(f"Total tickers: {len(watchlist_tickers)}")

# COMMAND ----------

import requests
from datetime import date, timedelta

all_price_records = []

to_date = date.today()
from_date = to_date - timedelta(days=30)

headers = {
    "Authorization": f"Bearer {get_massive_api_key()}",
    "Content-Type": "application/json"
}

for ticker in watchlist_tickers:

    print(f"Fetching {ticker}...")

    url = (
        f"{MASSIVE_API_BASE_URL}/v2/aggs/ticker/"
        f"{ticker}/range/1/day/{from_date}/{to_date}"
    )

    try:
        response = requests.get(
            url,
            headers=headers,
            params={
                "adjusted": "true",
                "sort": "asc",
                "limit": 5000,
            },
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()
        prices = data.get("results", [])

        print(f"  Records returned: {len(prices)}")

        for record in prices:
            all_price_records.append({
                "ticker": ticker,
                "trade_date": date.fromtimestamp(record["t"] / 1000),
                "open_price": record["o"],
                "high_price": record["h"],
                "low_price": record["l"],
                "close_price": record["c"],
                "volume": record["v"],
                "vwap": record["vw"],
            })

    except Exception as e:
        print(f"  ERROR: {e}")

print("\n" + "=" * 50)
print(f"Total records collected: {len(all_price_records)}")

# COMMAND ----------

import time
from datetime import date, timedelta

failed_tickers = ["NVDA", "TSLA"]

for ticker in failed_tickers:

    print(f"Retrying {ticker}...")

    url = (
        f"{MASSIVE_API_BASE_URL}/v2/aggs/ticker/"
        f"{ticker}/range/1/day/{from_date}/{to_date}"
    )

    for attempt in range(1, 4):

        try:
            response = requests.get(
                url,
                headers=headers,
                params={
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": 5000,
                },
                timeout=30,
            )

            if response.status_code == 429:
                wait_time = attempt * 10
                print(f"  Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()
            prices = data.get("results", [])

            print(f"  Records returned: {len(prices)}")

            for record in prices:
                all_price_records.append({
                    "ticker": ticker,
                    "trade_date": date.fromtimestamp(record["t"] / 1000),
                    "open_price": record["o"],
                    "high_price": record["h"],
                    "low_price": record["l"],
                    "close_price": record["c"],
                    "volume": record["v"],
                    "vwap": record["vw"],
                })

            break

        except Exception as e:
            print(f"  Attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(attempt * 10)

print("\n" + "=" * 50)
print(f"Total records collected: {len(all_price_records)}")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

price_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
])

all_prices_df = spark.createDataFrame(
    all_price_records,
    schema=price_schema
)

display(all_prices_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

all_prices_df = all_prices_df.withColumn(
    "updated_at",
    current_timestamp()
)

all_prices_df.printSchema()

# COMMAND ----------

display(
    all_prices_df
    .orderBy("ticker", "trade_date")
    .limit(10)
)

# COMMAND ----------

price_rows_all = all_prices_df.collect()

print(f"Rows ready for Lakebase: {len(price_rows_all)}")
print(f"First row: {price_rows_all[0]}")

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

insert_sql = """
    INSERT INTO stock_price_history (
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        vwap,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, trade_date)
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        vwap = EXCLUDED.vwap,
        updated_at = EXCLUDED.updated_at
"""

for row in price_rows_all:
    cur.execute(
        insert_sql,
        (
            row.ticker,
            row.trade_date,
            row.open_price,
            row.high_price,
            row.low_price,
            row.close_price,
            row.volume,
            row.vwap,
            row.updated_at
        )
    )

conn.commit()

print(f"Successfully upserted {len(price_rows_all)} rows")

cur.close()
conn.close()

# COMMAND ----------

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

cur.execute("""
    SELECT
        ticker,
        COUNT(*) AS price_records,
        MIN(trade_date) AS first_date,
        MAX(trade_date) AS last_date
    FROM stock_price_history
    GROUP BY ticker
    ORDER BY ticker
""")

results = cur.fetchall()

print("Stock price history verification")
print("=" * 70)

total_records = 0

for ticker, count, first_date, last_date in results:
    print(
        f"{ticker}: {count} records | "
        f"{first_date} → {last_date}"
    )
    total_records += count

print("=" * 70)
print(f"Total records: {total_records}")

cur.close()
conn.close()