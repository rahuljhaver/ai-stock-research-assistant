# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest Ticker News -> Vector Embeddings (Delta + Lakebase)
# MAGIC
# MAGIC This notebook is part of the **Context Engineering on Databricks** course.
# MAGIC
# MAGIC It writes to **both** Unity Catalog Delta tables AND Lakebase Postgres:
# MAGIC
# MAGIC 1. Reads the `watchlist` table from Unity Catalog to find out which ticker
# MAGIC    symbols are currently being tracked.
# MAGIC 2. Fetches recent news for those tickers directly from the Massive
# MAGIC    `/v2/reference/news` endpoint, rate limited to stay within the free
# MAGIC    Massive API tier's strict quota, and writes to:
# MAGIC    - **Delta**: `ticker_news_documents` (JSON columns as STRING)
# MAGIC    - **Lakebase**: `ticker_news_documents` (with JSONB columns)
# MAGIC 3. Computes a sentence embedding for each article (title + description)
# MAGIC    using Spark, distributed across the cluster via a pandas UDF, and writes to:
# MAGIC    - **Delta**: `ticker_news_embeddings` (embeddings as ARRAY<DOUBLE>)
# MAGIC    - **Lakebase**: `ticker_news_embeddings` (embeddings as pgvector VECTOR)
# MAGIC 4. Fetches the full article body for each `article_url` (via
# MAGIC    `trafilatura`, which strips nav/ads/boilerplate from the raw HTML),
# MAGIC    splits it into overlapping text chunks, embeds each chunk, and writes to:
# MAGIC    - **Delta**: `ticker_news_chunk_embeddings` (ARRAY<DOUBLE>)
# MAGIC    - **Lakebase**: `ticker_news_chunk_embeddings` (pgvector VECTOR)
# MAGIC
# MAGIC **Dual-write strategy** ensures:
# MAGIC - Delta tables for Databricks-native processing and analysis
# MAGIC - Lakebase Postgres for Flask app and external system queries

# COMMAND ----------

# MAGIC %pip install -q sentence-transformers trafilatura requests psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC Widgets let you override the source/destination table names and the
# MAGIC embedding model without editing the notebook - useful when running this
# MAGIC as a scheduled Databricks Job.

# COMMAND ----------

# DBTITLE 1,Cell 5
# Unity Catalog location for Delta tables
dbutils.widgets.text("catalog", "main", "Unity Catalog catalog name")
dbutils.widgets.text("schema", "default", "Unity Catalog schema name")

dbutils.widgets.text("watchlist_table_name", "watchlist", "Source table (watchlist symbols)")
dbutils.widgets.text("news_table_name", "ticker_news_documents", "Destination table (raw news)")
dbutils.widgets.text("embeddings_table_name", "ticker_news_embeddings", "Destination table (vectors)")
dbutils.widgets.text("chunk_embeddings_table_name", "ticker_news_chunk_embeddings", "Destination table (chunk vectors)")
dbutils.widgets.text("embedding_model", "sentence-transformers/all-MiniLM-L6-v2", "Embedding model")
dbutils.widgets.text("massive_secret_scope", "massive", "Massive API secret scope")
dbutils.widgets.text("massive_secret_key", "api-key", "Massive API secret key")
dbutils.widgets.text("massive_api_base_url", "https://api.massive.com", "Massive API base URL")
dbutils.widgets.text("news_fetch_limit", "50", "Max articles to fetch per ticker")
dbutils.widgets.text("max_requests_per_minute", "5", "Massive API rate limit (free tier is strict)")
dbutils.widgets.text("chunk_size", "800", "Article content chunk size (chars)")
dbutils.widgets.text("chunk_overlap", "100", "Article content chunk overlap (chars)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

WATCHLIST_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('watchlist_table_name')}"
NEWS_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('news_table_name')}"
EMBEDDINGS_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('embeddings_table_name')}"
CHUNK_EMBEDDINGS_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('chunk_embeddings_table_name')}"
EMBEDDING_MODEL_NAME = dbutils.widgets.get("embedding_model")
MASSIVE_SECRET_SCOPE = dbutils.widgets.get("massive_secret_scope")
MASSIVE_SECRET_KEY = dbutils.widgets.get("massive_secret_key")
MASSIVE_API_BASE_URL = dbutils.widgets.get("massive_api_base_url")
NEWS_FETCH_LIMIT = int(dbutils.widgets.get("news_fetch_limit"))
MAX_REQUESTS_PER_MINUTE = int(dbutils.widgets.get("max_requests_per_minute"))
CHUNK_SIZE = int(dbutils.widgets.get("chunk_size"))
CHUNK_OVERLAP = int(dbutils.widgets.get("chunk_overlap"))

# Different sentence-transformers models emit different vector sizes, and the
# pgvector column type (VECTOR(N)) must match exactly. Rather than hardcoding
# one dimension, switch on the model name so swapping EMBEDDING_MODEL_NAME via
# the widget above automatically resizes the destination table's vector column.
match EMBEDDING_MODEL_NAME:
    case "sentence-transformers/all-MiniLM-L6-v2":
        EMBEDDING_DIM = 384
    case "sentence-transformers/all-MiniLM-L12-v2":
        EMBEDDING_DIM = 384
    case "sentence-transformers/all-mpnet-base-v2":
        EMBEDDING_DIM = 768
    case "sentence-transformers/paraphrase-multilingual-mpnet-base-v2":
        EMBEDDING_DIM = 768
    case "BAAI/bge-small-en-v1.5":
        EMBEDDING_DIM = 384
    case "BAAI/bge-base-en-v1.5":
        EMBEDDING_DIM = 768
    case "BAAI/bge-large-en-v1.5":
        EMBEDDING_DIM = 1024
    case "text-embedding-3-small":
        EMBEDDING_DIM = 1536
    case "text-embedding-3-large":
        EMBEDDING_DIM = 3072
    case _:
        raise ValueError(
            f"Unknown embedding model {EMBEDDING_MODEL_NAME!r} - add its output "
            "dimension to the match/case block above before running this notebook."
        )

print(f"Using model {EMBEDDING_MODEL_NAME!r} -> {EMBEDDING_DIM}-dim vectors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Lakebase Connection
# MAGIC
# MAGIC Data is written to both Unity Catalog Delta tables AND synced to Lakebase
# MAGIC Postgres for external application access. The Lakebase connection uses the
# MAGIC same secret (scope `database`, key `lakebase-url`) as the Flask app.

# COMMAND ----------

import base64
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get Lakebase connection details
def get_lakebase_url() -> str:
    secret = w.secrets.get_secret(scope="database", key="lakebase-url")
    return base64.b64decode(secret.value).decode("utf-8")

lakebase_url = get_lakebase_url()
parsed = urlparse(lakebase_url)

# Build connection string for psycopg2 (used in sync functions)
lakebase_connection_string = lakebase_url

print(f"Unity Catalog location: {CATALOG}.{SCHEMA}")
print(f"Lakebase host: {parsed.hostname}:{parsed.port or 5432}")
print(f"\nTarget Delta tables:")
print(f"  - {WATCHLIST_TABLE_NAME}")
print(f"  - {NEWS_TABLE_NAME}")
print(f"  - {EMBEDDINGS_TABLE_NAME}")
print(f"  - {CHUNK_EMBEDDINGS_TABLE_NAME}")
print(f"\nTarget Lakebase tables:")
print(f"  - {dbutils.widgets.get('news_table_name')}")
print(f"  - {dbutils.widgets.get('embeddings_table_name')}")
print(f"  - {dbutils.widgets.get('chunk_embeddings_table_name')}")

# COMMAND ----------

# DBTITLE 1,Test JDBC Connection
# Test Delta table access
print("Testing Delta table access...")
try:
    test_df = spark.table(WATCHLIST_TABLE_NAME)
    count = test_df.count()
    print(f"✅ Delta: Found {count} rows in {WATCHLIST_TABLE_NAME}")
    display(test_df.limit(5))
except Exception as e:
    print(f"❌ Delta access failed: {e}")
    print(f"\nCreate the table with:")
    print(f"CREATE TABLE {WATCHLIST_TABLE_NAME} (user_id STRING, symbol STRING, added_at TIMESTAMP);")

# Test Lakebase connection
print("\nTesting Lakebase connection...")
try:
    import psycopg2
    conn = psycopg2.connect(lakebase_connection_string)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM watchlist")
    lb_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"✅ Lakebase: Found {lb_count} rows in watchlist")
except Exception as e:
    print(f"❌ Lakebase connection failed: {e}")
    print("Make sure psycopg2 is installed: %pip install psycopg2-binary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Setup
# MAGIC
# MAGIC This notebook writes to **both Delta and Lakebase Postgres**.
# MAGIC
# MAGIC ### Delta Tables (Unity Catalog)
# MAGIC Created automatically on first write:
# MAGIC 1. `ticker_news_documents` - Raw news (JSON as STRING)
# MAGIC 2. `ticker_news_embeddings` - Embeddings (ARRAY<DOUBLE>)
# MAGIC 3. `ticker_news_chunk_embeddings` - Chunk embeddings (ARRAY<DOUBLE>)
# MAGIC
# MAGIC ### Lakebase Tables (Postgres)
# MAGIC Must be created manually before running:
# MAGIC 1. Run `sql/01_setup_news_table.sql` to create `ticker_news_documents`
# MAGIC    (with JSONB columns for keywords and payload)
# MAGIC 2. Run `sql/02_setup_embeddings_table.sql` to create `ticker_news_embeddings`
# MAGIC    (with pgvector VECTOR columns)
# MAGIC 3. Run `sql/03_setup_chunk_embeddings_table.sql` to create `ticker_news_chunk_embeddings`
# MAGIC    (with pgvector VECTOR columns)
# MAGIC
# MAGIC Replace `{{EMBEDDING_DIM}}` in the SQL files with the model dimension (e.g., 384).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch news from Massive for watchlisted tickers
# MAGIC
# MAGIC This ETL is now self-contained: instead of relying on the Flask app's
# MAGIC `POST /news/sync` route to have populated `ticker_news_documents` ahead of
# MAGIC time, the notebook queries the `watchlist` table in Lakebase directly to
# MAGIC find out which tickers are being tracked, then pulls news for exactly
# MAGIC those tickers from Massive itself.
# MAGIC
# MAGIC The free Massive API tier is rate-limited very aggressively, so requests
# MAGIC are made **serially** (not distributed across Spark workers) with a sleep
# MAGIC between calls that enforces `MAX_REQUESTS_PER_MINUTE` (default 5/min).

# COMMAND ----------

import base64 as _b64
import json as _json
import time
from datetime import datetime

import requests
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType


def get_massive_api_key() -> str:
    secret = w.secrets.get_secret(scope=MASSIVE_SECRET_SCOPE, key=MASSIVE_SECRET_KEY)
    return _b64.b64decode(secret.value).decode("utf-8")


def get_watchlist_tickers() -> list[str]:
    """Distinct, uppercased ticker symbols currently tracked across all users
    in the watchlist table - these are the only tickers we fetch news for."""
    watchlist_df = spark.table(WATCHLIST_TABLE_NAME)
    symbols = watchlist_df.select("symbol").distinct().collect()
    return [row.symbol.strip().upper() for row in symbols if row.symbol]


def fetch_news_for_ticker(session: requests.Session, ticker: str, limit: int) -> list[dict]:
    """Single GET /v2/reference/news call for one ticker (mirrors
    MassiveClient.get_news in massive_client.py)."""
    resp = session.get(
        f"{MASSIVE_API_BASE_URL}/v2/reference/news",
        params={"ticker": ticker, "limit": limit, "order": "desc", "sort": "published_utc"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def sync_news_to_lakebase(ticker: str, articles: list[dict]) -> int:
    """Sync news articles to Lakebase Postgres using psycopg2 with JSONB casting.
    Returns count of new articles inserted."""
    import psycopg2
    from psycopg2.extras import execute_values
    
    if not articles:
        return 0
    
    conn = psycopg2.connect(lakebase_connection_string)
    cur = conn.cursor()
    
    # Check which IDs already exist
    article_ids = [str(article.get("id")) for article in articles]
    cur.execute(
        f"SELECT id FROM {dbutils.widgets.get('news_table_name')} WHERE id = ANY(%s)",
        (article_ids,)
    )
    existing_ids = {row[0] for row in cur.fetchall()}
    
    # Prepare rows for new articles only
    rows = []
    for article in articles:
        article_id = str(article.get("id"))
        if article_id in existing_ids:
            continue
            
        sentiment = None
        sentiment_reasoning = None
        for insight in article.get("insights", []) or []:
            if insight.get("ticker") == ticker:
                sentiment = insight.get("sentiment")
                sentiment_reasoning = insight.get("sentiment_reasoning")
                break
        
        publisher = article.get("publisher") or {}
        rows.append((
            article_id,
            ticker,
            article.get("title", ""),
            article.get("description"),
            article.get("author"),
            article.get("article_url"),
            publisher.get("name"),
            _json.dumps(article.get("keywords", [])),  # Will be cast to JSONB
            sentiment,
            sentiment_reasoning,
            article.get("published_utc"),
            _json.dumps(article),  # Will be cast to JSONB
        ))
    
    if rows:
        # Use raw SQL with explicit ::jsonb casts for keywords and payload
        execute_values(
            cur,
            f"""INSERT INTO {dbutils.widgets.get('news_table_name')} 
                (id, ticker, title, description, author, article_url, publisher_name, 
                 keywords, sentiment, sentiment_reasoning, published_utc, payload, synced_at)
                VALUES %s""",
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, NOW())"
        )
        conn.commit()
    
    count = len(rows)
    cur.close()
    conn.close()
    return count


def sync_news_to_delta(ticker: str, articles: list[dict]) -> int:
    """Convert Massive API response to Spark DataFrame and write to Delta table.
    Uses append mode with deduplication by reading existing IDs first."""
    rows = []
    rows = []
    for article in articles:
        sentiment = None
        sentiment_reasoning = None
        for insight in article.get("insights", []) or []:
            if insight.get("ticker") == ticker:
                sentiment = insight.get("sentiment")
                sentiment_reasoning = insight.get("sentiment_reasoning")
                break

        publisher = article.get("publisher") or {}
        rows.append(
            {
                "id": str(article.get("id")),
                "ticker": ticker,
                "title": article.get("title", ""),
                "description": article.get("description"),
                "author": article.get("author"),
                "article_url": article.get("article_url"),
                "publisher_name": publisher.get("name"),
                "keywords": _json.dumps(article.get("keywords", [])),
                "sentiment": sentiment,
                "sentiment_reasoning": sentiment_reasoning,
                "published_utc": article.get("published_utc"),
                "payload": _json.dumps(article),
            }
        )

    if not rows:
        return 0

    # Create DataFrame from API response
    new_df = spark.createDataFrame(rows).withColumn("synced_at", current_timestamp())

    # Read existing article IDs to avoid duplicates (only if table exists)
    if spark.catalog.tableExists(NEWS_TABLE_NAME):
        existing_ids = spark.table(NEWS_TABLE_NAME).select("id").distinct()
        # Filter out existing IDs
        new_df = new_df.join(existing_ids, on="id", how="left_anti")

    # Write new records to Delta table
    count = new_df.count()
    if count > 0:
        new_df.write.format("delta").mode("append").saveAsTable(NEWS_TABLE_NAME)
    return count


print(f"Fetching news for tickers in {WATCHLIST_TABLE_NAME}")
print(f"Writing to Delta table: {NEWS_TABLE_NAME}\n")

tickers = get_watchlist_tickers()
print(f"Found {len(tickers)} distinct watchlisted tickers: {tickers}")

# Enforce MAX_REQUESTS_PER_MINUTE by spacing calls evenly across a minute -
# e.g. 5/min -> one request every 12s. Sleeping BEFORE each call after the
# first keeps this correct even if a single request itself takes a while.
_seconds_between_requests = 60.0 / MAX_REQUESTS_PER_MINUTE

_massive_session = requests.Session()
_massive_session.headers.update(
    {"Authorization": f"Bearer {get_massive_api_key()}", "Content-Type": "application/json"}
)

delta_synced = 0
lakebase_synced = 0
for i, ticker in enumerate(tickers):
    if i > 0:
        time.sleep(_seconds_between_requests)
    try:
        articles = fetch_news_for_ticker(_massive_session, ticker, NEWS_FETCH_LIMIT)
        # Write to both Delta and Lakebase
        delta_synced += sync_news_to_delta(ticker, articles)
        lakebase_synced += sync_news_to_lakebase(ticker, articles)
    except Exception as exc:
        print(f"Skipping {ticker}: failed to fetch/sync news ({exc})")
        continue

print(f"\nSync complete:")
print(f"  Delta: {delta_synced} new articles -> {NEWS_TABLE_NAME}")
print(f"  Lakebase: {lakebase_synced} new articles -> {dbutils.widgets.get('news_table_name')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load raw news documents with Spark
# MAGIC
# MAGIC Reads the whole `ticker_news_documents` table (just synced from Massive
# MAGIC above) via JDBC into a Spark DataFrame so embedding computation can be
# MAGIC distributed across the cluster.

# COMMAND ----------

news_df = (
    spark.table(NEWS_TABLE_NAME)
    .selectExpr(
        "id",
        "ticker",
        "title",
        "description",
        "article_url",
        "published_utc",
        # Embed on title + description together for richer context.
        "trim(concat(coalesce(title, ''), '. ', coalesce(description, ''))) AS embedding_text",
    )
    .filter("embedding_text IS NOT NULL AND embedding_text != ''")
)

print(f"Loaded {news_df.count()} news documents from {NEWS_TABLE_NAME}")
display(news_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute embeddings (distributed pandas UDF)
# MAGIC
# MAGIC Loads the sentence-transformers model once per executor process (not per
# MAGIC row) and applies it in batches via `mapInPandas`, which scales across
# MAGIC however many workers the cluster has.

# COMMAND ----------

from typing import Iterator

import pandas as pd
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType

embeddings_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("ticker", StringType(), False),
        StructField("title", StringType(), False),
        StructField("published_utc", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), False),
    ]
)


def embed_partitions(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Runs once per Spark partition/task: load the model once, then embed
    every batch of rows handed to this partition."""
    import os
    from sentence_transformers import SentenceTransformer

    # Set cache directory to writable location on Serverless workers
    cache_dir = '/tmp/sentence_transformers_cache'
    os.makedirs(cache_dir, exist_ok=True)
    os.environ['HF_HOME'] = cache_dir
    os.environ['TRANSFORMERS_CACHE'] = cache_dir
    os.environ['SENTENCE_TRANSFORMERS_HOME'] = cache_dir
    
    model = SentenceTransformer(EMBEDDING_MODEL_NAME, cache_folder=cache_dir)

    for batch in iterator:
        vectors = model.encode(batch["embedding_text"].tolist(), show_progress_bar=False)
        yield pd.DataFrame(
            {
                "id": batch["id"],
                "ticker": batch["ticker"],
                "title": batch["title"],
                "published_utc": batch["published_utc"].astype(str),
                "embedding": [v.tolist() for v in vectors],
            }
        )


embeddings_df = news_df.mapInPandas(embed_partitions, schema=embeddings_schema)

print(f"Computed {embeddings_df.count()} embeddings using {EMBEDDING_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the pgvector destination table exists
# MAGIC
# MAGIC `pgvector` isn't a JDBC-native type, but plain SQL text (`vector(N)`,
# MAGIC `::vector` casts) works fine over a raw JDBC connection - no psycopg2
# MAGIC needed.

# COMMAND ----------

# Embeddings will be written to Delta table with the correct dimension
print(f"Embedding dimension: {EMBEDDING_DIM}")
print(f"Target Delta table: {EMBEDDINGS_TABLE_NAME}")
print("\nTable will be created automatically on first write.")

# COMMAND ----------

# DBTITLE 1,Verify Lakebase Sync
import psycopg2
import pandas as pd

print("Checking Lakebase Postgres tables...\n")

conn = psycopg2.connect(lakebase_connection_string)
cur = conn.cursor()

# Check news documents table
try:
    cur.execute(f"SELECT COUNT(*) FROM {dbutils.widgets.get('news_table_name')}")
    news_count = cur.fetchone()[0]
    print(f"✅ {dbutils.widgets.get('news_table_name')}: {news_count} rows")
    
    cur.execute(f"""
        SELECT ticker, COUNT(*) as article_count, MAX(synced_at) as last_sync 
        FROM {dbutils.widgets.get('news_table_name')} 
        GROUP BY ticker 
        ORDER BY article_count DESC
    """)
    news_stats = cur.fetchall()
    print("\nNews by ticker:")
    for ticker, count, last_sync in news_stats:
        print(f"  {ticker}: {count} articles (last sync: {last_sync})")
except Exception as e:
    print(f"❌ Error checking news table: {e}")

print()

# Check embeddings table
try:
    cur.execute(f"SELECT COUNT(*) FROM {dbutils.widgets.get('embeddings_table_name')}")
    emb_count = cur.fetchone()[0]
    print(f"✅ {dbutils.widgets.get('embeddings_table_name')}: {emb_count} rows")
    
    cur.execute(f"""
        SELECT ticker, COUNT(*) as embedding_count, MAX(embedded_at) as last_embedded
        FROM {dbutils.widgets.get('embeddings_table_name')}
        GROUP BY ticker
        ORDER BY embedding_count DESC
    """)
    emb_stats = cur.fetchall()
    print("\nEmbeddings by ticker:")
    for ticker, count, last_embedded in emb_stats:
        print(f"  {ticker}: {count} embeddings (last embedded: {last_embedded})")
    
    # Sample one embedding to verify vector format (pgvector type)
    cur.execute(f"""
        SELECT id, ticker, title, model_name
        FROM {dbutils.widgets.get('embeddings_table_name')}
        LIMIT 1
    """)
    sample = cur.fetchone()
    if sample:
        print(f"\nSample embedding: id={sample[0][:20]}..., ticker={sample[1]}, model={sample[3]}")
except Exception as e:
    print(f"❌ Error checking embeddings table: {e}")
    conn.rollback()  # Rollback transaction on error

print()

# Check chunk embeddings table
try:
    cur.execute(f"SELECT COUNT(*) FROM {dbutils.widgets.get('chunk_embeddings_table_name')}")
    chunk_count = cur.fetchone()[0]
    print(f"✅ {dbutils.widgets.get('chunk_embeddings_table_name')}: {chunk_count} rows")
    
    cur.execute(f"""
        SELECT ticker, COUNT(*) as chunk_count, MAX(embedded_at) as last_embedded
        FROM {dbutils.widgets.get('chunk_embeddings_table_name')}
        GROUP BY ticker
        ORDER BY chunk_count DESC
    """)
    chunk_stats = cur.fetchall()
    print("\nChunk embeddings by ticker:")
    for ticker, count, last_embedded in chunk_stats:
        print(f"  {ticker}: {count} chunks (last embedded: {last_embedded})")
    
    # Sample one chunk embedding
    cur.execute(f"""
        SELECT id, article_id, chunk_index, model_name,
               substring(chunk_text, 1, 50) as text_preview
        FROM {dbutils.widgets.get('chunk_embeddings_table_name')}
        LIMIT 1
    """)
    sample = cur.fetchone()
    if sample:
        print(f"\nSample chunk: id={sample[0]}, chunk_index={sample[2]}, model={sample[3]}")
        print(f"  Text: {sample[4]}...")
except Exception as e:
    print(f"❌ Error checking chunk embeddings table: {e}")
    conn.rollback()  # Rollback transaction on error

cur.close()
conn.close()

print("\n" + "="*60)
print("Sync verification complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write embeddings to Delta table
# MAGIC
# MAGIC Embeddings are written as arrays of doubles, which Delta natively supports.
# MAGIC Deduplication is handled by checking existing IDs before writing.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr, lit
from pyspark.sql.types import DoubleType

# Add model_name and embedded_at columns
embeddings_with_meta = embeddings_df.withColumn("model_name", lit(EMBEDDING_MODEL_NAME)).withColumn(
    "embedded_at", current_timestamp()
)

# Convert embedding array from ArrayType(FloatType) to ArrayType(DoubleType)
# Postgres JDBC expects DOUBLE PRECISION[] for vector columns
embeddings_final = embeddings_with_meta.withColumn(
    "embedding", expr("transform(embedding, x -> cast(x as double))")
)

# Read existing embedding IDs to avoid duplicates (deduplication strategy)
if spark.catalog.tableExists(EMBEDDINGS_TABLE_NAME):
    existing_ids = spark.table(EMBEDDINGS_TABLE_NAME).select("id").distinct()
    # Filter out existing IDs (left anti-join keeps only new records)
    new_embeddings = embeddings_final.join(existing_ids, on="id", how="left_anti")
else:
    # Table doesn't exist yet - write all embeddings
    new_embeddings = embeddings_final

embedding_count = new_embeddings.count()
if embedding_count > 0:
    # Write to Delta table - embeddings are stored as ARRAY<DOUBLE>
    new_embeddings.write.format("delta").mode("append").saveAsTable(EMBEDDINGS_TABLE_NAME)
    print(f"✅ Delta: Wrote {embedding_count} new embeddings to {EMBEDDINGS_TABLE_NAME}")
    
    # Also sync to Lakebase with pgvector format
    import psycopg2
    from psycopg2.extras import execute_values
    
    embeddings_to_sync = new_embeddings.collect()
    conn = psycopg2.connect(lakebase_connection_string)
    cur = conn.cursor()
    
    rows = [
        (
            row.id,
            row.ticker,
            row.title,
            row.published_utc,
            str(row.embedding),  # Will be cast to vector
            row.model_name,
        )
        for row in embeddings_to_sync
    ]
    
    execute_values(
        cur,
        f"""INSERT INTO {dbutils.widgets.get('embeddings_table_name')}
            (id, ticker, title, published_utc, embedding, model_name, embedded_at)
            VALUES %s""",
        rows,
        template="(%s, %s, %s, %s, %s::vector, %s, NOW())"
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Lakebase: Wrote {embedding_count} new embeddings to {dbutils.widgets.get('embeddings_table_name')}")
else:
    print("No new embeddings to write (all already exist).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch and chunk article content
# MAGIC
# MAGIC Title/description only gets you so far - the actual article body lives at
# MAGIC `article_url` on the publisher's site. This step fetches each URL, uses
# MAGIC `trafilatura` to extract just the article text (stripping nav/ads/related
# MAGIC links/etc.), and splits it into overlapping chunks so each chunk can be
# MAGIC embedded and retrieved independently. Fetching is distributed across the
# MAGIC cluster via `mapInPandas`; any URL that fails to fetch/extract (paywall,
# MAGIC timeout, dead link) is skipped rather than failing the whole job.

# COMMAND ----------

content_df = news_df.select("id", "ticker", "article_url").filter(
    "article_url IS NOT NULL AND article_url != ''"
)

chunks_schema = StructType(
    [
        StructField("article_id", StringType(), False),
        StructField("ticker", StringType(), False),
        StructField("chunk_index", StringType(), False),
        StructField("chunk_text", StringType(), False),
    ]
)


def fetch_and_chunk_partitions(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Runs once per Spark partition/task: fetch each article's HTML, extract
    the main body text with trafilatura, then split it into overlapping
    chunks of CHUNK_SIZE characters (CHUNK_OVERLAP characters shared between
    consecutive chunks so context isn't lost at chunk boundaries)."""
    import requests
    import trafilatura

    for batch in iterator:
        out_article_ids, out_tickers, out_chunk_indexes, out_chunk_texts = [], [], [], []
        for article_id, ticker, article_url in zip(
            batch["id"], batch["ticker"], batch["article_url"]
        ):
            try:
                resp = requests.get(article_url, timeout=15)
                resp.raise_for_status()
                text = trafilatura.extract(resp.text)
            except Exception:
                # Dead link, paywall, timeout, etc. - skip this article's
                # content chunks rather than failing the whole job.
                continue

            if not text:
                continue

            for chunk_index, start in enumerate(range(0, len(text), CHUNK_SIZE - CHUNK_OVERLAP)):
                chunk_text = text[start : start + CHUNK_SIZE].strip()
                if not chunk_text:
                    continue
                out_article_ids.append(article_id)
                out_tickers.append(ticker)
                out_chunk_indexes.append(str(chunk_index))
                out_chunk_texts.append(chunk_text)
                if start + CHUNK_SIZE >= len(text):
                    break

        yield pd.DataFrame(
            {
                "article_id": out_article_ids,
                "ticker": out_tickers,
                "chunk_index": out_chunk_indexes,
                "chunk_text": out_chunk_texts,
            }
        )


chunks_df = content_df.mapInPandas(fetch_and_chunk_partitions, schema=chunks_schema)

print(f"Extracted {chunks_df.count()} content chunks from {content_df.count()} article URLs")
display(chunks_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute chunk embeddings
# MAGIC
# MAGIC Same approach as the title/description embeddings above, but one vector
# MAGIC per content chunk instead of per article.

# COMMAND ----------

chunk_embeddings_schema = StructType(
    [
        StructField("article_id", StringType(), False),
        StructField("ticker", StringType(), False),
        StructField("chunk_index", StringType(), False),
        StructField("chunk_text", StringType(), False),
        StructField("embedding", ArrayType(FloatType()), False),
    ]
)


def embed_chunk_partitions(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Runs once per Spark partition: load the model once, then embed
    every batch of chunks handed to this partition."""
    import os
    from sentence_transformers import SentenceTransformer

    # Set cache directory to writable location on Serverless workers
    cache_dir = '/tmp/sentence_transformers_cache'
    os.makedirs(cache_dir, exist_ok=True)
    os.environ['HF_HOME'] = cache_dir
    os.environ['TRANSFORMERS_CACHE'] = cache_dir
    os.environ['SENTENCE_TRANSFORMERS_HOME'] = cache_dir
    
    model = SentenceTransformer(EMBEDDING_MODEL_NAME, cache_folder=cache_dir)

    for batch in iterator:
        vectors = model.encode(batch["chunk_text"].tolist(), show_progress_bar=False)
        yield pd.DataFrame(
            {
                "article_id": batch["article_id"],
                "ticker": batch["ticker"],
                "chunk_index": batch["chunk_index"],
                "chunk_text": batch["chunk_text"],
                "embedding": [v.tolist() for v in vectors],
            }
        )


chunk_embeddings_df = chunks_df.mapInPandas(embed_chunk_partitions, schema=chunk_embeddings_schema)

print(f"Computed {chunk_embeddings_df.count()} chunk embeddings using {EMBEDDING_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the chunk embeddings destination table exists

# COMMAND ----------

# Chunk embeddings will be written to Delta table with the correct dimension
print(f"Embedding dimension: {EMBEDDING_DIM}")
print(f"Target Delta table: {CHUNK_EMBEDDINGS_TABLE_NAME}")
print("\nTable will be created automatically on first write.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write chunk embeddings to Delta table

# COMMAND ----------

# Add id (article_id_chunk_index), model_name, and embedded_at columns
chunk_embeddings_with_meta = (
    chunk_embeddings_df.withColumn(
        "id", expr("concat(article_id, '_', chunk_index)")
    )
    .withColumn("model_name", lit(EMBEDDING_MODEL_NAME))
    .withColumn("embedded_at", current_timestamp())
    .withColumn("chunk_index", col("chunk_index").cast("int"))
)

# Convert embedding array from ArrayType(FloatType) to ArrayType(DoubleType)
# Postgres JDBC expects DOUBLE PRECISION[] for vector columns
chunk_embeddings_final = chunk_embeddings_with_meta.withColumn(
    "embedding", expr("transform(embedding, x -> cast(x as double))")
)

# Read existing chunk embedding IDs to avoid duplicates (deduplication strategy)
if spark.catalog.tableExists(CHUNK_EMBEDDINGS_TABLE_NAME):
    existing_ids = spark.table(CHUNK_EMBEDDINGS_TABLE_NAME).select("id").distinct()
    # Filter out existing IDs (left anti-join keeps only new records)
    new_chunk_embeddings = chunk_embeddings_final.join(existing_ids, on="id", how="left_anti")
else:
    # Table doesn't exist yet - write all chunk embeddings
    new_chunk_embeddings = chunk_embeddings_final

chunk_count = new_chunk_embeddings.count()
if chunk_count > 0:
    # Write to Delta table - embeddings are stored as ARRAY<DOUBLE>
    new_chunk_embeddings.write.format("delta").mode("append").saveAsTable(CHUNK_EMBEDDINGS_TABLE_NAME)
    print(f"✅ Delta: Wrote {chunk_count} new chunk embeddings to {CHUNK_EMBEDDINGS_TABLE_NAME}")
    
    # Also sync to Lakebase with pgvector format
    import psycopg2
    from psycopg2.extras import execute_values
    
    chunks_to_sync = new_chunk_embeddings.collect()
    conn = psycopg2.connect(lakebase_connection_string)
    cur = conn.cursor()
    
    rows = [
        (
            row.id,
            row.article_id,
            row.ticker,
            row.chunk_index,
            row.chunk_text,
            str(row.embedding),  # Will be cast to vector
            row.model_name,
        )
        for row in chunks_to_sync
    ]
    
    execute_values(
        cur,
        f"""INSERT INTO {dbutils.widgets.get('chunk_embeddings_table_name')}
            (id, article_id, ticker, chunk_index, chunk_text, embedding, model_name, embedded_at)
            VALUES %s""",
        rows,
        template="(%s, %s, %s, %s, %s, %s::vector, %s, NOW())"
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Lakebase: Wrote {chunk_count} new chunk embeddings to {dbutils.widgets.get('chunk_embeddings_table_name')}")
else:
    print("No new chunk embeddings to write (all already exist).")