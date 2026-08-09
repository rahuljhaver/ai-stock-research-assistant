from datetime import date, timedelta

from massive_client import MassiveClient

import base64
import psycopg2
from sentence_transformers import SentenceTransformer
from databricks.sdk import WorkspaceClient


# Create one reusable API client.
client = MassiveClient()

w = WorkspaceClient()

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def get_lakebase_url() -> str:
    secret = w.secrets.get_secret(
        scope="database",
        key="lakebase-url"
    )
    return base64.b64decode(secret.value).decode("utf-8")


lakebase_connection_string = get_lakebase_url()

embedding_model = SentenceTransformer(
    EMBEDDING_MODEL_NAME
)


def get_stock_price(
    ticker: str,
    days: int = 30
):
    """
    Get daily historical stock prices for a ticker.

    Args:
        ticker: Stock ticker symbol, such as "AAPL".
        days: Number of calendar days to retrieve.

    Returns:
        Dictionary containing ticker, date range, and price records.
    """

    ticker = ticker.strip().upper()

    if not ticker:
        raise ValueError("Ticker is required.")

    if days < 1:
        days = 1

    if days > 365:
        days = 365

    to_date = date.today()
    from_date = to_date - timedelta(days=days)

    prices = client.get_historical_prices(
        symbol=ticker,
        from_date=str(from_date),
        to_date=str(to_date),
    )

    records = []

    for record in prices:
        records.append({
            "ticker": ticker,
            "trade_date": date.fromtimestamp(
                record["t"] / 1000
            ).isoformat(),
            "open_price": record["o"],
            "high_price": record["h"],
            "low_price": record["l"],
            "close_price": record["c"],
            "volume": record["v"],
            "vwap": record.get("vw"),
        })

    return {
        "ticker": ticker,
        "from_date": str(from_date),
        "to_date": str(to_date),
        "record_count": len(records),
        "prices": records,
    }


def get_latest_stock_price(ticker: str):
    """
    Get the latest available stock price for a ticker.
    """

    ticker = ticker.strip().upper()

    if not ticker:
        raise ValueError("Ticker is required.")

    data = client.get_latest_price(ticker)

    results = data.get("results", [])

    if not results:
        return {
            "ticker": ticker,
            "status": "error",
            "message": "No price data returned."
        }

    record = results[0]

    return {
        "ticker": ticker,
        "open_price": record.get("o"),
        "high_price": record.get("h"),
        "low_price": record.get("l"),
        "close_price": record.get("c"),
        "volume": record.get("v"),
        "vwap": record.get("vw"),
        "timestamp": record.get("t"),
    }

def search_stock_research_data(
    query: str,
    ticker: str | None = None,
    top_k: int = 5,
) -> dict:
    """
    Perform semantic search over stock news stored in Lakebase.

    The query is converted to an embedding using the same
    sentence-transformers model used during ingestion.
    """

    if not query or not query.strip():
        raise ValueError("query is required")

    top_k = max(1, min(top_k, 10))

    query_vector = embedding_model.encode(
        query
    ).tolist()

    if ticker:
        sql = """
        SELECT
            a.article_id,
            b.ticker,
            b.title,
            b.article_url,
            b.published_utc,
            a.chunk_text,
            embedding <=> %s::vector AS distance
        FROM ticker_news_chunk_embeddings a
        LEFT JOIN ticker_news_documents b
            ON a.article_id = b.id
        WHERE b.ticker = %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
        """

        params = (
            query_vector,
            ticker.upper(),
            query_vector,
            top_k,
        )

    else:
        sql = """
        SELECT
            a.article_id,
            b.ticker,
            b.title,
            b.article_url,
            b.published_utc,
            a.chunk_text,
            embedding <=> %s::vector AS distance
        FROM ticker_news_chunk_embeddings a
        LEFT JOIN ticker_news_documents b
            ON a.article_id = b.id
        ORDER BY embedding <=> %s::vector
        LIMIT %s;
        """

        params = (
            query_vector,
            query_vector,
            top_k,
        )

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)

            columns = [
                desc[0]
                for desc in cur.description
            ]

            rows = [
                dict(zip(columns, row))
                for row in cur.fetchall()
            ]

        return {
            "query": query,
            "ticker": ticker,
            "matches": rows,
        }

    finally:
        conn.close()
