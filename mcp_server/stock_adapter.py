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

    Returns both the individual daily price records and
    calculated summary metrics for the requested period.

    Args:
        ticker: Stock ticker symbol, such as "AAPL" or "MSFT".
        days: Number of calendar days to retrieve.

    Returns:
        Dictionary containing:
        - ticker
        - date range
        - price summary
        - daily price records
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

    summary = None

    if records:
        first_close = records[0]["close_price"]
        last_close = records[-1]["close_price"]

        highest_close = max(
            record["close_price"]
            for record in records
        )

        lowest_close = min(
            record["close_price"]
            for record in records
        )

        price_change = last_close - first_close

        if first_close != 0:
            price_change_percent = (
                price_change / first_close
            ) * 100
        else:
            price_change_percent = None

        summary = {
            "start_date": records[0]["trade_date"],
            "end_date": records[-1]["trade_date"],
            "start_close": first_close,
            "end_close": last_close,
            "change": round(price_change, 2),
            "change_percent": round(
                price_change_percent, 2
            ) if price_change_percent is not None else None,
            "highest_close": highest_close,
            "lowest_close": lowest_close,
        }

    return {
        "ticker": ticker,
        "from_date": str(from_date),
        "to_date": str(to_date),
        "record_count": len(records),
        "summary": summary,
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
    published_after: str | None = None,
    published_before: str | None = None,
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

def get_watchlist(email: str) -> dict:
    """
    Get the stock watchlist for a user from Lakebase.

    Args:
        email: User email associated with the watchlist.

    Returns:
        Dictionary containing the user's watchlist.
    """

    if not email or not email.strip():
        raise ValueError("email is required")

    sql = """
    SELECT
        symbol,
        email,
        latest_price,
        updated_at
    FROM watchlist
    WHERE email = %s
    ORDER BY symbol;
    """

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (email.strip(),))

            columns = [
                desc[0]
                for desc in cur.description
            ]

            rows = [
                dict(zip(columns, row))
                for row in cur.fetchall()
            ]

        return {
            "email": email.strip(),
            "count": len(rows),
            "watchlist": rows,
        }

    finally:
        conn.close()

def add_to_watchlist(
    symbol: str,
    email: str,
    latest_price: float | None = None,
) -> dict:
    """
    Add a stock to a user's watchlist.

    Args:
        symbol: Stock ticker symbol.
        email: User email.
        latest_price: Optional current stock price.

    Returns:
        Details of the added watchlist item.
    """

    symbol = symbol.strip().upper()
    email = email.strip()

    if not symbol:
        raise ValueError("symbol is required")

    if not email:
        raise ValueError("email is required")

    sql = """
    INSERT INTO watchlist (
        symbol,
        email,
        latest_price,
        updated_at
    )
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT DO NOTHING
    RETURNING
        symbol,
        email,
        latest_price,
        updated_at;
    """

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    symbol,
                    email,
                    latest_price,
                ),
            )

            row = cur.fetchone()

            if row is None:
                conn.commit()

                return {
                    "status": "exists",
                    "symbol": symbol,
                    "email": email,
                    "message": (
                        f"{symbol} is already in the watchlist."
                    ),
                }

            columns = [
                desc[0]
                for desc in cur.description
            ]

            result = dict(zip(columns, row))

            conn.commit()

        return {
            "status": "success",
            "message": f"{symbol} added to watchlist.",
            "watchlist_item": result,
        }

    finally:
        conn.close()

def remove_from_watchlist(
    symbol: str,
    email: str,
) -> dict:
    """
    Remove a stock from a user's watchlist.

    Args:
        symbol: Stock ticker symbol.
        email: User email.

    Returns:
        Result of the deletion.
    """

    symbol = symbol.strip().upper()
    email = email.strip()

    if not symbol:
        raise ValueError("symbol is required")

    if not email:
        raise ValueError("email is required")

    sql = """
    DELETE FROM watchlist
    WHERE symbol = %s
      AND email = %s
    RETURNING symbol;
    """

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    symbol,
                    email,
                ),
            )

            row = cur.fetchone()

            conn.commit()

        if row is None:
            return {
                "status": "not_found",
                "symbol": symbol,
                "email": email,
                "message": (
                    f"{symbol} was not found in the watchlist."
                ),
            }

        return {
            "status": "success",
            "symbol": symbol,
            "email": email,
            "message": (
                f"{symbol} removed from watchlist."
            ),
        }

    finally:
        conn.close()

def save_research_note(
    title: str,
    note: str,
    email: str,
    symbol: str | None = None,
) -> dict:
    """
    Save a research note for a user.

    Args:
        title: Title of the research note.
        note: Research note content.
        email: User email.
        symbol: Optional stock ticker symbol.

    Returns:
        Saved research note details.
    """

    title = title.strip()
    note = note.strip()
    email = email.strip()

    if not title:
        raise ValueError("title is required")

    if not note:
        raise ValueError("note is required")

    if not email:
        raise ValueError("email is required")

    if symbol:
        symbol = symbol.strip().upper()

    sql = """
    INSERT INTO research_notes (
        email,
        symbol,
        title,
        note
    )
    VALUES (%s, %s, %s, %s)
    RETURNING
        id,
        email,
        symbol,
        title,
        note,
        created_at;
    """

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    email,
                    symbol,
                    title,
                    note,
                ),
            )

            row = cur.fetchone()

            columns = [
                desc[0]
                for desc in cur.description
            ]

            result = dict(zip(columns, row))

            conn.commit()

        return {
            "status": "success",
            "message": "Research note saved successfully.",
            "research_note": result,
        }

    finally:
        conn.close()

def save_analysis_report(
    title: str,
    analysis: str,
    email: str,
    symbol: str | None = None,
) -> dict:
    """
    Save a generated stock analysis report for a user.

    Args:
        title: Report title.
        analysis: Full analysis content.
        email: User email.
        symbol: Optional stock ticker.

    Returns:
        Saved analysis report details.
    """

    title = title.strip()
    analysis = analysis.strip()
    email = email.strip()

    if not title:
        raise ValueError("title is required")

    if not analysis:
        raise ValueError("analysis is required")

    if not email:
        raise ValueError("email is required")

    if symbol:
        symbol = symbol.strip().upper()

    sql = """
    INSERT INTO analysis_reports (
        email,
        symbol,
        title,
        analysis
    )
    VALUES (%s, %s, %s, %s)
    RETURNING
        id,
        email,
        symbol,
        title,
        analysis,
        created_at;
    """

    conn = psycopg2.connect(lakebase_connection_string)

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    email,
                    symbol,
                    title,
                    analysis,
                ),
            )

            row = cur.fetchone()

            columns = [
                desc[0]
                for desc in cur.description
            ]

            result = dict(zip(columns, row))

            conn.commit()

        return {
            "status": "success",
            "message": "Analysis report saved successfully.",
            "analysis_report": result,
        }

    finally:
        conn.close()                                