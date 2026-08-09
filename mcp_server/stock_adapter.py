from datetime import date, timedelta

from massive_client import MassiveClient


# Create one reusable API client.
client = MassiveClient()


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
