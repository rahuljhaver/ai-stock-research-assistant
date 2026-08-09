"""
Stock Research MCP server.

Exposes stock research tools over MCP so a Databricks Agent Bricks
agent can call market-data tools.

Tools:
- get_stock_price(ticker, days)
- get_latest_stock_price(ticker)
- get_current_user()

The stock tools are backed by the Massive API through stock_adapter.py.
"""

import os
import logging
from contextvars import ContextVar

from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from stock_adapter import (
    get_stock_price as fetch_stock_price,
    get_latest_stock_price as fetch_latest_stock_price,
)


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Context variable to store request headers for end-user identity
_request_context: ContextVar[dict] = ContextVar(
    "request_context",
    default={}
)


# Create MCP server
mcp = FastMCP("stock-research-mcp")


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Capture HTTP headers containing end-user identity."""

    async def dispatch(self, request: Request, call_next):
        headers = {
            "x-forwarded-user": request.headers.get("x-forwarded-user"),
            "x-forwarded-email": request.headers.get("x-forwarded-email"),
        }

        _request_context.set(headers)

        response = await call_next(request)

        return response


@mcp.tool()
def get_stock_price(ticker: str, days: int = 30):
    """
    Get historical daily stock prices for a ticker.

    Args:
        ticker: Stock ticker symbol, such as "AAPL" or "MSFT".
        days: Number of calendar days to retrieve, from 1 to 365.

    Returns:
        Historical stock price records including open, high, low,
        close, volume, and VWAP.
    """
    try:
        if days < 1:
            days = 1

        if days > 365:
            days = 365

        return fetch_stock_price(
            ticker=ticker,
            days=days,
        )

    except Exception as e:
        logger.exception("Failed to retrieve stock price")

        return {
            "status": "error",
            "message": str(e),
        }


@mcp.tool()
def get_latest_stock_price(ticker: str):
    """
    Get the latest available stock price for a ticker.

    Args:
        ticker: Stock ticker symbol, such as "AAPL" or "MSFT".

    Returns:
        Latest available open, high, low, close, volume, and VWAP data.
    """
    try:
        return fetch_latest_stock_price(ticker)

    except Exception as e:
        logger.exception("Failed to retrieve latest stock price")

        return {
            "status": "error",
            "message": str(e),
        }


@mcp.tool()
def get_current_user() -> dict:
    """
    Get information about the currently authenticated end user
    accessing the MCP server.

    When running as a Databricks App, this uses the X-Forwarded-User
    header injected by Databricks.
    """

    try:
        headers = _request_context.get()

        forwarded_user = headers.get("x-forwarded-user")
        forwarded_email = headers.get("x-forwarded-email")

        if forwarded_user:
            return {
                "tool_name": "get_current_user",
                "status": "success",
                "message": (
                    f"Current user identified from request headers: "
                    f"{forwarded_user}"
                ),
                "data": {
                    "user_name": forwarded_user,
                    "forwarded_email": forwarded_email,
                    "source": "request_header",
                },
            }

        # Fallback for local/non-App execution
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        user = w.current_user.me()

        return {
            "tool_name": "get_current_user",
            "status": "success",
            "message": (
                f"Service principal identified: {user.user_name}"
            ),
            "data": {
                "user_name": user.user_name,
                "display_name": user.display_name,
                "active": user.active,
                "source": "service_principal",
            },
        }

    except Exception as e:
        logger.exception("Failed to get current user")

        return {
            "tool_name": "get_current_user",
            "status": "error",
            "message": f"Failed to get current user: {str(e)}",
            "data": None,
        }


if __name__ == "__main__":
    # Skip server startup in Databricks interactive environments.
    # This file is intended to be deployed as a Databricks App.

    import sys

    if (
        "databricks" in sys.modules
        or os.getenv("DATABRICKS_RUNTIME_VERSION")
    ):
        print(
            "This file is designed to be deployed as a Databricks App."
        )
        print("Use the app.yaml configuration to deploy the server.")

    else:
        # Add middleware before starting the MCP server.
        if hasattr(mcp, "app") and mcp.app is not None:
            mcp.app.add_middleware(RequestContextMiddleware)

        port = int(
            os.getenv(
                "DATABRICKS_APP_PORT",
                os.getenv("PORT", 8000),
            )
        )

        mcp.run(
            transport="http",
            host="0.0.0.0",
            port=port,
        )