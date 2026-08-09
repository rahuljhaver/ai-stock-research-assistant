# Stock Research AI Agent

A Databricks-native stock research application with **two independent entry points** that share one Lakebase Postgres database:

1. **Custom Web UI** — a Flask dashboard for watchlist management, price charts, and semantic news search.
2. **Agent Bricks / Playground** — a Databricks AI Agent that talks to a separate MCP server for conversational stock research.

> **The Web UI does not use MCP or the Databricks Agent.** It talks directly to Lakebase and the Massive API.
> **Agent Bricks/Playground uses MCP exclusively.** It never calls the Flask app.
>
> The two paths are separate applications. The only thing that connects them is that they read and write overlapping tables in the same Lakebase database.

## Architecture

```
                     Lakebase Postgres (shared)
                     watchlist · news + embeddings · price history · notes/reports
                              ▲                        ▲
                 direct SQL  │                          │  direct SQL
                              │                          │
        ┌─────────────────────┐              ┌─────────────────────┐
        │      Web UI Path      │              │      Agent Path       │
        │                       │              │                       │
        │  Browser              │              │  Agent Bricks /       │
        │    ↓                  │              │  Playground           │
        │  Flask app (app.py)   │              │    ↓ MCP              │
        │    ↓                  │              │  Databricks Agent     │
        │  Massive API           │              │    ↓ MCP              │
        └─────────────────────┘              │  MCP Server           │
                                                │    ↓                  │
                                                │  Massive API           │
                                                └─────────────────────┘

        No connection between the two paths — they meet only in Lakebase.
```

## Capability Matrix

| Capability | Web UI | Agent (via MCP) |
|---|:---:|:---:|
| View watchlist | ✅ | ✅ |
| Add / remove watchlist symbol | ✅ | ✅ |
| Latest price lookup | ✅ | ✅ |
| Historical price chart / data | ✅ (from Lakebase `stock_price_history`) | ✅ (live from Massive API) |
| Semantic news search | ✅ | ✅ |
| Save research notes | ❌ | ✅ |
| Save analysis reports | ❌ | ✅ |
| Identify current user | ✅ (`X-Forwarded-Email` header) | ✅ (`get_current_user` tool) |

Watchlist and news-search data are shared — an item added in one path is visible in the other. Research notes and analysis reports exist **only** on the Agent path; the Web UI has no equivalent feature.

## 1. Overview

Lets a user — via a web dashboard, or conversationally via a Databricks Agent — track a stock watchlist, view price history, and semantically search financial news to understand what may be driving price movement.

**Problem it solves:** consolidates price data, news, and research notes that would otherwise live in separate tools, and adds semantic search so a user can ask research questions in natural language instead of skimming headlines.

**Core technologies:** Flask, Databricks Apps, Databricks-managed Postgres (Lakebase) with `pgvector`, FastMCP, `sentence-transformers` (`all-MiniLM-L6-v2`), Databricks Asset Bundles, Databricks Workflows/Spark, and the Massive stock/news API (internally Polygon.io — see [Notes](#notes)).

## 2. Web UI

Flask app (`app.py`) serving `templates/index.html`. Talks directly to Lakebase (`lakebase.py`) and to Massive (`massive_client.py`) — no MCP, no agent.

**Routes the UI actually calls:**
- `GET /watchlist`, `POST /watchlist`, `DELETE /watchlist/<symbol>`
- `GET /stocks/<ticker>/history` — reads `stock_price_history` from Lakebase to draw the price chart
- `POST /news/search` — embeds the query in-process, searches `ticker_news_chunk_embeddings` via pgvector

A few other routes exist in `app.py` (`/records`, `/sync`, `/news/sync`, `/stocks/<symbol>/massive-history`) but aren't called by the UI — they're for manual/API use. `templates/index_old.html` is also present but unused.

## 3. Agent / MCP

The Databricks Agent (configured in Agent Bricks/Playground, outside this repo) discovers and calls 9 tools exposed by the MCP server at `mcp_server/stock_mcp_server.py` (FastMCP, HTTP transport). Tool logic lives in `mcp_server/stock_adapter.py`, which opens its own Lakebase connections and uses its own copy of `massive_client.py` — it does not share code with the Web UI.

| Tool | Purpose | Backend |
|---|---|---|
| `get_stock_price` | Historical daily OHLCV + summary stats | Massive API (live) |
| `get_latest_stock_price` | Latest OHLCV for a ticker | Massive API |
| `get_watchlist` / `add_to_watchlist` / `remove_from_watchlist` | Manage watchlist | Lakebase `watchlist` |
| `save_research_note` | Persist a research note | Lakebase `research_notes` |
| `save_analysis_report` | Persist a completed analysis | Lakebase `analysis_reports` |
| `search_stock_research` | Semantic news search | Lakebase `ticker_news_chunk_embeddings` via pgvector |
| `get_current_user` | Identify the calling user | `x-forwarded-user`/`x-forwarded-email` headers, or service-principal fallback |

Each tool wraps its body in `try/except` and returns `{"status": "error", ...}` on failure rather than raising.

## 4. RAG / News Pipeline

A scheduled Databricks Workflow job runs `notebooks/ingest_ticker_news_embeddings.py` (currently **paused** by default): reads tickers from `watchlist` → fetches news per ticker from Massive (rate-limited) → stores raw articles in `ticker_news_documents` → embeds title/description into `ticker_news_embeddings` → scrapes full article bodies (`trafilatura`), chunks them (800 chars / 100 overlap), and embeds chunks into `ticker_news_chunk_embeddings`. Embeddings are written via Spark JDBC as plain arrays and must be cast to pgvector's `VECTOR` type afterward (see `sql/README.md`).

**Both entry points query `ticker_news_chunk_embeddings` directly** — that's the table actually powering search in both the Web UI and the `search_stock_research` tool. `ticker_news_embeddings` (title/description-level) is written but not read by either path.

## 5. Lakebase (Shared Database)

One Postgres instance, six tables, accessed independently by each path (the MCP server does not import `lakebase.py`; it opens its own connections).

| Table | Web UI | Agent/MCP |
|---|:---:|:---:|
| `watchlist` | ✅ | ✅ |
| `ticker_news_documents` | ✅ (read) | ✅ (read) |
| `ticker_news_chunk_embeddings` | ✅ (read) | ✅ (read) |
| `ticker_news_embeddings` | — | — |
| `stock_price_history` | ✅ | — |
| `research_notes` | — | ✅ |
| `analysis_reports` | — | ✅ |

`sql/` contains setup scripts for `watchlist`, `ticker_news_documents`, `ticker_news_embeddings`, `ticker_news_chunk_embeddings`, `research_notes`, and `analysis_reports`. **`stock_price_history` has no setup script in this repo** — see Known Limitations.

## 6. Deployment

| Component | Mechanism | Bundle-managed? |
|---|---|---|
| Web UI (Flask) | Databricks App (`app.yaml`) | Yes — `resources/lakebase_app.yml` |
| MCP server | Databricks App (`mcp_server/app.yaml`) | **No** — deploy manually |
| News ingestion | Databricks Workflow job | Yes — `resources/ingest_ticker_news_embeddings_job.yml` (schedule paused by default) |
| Price history ingestion | Notebook (`notebooks/ingest_stock_price_history.ipynb`) | **No** — run manually |

Deploy with `databricks bundle deploy -t dev` (or `-t prod`); both targets in `databricks.yml` point at the same workspace host.

## 7. Setup

1. Run `setup_secrets.py` to store the Lakebase URL (secret scope `database`/`lakebase-url`) and Massive API key (scope `massive`/`api-key`, commented out by default).
2. Run `sql/00`–`sql/03` (replace `{{EMBEDDING_DIM}}` with `384`) and `sql/05`. Create `stock_price_history` manually — no script ships for it.
3. Deploy the Web UI: `databricks bundle deploy -t dev`. Locally: `pip install -r requirements.txt && python app.py`.
4. Deploy the MCP server manually (not bundle-wired): `pip install -r mcp_server/requirements.txt && python mcp_server/stock_mcp_server.py`, or as its own Databricks App using `mcp_server/app.yaml`.
5. Point Agent Bricks/Playground at the MCP server's URL.
6. Run `databricks bundle run ingest_ticker_news_embeddings_job -t dev` once, then run the vector-cast SQL in `sql/README.md`. Unpause the schedule if you want daily runs.
7. Run `notebooks/ingest_stock_price_history.ipynb` manually to populate the Web UI's price chart data.

## 8. Known Limitations

- **`DEMO_USER_EMAIL` is referenced but never defined** in `mcp_server/stock_mcp_server.py` (5 tools fall back to it). Calling a tool without an explicit `email` and without a forwarded-email header raises `NameError`, caught and returned as a generic error.
- **`stock_price_history` has no setup script and no scheduled ingestion job** — the Web UI's price chart depends on a table nothing in this repo formally provisions.
- **The MCP server has no bundle resource** — unlike the Flask app, it isn't declared in `databricks.yml`/`resources/`.
- **`search_stock_research`'s date filters are accepted but unused** — `published_after`/`published_before` never reach the SQL.
- **Two independent embedding-model instances** — the Web UI and MCP server each load their own copy of `sentence-transformers/all-MiniLM-L6-v2`; no shared embedding service.
- **`ticker_news_embeddings` is written but never queried** by either path.

## 9. Future Enhancements

- Add a setup script and scheduled job for `stock_price_history`.
- Add a bundle resource for the MCP server.
- Apply the date filters in `search_stock_research`.
- Define (or remove) the `DEMO_USER_EMAIL` fallback.
- Move to a shared embedding endpoint instead of two in-process copies.

## 10. Project Status

**Working end-to-end:** Web UI (watchlist, price chart, news search), MCP server (all 9 tools), RAG pipeline (ingestion → chunking → embedding → pgvector search), and the shared Lakebase data model connecting both paths.

**Not fully wired:** MCP deployment automation, price-history ingestion automation, date filtering in semantic search, `DEMO_USER_EMAIL`.

## 11. Repository Structure

```
ai-stock-research-assistant-main/
├── app.py, lakebase.py, massive_client.py    # Web UI path
├── templates/index.html                       # Web UI (index_old.html is unused)
├── app.yaml, requirements.txt
├── mcp_server/                                 # Agent path
│   ├── stock_mcp_server.py, stock_adapter.py, massive_client.py
│   └── app.yaml, requirements.txt
├── notebooks/
│   ├── ingest_ticker_news_embeddings.py        # scheduled
│   └── ingest_stock_price_history.ipynb        # manual only
├── sql/                                        # table DDL + README
├── resources/                                  # bundle resources (Web UI + news job only)
├── databricks.yml
└── setup_secrets.py
```

## Notes

- The provider is referred to as "Massive API" throughout, but the actual endpoints are Polygon.io's — a naming artifact.
- Lakebase auth uses a single static `LAKEBASE_URL` secret (no token refresh needed).
