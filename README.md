# Stock Research AI Agent

An intelligent stock research assistant powered by Databricks Agent Framework, MCP (Model Context Protocol), and Retrieval-Augmented Generation (RAG) over financial news data.

## Overview

The Stock Research AI Agent enables natural-language stock research through a conversational interface. Users can query real-time stock prices, search semantically through financial news, maintain personalized watchlists, and save research insights—all through simple conversational prompts.

### What Problem Does It Solve?

* **Information Overload**: Filters vast amounts of financial news to surface only what's relevant to your research questions
* **Time-Consuming Research**: Automates the discovery of price catalysts by correlating news with price movements
* **Fragmented Data**: Unifies stock prices, news articles, and user research in one conversational interface
* **Context Loss**: Maintains research history and watchlists across sessions

## Architecture

The Stock Research AI Agent follows a multi-layered architecture that separates concerns between the conversational interface, tool execution, data retrieval, and storage:

```
User Query
    ↓
Databricks Agent (Genie)
    ↓
MCP Server (Tool Router)
    ↓
    ├─→ Stock Price API (Polygon.io via Massive Client)
    ├─→ Lakebase Postgres
    │   ├─→ Watchlist (user preferences)
    │   ├─→ Research Notes (saved insights)
    │   ├─→ Analysis Reports (completed research)
    │   └─→ News Documents + Embeddings (RAG corpus)
    └─→ pgvector Semantic Search
    ↓
Agent Response
```

### Components

#### 1. Databricks Agent / Genie
* **Role**: Conversational orchestrator
* **Capabilities**: Understands user intent, invokes MCP tools, synthesizes responses
* **Technology**: Databricks Agent Framework with foundation model (e.g., DBRX, Llama)

#### 2. MCP Server
* **Role**: Tool execution layer
* **Location**: `mcp_server/stock_mcp_server.py`
* **Technology**: FastMCP (Model Context Protocol implementation)
* **Deployment**: Databricks App with HTTP transport

#### 3. Stock Market Data API
* **Provider**: Polygon.io (accessed via `massive_client.py`)
* **Endpoints**:
  * `/v2/aggs/ticker/{symbol}/prev` - Latest price
  * `/v2/aggs/ticker/{symbol}/range/1/day/{from}/{to}` - Historical prices
  * `/v2/reference/news` - News articles by ticker
* **Authentication**: API key stored in Databricks secrets (`massive/api-key`)

#### 4. Lakebase / Postgres
* **Role**: Persistent storage for structured data and vector embeddings
* **Technology**: Databricks-managed Postgres with pgvector extension
* **Connection**: Native Postgres role with static password (stored in `database/lakebase-url` secret)
* **Tables**:
  * `watchlist` - User stock preferences
  * `ticker_news_documents` - Raw news articles
  * `ticker_news_embeddings` - Title/description embeddings (384-dim)
  * `ticker_news_chunk_embeddings` - Article body chunk embeddings (384-dim)
  * `research_notes` - User-saved notes
  * `analysis_reports` - Completed analysis documents

#### 5. pgvector / Vector Search
* **Role**: Semantic similarity search over news embeddings
* **Technology**: PostgreSQL pgvector extension with HNSW indexes
* **Model**: `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)
* **Distance Metric**: Cosine similarity (`<=>` operator)

### End-to-End Flow

#### Example: "What news could be driving AMD's price movement?"

1. **User** types question in Databricks Agent interface
2. **Agent** parses intent: needs AMD price history + relevant news
3. **Agent** calls MCP tool `get_stock_price(ticker="AMD", days=30)`
4. **MCP Server** → `stock_adapter.py` → `massive_client.py` → Polygon.io API
5. **Response** returns 30 days of OHLCV data
6. **Agent** calls MCP tool `search_stock_research(query="AMD price catalyst", ticker="AMD", top_k=5)`
7. **MCP Server** → `stock_adapter.py`:
   * Encodes query using `sentence-transformers/all-MiniLM-L6-v2`
   * Executes pgvector query against `ticker_news_chunk_embeddings`
   * Returns top 5 semantically relevant article chunks
8. **Agent** synthesizes response correlating price movements with news events
9. **User** sees analysis in chat

## MCP Tools

The MCP server (`mcp_server/stock_mcp_server.py`) exposes the following tools to the Databricks Agent:

### Stock Price Tools

#### `get_stock_price`
**Purpose**: Retrieve historical daily stock prices

**Inputs**:
* `ticker` (string, required): Stock symbol (e.g., "AAPL", "MSFT")
* `days` (int, optional): Number of calendar days to retrieve (default: 30, range: 1-365)

**Returns**:
```json
{
  "ticker": "AMD",
  "from_date": "2024-01-01",
  "to_date": "2024-01-31",
  "record_count": 21,
  "summary": {
    "start_date": "2024-01-02",
    "end_date": "2024-01-31",
    "start_close": 145.23,
    "end_close": 152.67,
    "change": 7.44,
    "change_percent": 5.12,
    "highest_close": 155.89,
    "lowest_close": 143.10
  },
  "prices": [
    {
      "ticker": "AMD",
      "trade_date": "2024-01-02",
      "open_price": 144.50,
      "high_price": 146.78,
      "low_price": 143.90,
      "close_price": 145.23,
      "volume": 52389100,
      "vwap": 145.02
    }
    // ... more daily records
  ]
}
```

#### `get_latest_stock_price`
**Purpose**: Retrieve the most recent traded price

**Inputs**:
* `ticker` (string, required): Stock symbol

**Returns**:
```json
{
  "ticker": "AMD",
  "open_price": 152.10,
  "high_price": 153.45,
  "low_price": 151.80,
  "close_price": 152.67,
  "volume": 48920300,
  "vwap": 152.31,
  "timestamp": 1706745600000
}
```

### Research Tools

#### `search_stock_research`
**Purpose**: Semantic search over financial news using pgvector

**Inputs**:
* `query` (string, required): Natural-language research question
* `ticker` (string, optional): Filter results to specific stock symbol
* `published_after` (string, optional): ISO date/time for filtering (e.g., "2024-01-01")
* `published_before` (string, optional): ISO date/time for filtering
* `top_k` (int, optional): Number of results to return (default: 5, range: 1-10)

**Returns**:
```json
{
  "query": "AMD data center GPU competition",
  "ticker": "AMD",
  "matches": [
    {
      "article_id": "abc123",
      "ticker": "AMD",
      "title": "AMD Unveils MI300 AI Accelerator...",
      "article_url": "https://...",
      "published_utc": "2024-01-15T14:30:00Z",
      "chunk_text": "AMD's new MI300X GPU targets NVIDIA's dominance...",
      "distance": 0.23
    }
    // ... more chunks, ordered by semantic similarity
  ]
}
```

### Watchlist Tools

#### `get_watchlist`
**Purpose**: Retrieve user's saved stock symbols

**Inputs**:
* `email` (string, optional): User email (auto-detected from request headers or falls back to configured email)

**Returns**:
```json
{
  "email": "user@example.com",
  "count": 3,
  "watchlist": [
    {
      "symbol": "AMD",
      "email": "user@example.com",
      "latest_price": 152.67,
      "updated_at": "2024-01-31T18:45:00Z"
    },
    {"symbol": "NVDA", "email": "user@example.com", "latest_price": 505.48, "updated_at": "2024-01-31T18:45:00Z"},
    {"symbol": "INTC", "email": "user@example.com", "latest_price": 43.21, "updated_at": "2024-01-31T18:45:00Z"}
  ]
}
```

#### `add_to_watchlist`
**Purpose**: Add a stock to user's watchlist

**Inputs**:
* `symbol` (string, required): Stock ticker to add
* `email` (string, optional): User email (auto-detected)

**Returns**:
```json
{
  "status": "success",
  "message": "AMD added to watchlist.",
  "watchlist_item": {
    "symbol": "AMD",
    "email": "user@example.com",
    "latest_price": null,
    "updated_at": "2024-01-31T19:00:00Z"
  }
}
```

#### `remove_from_watchlist`
**Purpose**: Remove a stock from user's watchlist

**Inputs**:
* `symbol` (string, required): Stock ticker to remove
* `email` (string, optional): User email (auto-detected)

**Returns**:
```json
{
  "status": "success",
  "symbol": "AMD",
  "email": "user@example.com",
  "message": "AMD removed from watchlist."
}
```

### Persistence Tools

#### `save_research_note`
**Purpose**: Save user research notes to Lakebase

**Inputs**:
* `title` (string, required): Note title
* `note` (string, required): Note content
* `symbol` (string, optional): Associated stock ticker
* `email` (string, optional): User email (auto-detected)

**Returns**:
```json
{
  "status": "success",
  "message": "Research note saved successfully.",
  "research_note": {
    "id": 42,
    "email": "user@example.com",
    "symbol": "AMD",
    "title": "Q4 2024 Data Center Analysis",
    "note": "AMD's MI300X ramp appears stronger than expected...",
    "created_at": "2024-01-31T19:15:00Z"
  }
}
```

#### `save_analysis_report`
**Purpose**: Save completed stock analysis

**Inputs**:
* `title` (string, required): Report title
* `analysis` (string, required): Full analysis text
* `symbol` (string, optional): Associated stock ticker
* `email` (string, optional): User email (auto-detected)

**Returns**:
```json
{
  "status": "success",
  "message": "Analysis report saved successfully.",
  "analysis_report": {
    "id": 15,
    "email": "user@example.com",
    "symbol": "AMD",
    "title": "AMD 30-Day Price Catalyst Analysis",
    "analysis": "Over the past 30 days, AMD's stock price increased 5.12%...",
    "created_at": "2024-01-31T19:20:00Z"
  }
}
```

### Utility Tools

#### `get_current_user`
**Purpose**: Retrieve authenticated user information

**Inputs**: None

**Returns**:
```json
{
  "tool_name": "get_current_user",
  "status": "success",
  "message": "Current user identified from request headers: user@example.com",
  "data": {
    "user_name": "user@example.com",
    "forwarded_email": "user@example.com",
    "source": "request_header"
  }
}
```

## Step-by-step setup

### 1. Create a Polygon.io account and get an API key

**Note**: The codebase refers to the stock data API as "Massive API" but it uses Polygon.io endpoints.

1. Go to [https://polygon.io](https://polygon.io) and sign up for a new account
2. Navigate to your **Dashboard** after logging in
3. Find the **API Keys** section
4. Copy your API key (free tier provides 5 API calls/minute)
5. Keep this key handy for step 3 (Store your secrets) below. Do **not** commit it to git or store in plaintext.

**API Endpoints Used**:
* `/v2/aggs/ticker/{symbol}/prev` - Latest price
* `/v2/aggs/ticker/{symbol}/range/1/day/{from}/{to}` - Historical prices
* `/v2/reference/news` - News articles

**Rate Limits**: Free tier is limited to 5 requests/minute. The news ingestion notebook respects this limit.

### 2. Create a Lakebase instance and a native-password role

1. In your Databricks workspace, go to **Catalog** (left sidebar) and select the **Lakebase** tab (or search "Lakebase" in the workspace search bar).
2. Click **Create Lakebase instance** (sometimes labeled **Create database instance**).
   - Give it a name (e.g. `massive-sync-db`).
   - Choose the capacity/compute size and region appropriate for your workload (defaults are fine to start).
   - Click **Create** and wait for the instance to reach the **Available**/**Running** state.
3. Open the newly created instance, then go to the **Roles & Databases** tab (sometimes called **Permissions** or **Roles**).
4. **Enable native (password) authentication** for the instance if it isn't already on:
   - Look for an authentication setting such as **Native passwords** or **Password authentication** and toggle/enable it. By default some Lakebase instances only support OAuth/token-based auth — you need password auth enabled so the role below gets a static password instead of a short-lived token.
5. **Create a new role**:
   - Click **Add role** / **Create role**.
   - Choose **Password** as the authentication method (not OAuth).
   - Name the role (e.g. `massive_app`) and let Databricks generate (or set) a password.
6. **Copy the connection URL** shown for the role. It will look like:

   ```
   postgresql://<role>:<password>@<host>.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require
   ```

   Keep this URL — you'll paste it into `setup_secrets.py`'s prompt in the next step.

### 3. Store your secrets

Run once from a **Databricks notebook** in your workspace (no CLI needed):

1. Create a new notebook (or open the Git folder you'll create in step 5, once it's cloned) and attach it to any running cluster.
2. In a cell, run:

   ```python
   %sh python setup_secrets.py
   ```

   or open a terminal from the notebook (**Run** > **Open terminal**, if enabled on your cluster) and run `python setup_secrets.py` there.

This prompts (via `getpass`, so nothing is echoed or written to disk/shell history) for:
- Your **Massive API key** (from step 1) → stored as secret `massive/api-key`
- Your **Lakebase connection URL** (from step 2) → stored as secret `database/lakebase-url`

### 4. Configure environment variables (local dev)

Copy `.env.example` to `.env` and paste your Lakebase URL as `LAKEBASE_URL` for local runs:

```bash
cp .env.example .env
```

For deployment, `app.yaml` already pulls `LAKEBASE_URL` from the `database/lakebase-url` secret automatically — no manual editing needed there.

### 5. Install dependencies

```bash
pip install -r requirements.txt
```

### 6. Run locally

```bash
python app.py
```

### 7. Create a Git folder in Databricks and deploy the app (no CLI required)

All of this is done through the Databricks workspace UI:

1. **Create a Git folder**:
   - In the Databricks workspace sidebar, click **Workspace** > **Create** > **Git folder** (in older UIs this is called **Repos** > **Add Repo**).
   - Paste the Git URL of this project's repository (e.g. your GitHub/GitLab remote for this codebase).
   - Choose a folder name and click **Create Git folder**. Databricks will clone the repo directly into your workspace — this becomes the source for your app.

2. **Create the Databricks App**:
   - In the sidebar, go to **Compute** > **Apps** (or search "Apps" in the workspace search bar).
   - Click **Create app**, then choose **Custom** (or "From scratch").
   - Give the app a name (e.g. `massive-lakebase-sync`).

3. **Point the app at your Git folder**:
   - When prompted for the source code location, select **Workspace files** / **Git folder** and browse to the Git folder you created in step 1 (the folder containing `app.py` and `app.yaml`).
   - Databricks will read `app.yaml` from that folder automatically to configure the `command` and `env` (including the `LAKEBASE_URL`, `MASSIVE_API_BASE_URL`, and secret scope/key references).

4. **Deploy**:
   - Click **Deploy** (or **Create and deploy**) in the Apps UI. Databricks will build and start the app using the Git folder's current contents — no `databricks` CLI commands are needed.
   - Whenever you update the code, pull the latest changes into the Git folder (**Git folder** > **Pull**, via the UI) and click **Deploy** again in the Apps UI to redeploy.

5. Once deployed, open the app's URL from the Apps UI and hit `GET /healthz` to confirm it's running, then try `POST /sync` to pull data from Massive into Lakebase.

## Endpoints

- `GET /healthz` - health check
- `GET /records?limit=100` - read synced records from Lakebase
- `POST /sync?batch_size=500` with optional JSON body `{"path": "/records"}` - pull from Massive API and upsert into Lakebase
- `GET /watchlist` - get the current user's watchlist symbols with last known price
- `POST /watchlist` - add/update a symbol on the current user's watchlist
- `DELETE /watchlist/<symbol>` - remove a symbol from the current user's watchlist
- `POST /news/sync` with optional JSON body `{"tickers": ["AAPL", "MSFT"], "limit": 50}` - pull recent news per ticker from Massive and upsert into `ticker_news_documents`

## Scheduling the embeddings notebook as a Databricks Workflow

`notebooks/ingest_ticker_news_embeddings.py` is a self-contained ETL: it reads the distinct
tickers from the `watchlist` table, fetches news for those tickers directly from Massive
(serially, rate-limited to `max_requests_per_minute` - 5/min by default, matching the free
Massive API tier's strict limits), and upserts them into `ticker_news_documents`. It then turns
those rows into vector embeddings in `ticker_news_embeddings` (title + description) and
`ticker_news_chunk_embeddings` (chunks of the full article body, fetched from each article's
`article_url` and extracted with `trafilatura`). You can run it on a schedule two ways — pick
whichever fits your setup:

### Option A: Databricks Asset Bundle (CLI, version-controlled)

This repo already includes bundle config for this: `databricks.yml` +
`resources/ingest_ticker_news_embeddings_job.yml`. This is the recommended path if you want the
job definition tracked in git alongside the code.

1. Set the real workspace URL in `databricks.yml` (replace `<your-workspace-instance>`).
2. Deploy: `databricks bundle deploy -t dev`
3. Test it once manually: `databricks bundle run ingest_ticker_news_embeddings_job -t dev`
4. Once you've confirmed a successful run, flip `pause_status: PAUSED` to `pause_status: UNPAUSED`
   in `resources/ingest_ticker_news_embeddings_job.yml` and redeploy to turn on the daily schedule.

### Option B: Workflows UI (no CLI required)

If you'd rather not use the CLI, you can create the equivalent job by hand in the Databricks UI:

1. **Get the notebook into your workspace**: if you already created a Git folder for this repo
   (see step 7 above), the notebook is already there at `notebooks/ingest_ticker_news_embeddings.py`.
   Otherwise, upload/import it via **Workspace** > **Create** > **Notebook** > **Import**.
2. **Create the job**: go to **Workflows** (left sidebar) > **Jobs** > **Create Job**.
3. **Add a task**:
   - Task type: **Notebook**.
   - Notebook path: browse to `notebooks/ingest_ticker_news_embeddings.py` in your Git folder.
   - Cluster: choose **New job cluster** (a small general-purpose cluster is enough) or an existing
     cluster/serverless, if available.
   - Under **Parameters**, add the same widget values the notebook expects:
     - `watchlist_table_name` = `watchlist`
     - `news_table_name` = `ticker_news_documents`
     - `embeddings_table_name` = `ticker_news_embeddings`
     - `chunk_embeddings_table_name` = `ticker_news_chunk_embeddings`
     - `embedding_model` = `sentence-transformers/all-MiniLM-L6-v2`
     - `massive_secret_scope` = `massive`
     - `massive_secret_key` = `api-key`
     - `massive_api_base_url` = `https://api.massive.com`
     - `news_fetch_limit` = `50`
     - `max_requests_per_minute` = `5`
     - `chunk_size` = `800`
     - `chunk_overlap` = `100`
4. **Add a schedule**: click **Add trigger** on the job, choose **Scheduled**, and set it to run
   daily (e.g. 6:00 AM UTC) using either the simple picker or a cron expression
   (`0 0 6 * * ?`, timezone UTC).
5. **Add a failure notification**: under **Notifications**, add your email/Slack webhook for
   on-failure alerts.
6. Click **Create** and optionally **Run now** to validate the job before its first scheduled run.

Both options produce the same result — a Databricks Workflow that runs the notebook and refreshes
`ticker_news_embeddings`. The Asset Bundle keeps the definition in git and reproducible across
workspaces; the UI path is quicker for a one-off class demo but isn't tracked in version control.

## Enabling Change Data Feed (CDF) for Postgres tables

Lakebase supports **Change Data Feed (CDF)**, a managed way to stream row-level inserts/updates/deletes
from your Lakebase Postgres tables into Unity Catalog Delta tables (no Debezium, no custom connectors).
CDF is enabled per-**schema** in the `databricks_postgres` database, and every table in that schema that
meets two conditions is picked up automatically: it has `REPLICA IDENTITY FULL` set, and it has at least
one row.

> **Note:** CDF is only available on paid Databricks accounts — it is not supported on the free
> Databricks Community Edition or trial tier.

### 1. Set `REPLICA IDENTITY FULL` on the tables you want to track

By default, Postgres only logs primary-key columns on change. To capture full row contents (needed for
CDF), enable `REPLICA IDENTITY FULL` on each table — including `watchlist` and `massive_records` from
this app:

```sql
ALTER TABLE watchlist REPLICA IDENTITY FULL;
ALTER TABLE massive_records REPLICA IDENTITY FULL;
```

Run this once per table, either from a Databricks SQL editor connected to your Lakebase instance, or
from a `psql` session using your `LAKEBASE_URL`. Any new table you add later (e.g. via `ensure_table`-style
helpers in `app.py`) needs the same `ALTER TABLE ... REPLICA IDENTITY FULL` statement run once before it
will be included in the feed. Tables with the setting but zero rows are skipped until the first row is
inserted, then picked up automatically.

You can confirm which tables currently qualify by querying:

```sql
SELECT * FROM wal2delta.tables;
```

### 2. Start CDF from the Lakebase UI

1. In your Databricks workspace, open the **Lakebase** tab for your instance.
2. Go to **Lakebase CDF** and click **Start**.
3. Select the `databricks_postgres` database and the schema containing your tables (the default
   schema, `public`, works — it's inside `databricks_postgres`).
4. Choose the Unity Catalog destination schema/catalog where the CDF history tables should land.
5. Confirm — the UI shows a preview of qualifying tables (e.g. `watchlist`, `massive_records`) and
   their sync status before you start.

Once running, each qualifying table gets a corresponding Delta table named `lb_<table_name>_history`
(e.g. `lb_watchlist_history`) in Unity Catalog, updated roughly every 15 seconds. Each row includes
metadata columns (`_pg_change_type`, `_pg_lsn`, `_pg_xid`, `_timestamp`, `_sort_by`) describing the
change, so downstream Delta Live Tables/pipelines can build Silver/Gold layers off the append-only
history.

> **Note:** Disabling CDF is lossy — changes made while it's off aren't captured, and re-enabling
> triggers a full resync (every row reloaded as an `insert`). There's no per-table exclusion option
> within an enabled schema; the only way to keep a table out of the feed is to not set
> `REPLICA IDENTITY FULL` on it.

## Deployment and Running

### MCP Server Deployment

The MCP server is deployed as a **Databricks App** and provides the tool execution layer for the agent.

**Location**: `mcp_server/`

**Configuration**: `mcp_server/app.yaml`
```yaml
command:
  - "python"
  - "stock_mcp_server.py"

resources:
  - name: requirements
    source:
      path: ./requirements.txt
```

**Deployment Steps**:

1. **Create a Git Folder** in Databricks workspace pointing to this repository
2. **Navigate to Compute > Apps** in Databricks UI
3. **Create App**:
   * Name: `stock-research-mcp-server`
   * Source: Point to `mcp_server/` directory in Git folder
   * Databricks reads `app.yaml` automatically
4. **Deploy**: Click Deploy in Apps UI
5. **Get App URL**: Copy the deployed app URL (needed for Genie/Agent configuration)

**Environment Variables** (auto-configured via secrets):
* `MASSIVE_SECRET_SCOPE=massive`
* `MASSIVE_SECRET_KEY=api-key`
* `MASSIVE_API_BASE_URL=https://api.polygon.io` (note: code calls it "Massive" but uses Polygon.io)

**Port**: Default 8000 (via `DATABRICKS_APP_PORT` or `PORT` env var)

**Health Check**: `GET /mcp/health` (FastMCP standard endpoint)

### Flask App (Optional)

The root-level `app.py` provides a Flask web UI for:
* Watchlist management (add/remove stocks via web form)
* News sync triggering
* Direct Lakebase data inspection

**Not required for the Agent/MCP workflow** — the agent calls MCP tools directly, not the Flask endpoints.

**To run locally**:
```bash
pip install -r requirements.txt
python app.py
```

**Endpoints**:
* `GET /` - Watchlist web UI
* `GET /healthz` - Health check
* `GET /watchlist` - Get current user's watchlist (JSON)
* `POST /watchlist` - Add stock to watchlist
* `DELETE /watchlist/<symbol>` - Remove stock
* `POST /news/sync` - Trigger news fetch for tickers

### Connecting Databricks Agent to MCP Server

1. **Deploy MCP Server** as Databricks App (steps above)
2. **Copy App URL** (e.g., `https://<workspace>.cloud.databricks.com/apps/<app-id>`)
3. **Configure Genie Space** or **Agent Bricks Agent**:
   * Add MCP server URL to agent configuration
   * Agent Framework auto-discovers tools via MCP introspection
4. **Test**: Ask agent "Show me my watchlist" — agent should call MCP `get_watchlist` tool

### Database Setup

Before running news ingestion, manually create tables in Lakebase:

1. **Connect to Lakebase** using the connection URL from Step 2 (setup secrets)
2. **Run SQL scripts** in `sql/` directory in order:
   * `01_setup_news_table.sql` - Creates `ticker_news_documents`
   * `02_setup_embeddings_table.sql` - Creates `ticker_news_embeddings` (replace `{{EMBEDDING_DIM}}` with `384`)
   * `03_setup_chunk_embeddings_table.sql` - Creates `ticker_news_chunk_embeddings` (replace `{{EMBEDDING_DIM}}` with `384`)
   * `05_setup_research_tables.sql` - Creates `research_notes` and `analysis_reports`

**Watchlist table** is auto-created by `app.py` on first run.

**Why manual setup?** 
* Spark JDBC cannot execute `CREATE EXTENSION vector` or `CREATE INDEX USING hnsw`
* Spark cannot write directly to pgvector `VECTOR` type
* Manual setup ensures proper indexing for semantic search performance

### News Ingestion ETL

**Notebook**: `notebooks/ingest_ticker_news_embeddings.py`

**Scheduling Options**:

#### Option A: Databricks Asset Bundle (CLI)
```bash
# Edit databricks.yml: set workspace URL
databricks bundle deploy -t dev
databricks bundle run ingest_ticker_news_embeddings_job -t dev

# To enable daily schedule: change pause_status to UNPAUSED in resources/ingest_ticker_news_embeddings_job.yml
```

#### Option B: Workflows UI (No CLI)
1. Go to **Workflows > Jobs > Create Job**
2. Add notebook task: `notebooks/ingest_ticker_news_embeddings.py`
3. Set parameters (see notebook for required widget values)
4. Add trigger: Daily schedule (e.g., 6 AM UTC)
5. Save and run

**What it does**:
* Reads tickers from `watchlist` table
* Fetches news per ticker from Polygon.io (rate-limited: 5 req/min)
* Stores articles in `ticker_news_documents`
* Generates embeddings:
  * Title + description → `ticker_news_embeddings`
  * Article body chunks → `ticker_news_chunk_embeddings`
* Uses `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)

**Post-processing** (after first run):
```sql
-- Convert arrays to pgvector type
UPDATE ticker_news_embeddings 
SET embedding = embedding::vector 
WHERE embedding IS NOT NULL;

UPDATE ticker_news_chunk_embeddings 
SET embedding = embedding::vector 
WHERE embedding IS NOT NULL;
```

## Known Limitations

### User Identity in MCP Requests

**Issue**: The current MCP request environment does not include `X-Forwarded-User` or `X-Forwarded-Email` headers.

**Impact**: 
* MCP tools that require user identification (`get_watchlist`, `add_to_watchlist`, `remove_from_watchlist`, `save_research_note`, `save_analysis_report`) cannot automatically determine the calling user
* The code references a `DEMO_USER_EMAIL` fallback variable, but this variable is **not defined** in `stock_mcp_server.py`

**Current Behavior**:
* If `email` parameter is explicitly passed to the tool, that email is used
* If `X-Forwarded-Email` header is present, that email is used (currently not present)
* Otherwise, code attempts to fall back to `DEMO_USER_EMAIL` (undefined, will raise `NameError`)

**Workaround**:
* For testing: Pass `email` parameter explicitly in tool calls
* For production: Define `DEMO_USER_EMAIL` environment variable in MCP server deployment, or fix authentication header forwarding in the Databricks App/MCP integration

**Code Location**: `mcp_server/stock_mcp_server.py` lines 140, 174, 211, 252, 295

### API Rate Limits

**Polygon.io Free Tier**: 5 API calls per minute

**Mitigations**:
* News ingestion notebook includes configurable rate limiting (`max_requests_per_minute` parameter, default: 5)
* ETL runs once daily (scheduled), not on-demand
* Historical price queries count against rate limit — use sparingly in testing

### Embedding Model on Serverless

**Current**: `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions, CPU-friendly)

**Limitation**: Model loads on every MCP tool invocation; no persistent embedding service

**Impact**: `search_stock_research` has ~500ms+ cold-start latency on first query

**Future Enhancement**: Deploy embedding model as Model Serving endpoint for sub-100ms inference

### Vector Search Scope

**Current**: pgvector with HNSW index in Lakebase Postgres

**Limitations**:
* No cross-ticker semantic search UI (must specify ticker or search all)
* No reranking or hybrid search (keyword + semantic)
* No query expansion or multi-vector retrieval

**Performance**: HNSW index provides sub-50ms retrieval for top-k queries (k ≤ 10) on datasets up to ~100K chunks

## Project Status

### ✅ Completed Capabilities

* **MCP Server Deployment**
  * FastMCP server running as Databricks App
  * HTTP transport with health check endpoint
  * 9 tools exposed to Databricks Agent

* **Stock Price Retrieval**
  * Historical daily prices (OHLCV + VWAP)
  * Latest price lookup
  * Price summary statistics (change, % change, highs/lows)
  * Integration with Polygon.io API

* **Semantic News Search**
  * News ingestion from Polygon.io
  * Article chunking with overlap
  * Embedding generation (`sentence-transformers/all-MiniLM-L6-v2`)
  * pgvector storage with HNSW indexing
  * Cosine similarity search
  * Filtering by ticker and date range

* **Watchlist Management**
  * Per-user watchlist in Lakebase
  * Add/remove stocks
  * List watchlist with last known prices
  * Integration with news ETL (fetches news for watchlist tickers only)

* **Research Persistence**
  * Save research notes with optional ticker association
  * Save completed analysis reports
  * Timestamped records per user

* **ETL Pipeline**
  * Automated news ingestion notebook
  * Rate-limited API calls (respects free tier)
  * Incremental updates (deduplication via article ID)
  * Article body extraction (trafilatura)
  * Scheduled execution via Databricks Workflows

* **Database Layer**
  * Lakebase Postgres with pgvector extension
  * 6 tables: watchlist, news documents, embeddings (2 tables), research notes, analysis reports
  * HNSW indexes for vector search
  * Native password authentication (static credentials)

* **Agent Integration**
  * Tools discoverable via MCP introspection
  * Conversational interface via Databricks Agent Framework
  * Multi-step reasoning (price + news correlation)
  * Natural language → SQL/API → synthesis

### 🚧 Known Issues

* **User identity**: `X-Forwarded-Email` header not present in MCP requests; `DEMO_USER_EMAIL` fallback undefined
* **Cold start latency**: Embedding model loads on every MCP server invocation (~500ms)
* **No cross-ticker search UI**: Agent must specify ticker or search all news (no smart scoping)

### 📋 Future Enhancements

* **Model Serving**: Deploy embedding model as Databricks Model Serving endpoint
* **Reranking**: Add semantic reranker for improved top-k precision
* **Hybrid Search**: Combine keyword (BM25) + semantic for better recall
* **Streaming ETL**: Real-time news ingestion via Polygon.io WebSocket
* **Multi-modal**: Image analysis for charts in financial documents
* **Portfolio Tracking**: Track positions, cost basis, P&L
* **Backtesting**: Historical "what would the agent have recommended?" analysis

## Repository Structure

```
ai-stock-research-assistant/
├── mcp_server/                    # MCP server (Databricks App)
│   ├── stock_mcp_server.py        # FastMCP tool definitions
│   ├── stock_adapter.py           # Tool implementation (DB + API calls)
│   ├── massive_client.py          # Polygon.io API client
│   ├── app.yaml                   # Databricks App config
│   └── requirements.txt
├── notebooks/
│   ├── ingest_ticker_news_embeddings.py   # ETL: news → embeddings
│   └── ingest_stock_price_history.py      # ETL: historical prices
├── sql/                           # Lakebase table DDL
│   ├── 01_setup_news_table.sql
│   ├── 02_setup_embeddings_table.sql
│   ├── 03_setup_chunk_embeddings_table.sql
│   ├── 04_cast_arrays_to_vectors.sql
│   ├── 05_setup_research_tables.sql
│   └── README.md
├── app.py                         # Flask app (optional web UI)
├── lakebase.py                    # Lakebase connection helper
├── massive_client.py              # Polygon.io client (root-level copy)
├── setup_secrets.py               # One-time secret configuration
├── app.yaml                       # Flask app Databricks App config
├── databricks.yml                 # Databricks Asset Bundle config
├── resources/
│   └── ingest_ticker_news_embeddings_job.yml
├── requirements.txt
└── README.md                      # This file
```

## Notes

- Lakebase auth uses a single `LAKEBASE_URL` secret pointing at a native Postgres role with a static, non-expiring password — no token refresh logic needed.
- The codebase refers to the stock data API as "Massive API" but actually uses Polygon.io endpoints (historical naming artifact).
- For very large batch upserts, consider `psycopg2.extras.execute_values` instead of per-row inserts.
- The Flask app (`app.py`) and MCP server (`mcp_server/stock_mcp_server.py`) are separate deployments — agents call MCP tools, not Flask endpoints.
