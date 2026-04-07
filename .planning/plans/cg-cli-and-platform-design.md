# CG CLI & Unified Platform Design

## 1. Research Findings

### 1.1 FastMCP Client (Python)

FastMCP provides `fastmcp.Client` — an async Python client that can call any MCP server's tools programmatically:

```python
from fastmcp import Client

async with Client("https://mcp.common-ground.nyc/mcp") as client:
    tools = await client.list_tools()
    result = await client.call_tool("building", {"address": "305 Linden Blvd, Brooklyn"})
```

Key facts:
- **Async-native** — uses `async with` context manager for connection lifecycle
- **Transport**: SSE (server-sent events) over HTTP — the same transport our FastMCP server already exposes
- **Structured results**: `call_tool()` returns `ToolResult` with `.content`, `.structured_content`, `.meta`
- **No extra dependencies** — `fastmcp` (already in our stack) includes the client
- **Auth**: Custom headers can be passed via transport config (for API key auth)

Source: [FastMCP Client docs](https://gofastmcp.com/clients/client)

### 1.2 Typer + Rich for CLI

**Typer** (by the FastAPI author) is the standard Python CLI framework:
- Type-hint-driven argument parsing — no boilerplate
- Auto-generated `--help` from docstrings
- Built-in shell completion (bash, zsh, fish)
- Subcommand groups via `typer.Typer()` nesting
- Version: 0.15+ (stable, mature)

**Rich** provides terminal formatting:
- Tables, panels, progress bars, syntax highlighting
- Markdown rendering in the terminal
- JSON pretty-printing with color
- Tree views (for ownership networks)

Combined: `typer` + `rich` + `httpx` (for HTTP transport to MCP) = ~3 dependencies total.

Sources: [Typer docs](https://typer.tiangolo.com/), [dasroot.net guide](https://dasroot.net/posts/2026/01/building-cli-tools-with-typer-and-rich/)

### 1.3 Distribution

Three channels, in order of ease:

| Channel | Install command | Effort |
|---------|----------------|--------|
| **pipx** | `pipx install cg-cli` | Publish to PyPI, done |
| **Homebrew tap** | `brew install commonground/tap/cg` | GitHub repo `commonground/homebrew-tap` + formula via homebrew-pypi-poet |
| **Direct** | `uv tool install cg-cli` | Same PyPI package, zero config |

**Recommendation**: Start with PyPI (`pipx install cg-cli`). Add Homebrew tap later when demand warrants. The `uv tool install` path is free once on PyPI.

Source: [Simon Willison's Homebrew packaging TIL](https://til.simonwillison.net/homebrew/packaging-python-cli-for-homebrew)

---

## 2. MCP Server Tool Inventory

15 tools registered as read-only super tools. Each absorbs multiple sub-functions via `mode`/`view`/`type`/`role` parameters:

| Tool | Primary input | Dispatch param | Sub-functions |
|------|-------------|----------------|---------------|
| `address_report` | address string | — | Full address report |
| `building` | address or BBL | `view` | profile, story, context, block, twins, enforcement, history, flipper |
| `entity` | person/company name | `role` | auto, background, cop, judge, vitals, top |
| `neighborhood` | ZIP code | `view` | full, compare, gentrification, environment, hotspot, area, restaurants |
| `network` | name or BBL | `type` | all, ownership, corporate, political, worst, transaction, contractor, influence |
| `school` | name, DBN, or ZIP | — | School profile + comparison |
| `semantic_search` | natural language | `domain` | auto, complaints, violations, entities, explore |
| `query` | SQL or keyword | `mode` | sql, nl, catalog, schemas, tables, describe, health, admin |
| `safety` | precinct/ZIP/coords | `view` | full, crashes, shootings, force, hate, summons |
| `health` | ZIP code | — | Health indicators |
| `legal` | name or BBL | — | Court cases, settlements |
| `civic` | name or keyword | `view` | contracts, permits, jobs, budget |
| `transit` | location | — | Parking tickets, MTA, traffic |
| `services` | ZIP code | — | Childcare, shelters, food pantries |
| `suggest` | — | — | Onboarding, discovery |

**Server endpoint**: `https://mcp.common-ground.nyc/mcp` (SSE transport via Cloudflare Tunnel)

---

## 3. CLI Design: `cg`

### 3.1 Command Mapping

Each MCP tool maps to a top-level CLI command. The CLI is a **thin client** — it formats args, calls the MCP tool, and renders the response.

```
cg building "305 Linden Blvd, Brooklyn"           → building(address="305 Linden Blvd, Brooklyn")
cg building 3012340001 --view history              → building(address="3012340001", view="history")
cg entity "Jared Kushner"                          → entity(name="Jared Kushner")
cg entity "Kushner" --role background              → entity(name="Kushner", role="background")
cg neighborhood 10456                              → neighborhood(location="10456")
cg neighborhood 10456,10457 --view compare         → neighborhood(location="10456", zipcodes="10456,10457", view="compare")
cg network "Kushner Companies" --type corporate    → network(name="Kushner Companies", type="corporate")
cg school "Brooklyn Tech"                          → school(query="Brooklyn Tech")
cg search "apartments with mold"                   → semantic_search(query="apartments with mold")
cg search "John Smith" --domain entities           → semantic_search(query="John Smith", domain="entities")
cg sql "SELECT * FROM lake.housing.hpd_violations LIMIT 10"  → query(input="...", mode="sql")
cg catalog "eviction"                              → query(input="eviction", mode="catalog")
cg safety 10456                                    → safety(location="10456")
cg health 10456                                    → health(location="10456")
cg legal "305 Linden Blvd"                         → legal(query="305 Linden Blvd")
cg civic --view contracts                          → civic(query="", view="contracts")
cg transit "Grand Central"                         → transit(location="Grand Central")
cg services 10456                                  → services(location="10456")
cg suggest                                         → suggest()
cg watch "305 Linden Blvd" --events violations     → subscription API (see §4)
```

### 3.2 Output Modes

```
cg building "305 Linden Blvd"                  # default: Rich table
cg building "305 Linden Blvd" --format json    # raw JSON (pipe to jq)
cg building "305 Linden Blvd" --format csv     # CSV (pipe to csvtool, import to sheets)
cg building "305 Linden Blvd" --format raw     # plain text (MCP ToolResult.content as-is)
```

Global flags:
- `--format` / `-f`: `table` (default), `json`, `csv`, `raw`
- `--no-color`: disable Rich formatting
- `--verbose` / `-v`: show MCP call metadata (timing, tool name)
- `--version`: print version

### 3.3 Configuration

```toml
# ~/.cg/config.toml
[connection]
endpoint = "https://mcp.common-ground.nyc/mcp"
api_key = "cg_live_..."    # or set CG_API_KEY env var

[display]
format = "table"           # default output format
color = true
```

CLI config commands:
```
cg config set endpoint https://mcp.common-ground.nyc/mcp
cg config set api_key cg_live_abc123
cg config show
```

Auth resolution order:
1. `CG_API_KEY` env var
2. `~/.cg/config.toml` → `connection.api_key`
3. Anonymous (rate-limited, no subscriptions)

### 3.4 Shell Completion

Typer provides this for free:
```
cg --install-completion    # adds to .zshrc / .bashrc
```

### 3.5 Project Structure

```
cg-cli/
├── pyproject.toml          # [project.scripts] cg = "cg_cli.main:app"
├── src/cg_cli/
│   ├── main.py             # typer.Typer() app, top-level commands
│   ├── client.py           # FastMCP Client wrapper (async → sync bridge)
│   ├── config.py           # ~/.cg/config.toml read/write
│   ├── display.py          # Rich formatters (table, json, csv, raw)
│   ├── commands/
│   │   ├── building.py     # cg building
│   │   ├── entity.py       # cg entity
│   │   ├── neighborhood.py # cg neighborhood
│   │   ├── network.py      # cg network
│   │   ├── search.py       # cg search (→ semantic_search)
│   │   ├── query.py        # cg sql, cg catalog
│   │   ├── safety.py       # cg safety
│   │   ├── watch.py        # cg watch (subscriptions)
│   │   └── ...             # health, legal, civic, transit, services, school, suggest
│   └── subscriptions.py    # Subscription CRUD (shared with Telegram)
└── tests/
    ├── test_client.py
    ├── test_display.py
    └── test_commands/
```

### 3.6 Key Implementation: MCP Client Bridge

The CLI needs a sync-to-async bridge since Typer is synchronous:

```python
# src/cg_cli/client.py
import asyncio
from fastmcp import Client

class CGClient:
    """Thin sync wrapper around FastMCP async client."""

    def __init__(self, endpoint: str, api_key: str | None = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def call(self, tool: str, args: dict) -> dict:
        return asyncio.run(self._call(tool, args))

    async def _call(self, tool: str, args: dict) -> dict:
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with Client(self.endpoint, headers=headers) as client:
            result = await client.call_tool(tool, args)
            return {
                "content": result.content,
                "structured": result.structured_content,
                "meta": result.meta,
            }
```

### 3.7 Example Command Implementation

```python
# src/cg_cli/commands/building.py
import typer
from typing import Annotated

from cg_cli.client import get_client
from cg_cli.display import render

app = typer.Typer()

@app.command()
def building(
    address: Annotated[str, typer.Argument(help="NYC address or BBL")],
    view: Annotated[str, typer.Option(help="profile|story|context|block|twins|enforcement|history|flipper")] = "profile",
    format: Annotated[str, typer.Option("--format", "-f", help="table|json|csv|raw")] = "table",
) -> None:
    """Look up a NYC building by address or BBL."""
    client = get_client()
    result = client.call("building", {"address": address, "view": view})
    render(result, format=format)
```

---

## 4. Shared Subscription API

### 4.1 Why Shared

Both `cg watch` (CLI) and the Telegram bot create subscriptions to track changes. They need a shared backend so:
- A user who subscribes via CLI sees the same subscription in Telegram
- The notification engine has one source of truth
- Subscription CRUD is DRY

### 4.2 API Design

REST endpoints, either as a new service or mounted on the existing FastMCP server (FastAPI underneath):

```
POST   /api/subscriptions          — create a subscription
GET    /api/subscriptions          — list my subscriptions
GET    /api/subscriptions/:id      — get one
PATCH  /api/subscriptions/:id      — modify (change events, pause)
DELETE /api/subscriptions/:id      — remove
```

### 4.3 Subscription Schema

```sql
CREATE TABLE subscriptions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       VARCHAR NOT NULL,         -- "telegram:12345" or "apikey:cg_live_abc"
    target_type   VARCHAR NOT NULL,         -- "address", "bbl", "entity", "zip", "query"
    target_value  VARCHAR NOT NULL,         -- "305 Linden Blvd", "3012340001", "Kushner"
    events        VARCHAR[] NOT NULL,       -- ["violations", "sales", "permits", "complaints"]
    channel       VARCHAR NOT NULL,         -- "telegram", "webhook", "email" (future)
    channel_config JSONB DEFAULT '{}',      -- {"chat_id": 12345} or {"url": "https://..."}
    active        BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    last_notified TIMESTAMPTZ,
    notify_count  INTEGER DEFAULT 0
);

CREATE INDEX idx_subs_user ON subscriptions(user_id);
CREATE INDEX idx_subs_target ON subscriptions(target_type, target_value);
```

### 4.4 Auth Model

| Client | user_id format | Auth mechanism |
|--------|---------------|----------------|
| Telegram bot | `telegram:{user_id}` | Bot verifies Telegram user_id internally |
| CLI | `apikey:{key_prefix}` | `Authorization: Bearer cg_live_...` header |
| Website | `browser:{fingerprint}` | Session cookie (future) |

API keys are stored hashed (SHA-256). The `user_id` is derived from the key at auth time.

### 4.5 Subscription Request

```json
POST /api/subscriptions
{
    "target_type": "address",
    "target_value": "305 Linden Blvd, Brooklyn",
    "events": ["violations", "sales"],
    "channel": "telegram",
    "channel_config": {"chat_id": 12345}
}
```

### 4.6 CLI Usage

```bash
# Watch an address for new violations
cg watch "305 Linden Blvd" --events violations,sales

# Watch an entity for any new cross-references
cg watch --entity "Kushner Companies" --events all

# Watch a ZIP code for new complaints
cg watch --zip 10456 --events complaints

# List my subscriptions
cg watch --list

# Remove a subscription
cg watch --remove sub_abc123
```

### 4.7 LLM-Powered Natural Language Subscriptions

The CLI supports free-form subscription descriptions via a **server-side parse endpoint**:

```bash
cg watch "anything about lead paint in the Bronx"
```

#### Parse Endpoint (shared by CLI + Telegram)

```
POST /api/subscriptions/parse
Body: {"text": "anything about lead paint in the Bronx"}

Response (success):
{
  "parsed": {
    "sub_type": "keyword_watch",
    "filter_value": "lead paint",
    "filter_extra": {"tables": ["hpd_violations", "hpd_complaints"], "borough": "BRONX"},
    "frequency": "daily",
    "description": "Lead paint violations and complaints in the Bronx"
  },
  "preview": {"last_30d_alerts": 47},
  "status": "ok"
}

Response (ambiguous):
{
  "clarification": "Did you mean lead paint violations specifically, or all lead-related complaints?",
  "status": "clarify"
}
```

#### Two-Stage Pipeline (per NLP designer)

1. **Stage 1: Haiku 4.5** extracts intent + slots (sub_type, filter_value, filter_extra, frequency) via strict tool use — guarantees structured output schema every time
2. **Stage 2: MCP resolution** — `building()` resolves address to BBL, `entity()` verifies names exist in the lake

**Important**: Do NOT reuse the existing `query(mode="nl")` Gemini path. That translates NL to SQL for one-shot queries. Subscriptions need structured metadata (sub_type, filter_value, frequency), not raw SQL. The Haiku strict-tool-use approach is purpose-built for this.

#### Why Server-Side

1. **Single parser, two clients** — Telegram and CLI share the same prompt, tool definitions, and resolution logic. No drift.
2. **API key management** — Anthropic key stays on the server. CLI never touches it.
3. **Prompt iteration** — Update the system prompt once, both clients improve. No CLI release needed.
4. **Pre-filter regex** runs server-side — ~30-40% of requests skip the LLM entirely.

#### CLI Flow

```
$ cg watch "anything about lead paint in the Bronx"

Parsed subscription:
  Type:      keyword_watch
  Keyword:   lead paint
  Area:      Bronx
  Tables:    hpd_violations, hpd_complaints
  Frequency: daily
  Preview:   47 alerts in last 30 days

Confirm? [Y/n]
```

#### Structured Fallback

```
$ cg watch "stuff"

Could not parse subscription. Use structured flags:
  cg watch --type address --address "305 Linden Blvd, Brooklyn"
  cg watch --type zip --zip 11226 --events violations,complaints
  cg watch --type keyword --keyword "lead paint" --tables hpd_violations
```

#### Guardrails

1. **Reject overly broad** — Haiku system prompt requires clarifying questions for ambiguous requests
2. **Cap per-user** — Server-side check before INSERT: 20 active subscriptions per user
3. **Confirmation required** — CLI uses interactive Y/n prompt; Telegram uses inline [Confirm] button. No silent subscriptions.

See full parser design: `.planning/plans/cg-subscription-parser-design.md`

---

## 5. Unified Platform Architecture

### 5.1 System Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         COMMON GROUND PLATFORM                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   CLIENTS                                                               │
│   ┌──────────┐  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐  │
│   │  cg CLI   │  │ Telegram Bot │  │    Website     │  │  MCP Hosts  │  │
│   │  (typer)  │  │  (aiogram)   │  │ (common-ground │  │  (Claude,   │  │
│   │           │  │              │  │   .nyc)        │  │   Cursor)   │  │
│   └─────┬─────┘  └──────┬───────┘  └───────┬───────┘  └──────┬──────┘  │
│         │               │                   │                  │         │
│         │  API key       │  Bot token        │  Session         │  SSE   │
│         ▼               ▼                   ▼                  ▼         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    GATEWAY (Cloudflare Tunnel)                   │   │
│   │  mcp.common-ground.nyc → :4213 (MCP/SSE)                       │   │
│   │  api.common-ground.nyc → :8080 (REST subscriptions)             │   │
│   │  common-ground.nyc     → :3000 (website)                        │   │
│   └──────────────────────────────┬──────────────────────────────────┘   │
│                                  │                                       │
│   ┌──────────────────────────────┼──────────────────────────────────┐   │
│   │              HETZNER SERVER (178.156.228.119)                    │   │
│   │                              │                                   │   │
│   │  ┌───────────────────────────┼────────────────────────────────┐ │   │
│   │  │                     DOCKER COMPOSE                         │ │   │
│   │  │                           │                                │ │   │
│   │  │  ┌────────────────┐  ┌────┴───────────┐  ┌──────────────┐ │ │   │
│   │  │  │  FastMCP Server │  │ Subscription   │  │  Notification │ │ │   │
│   │  │  │  (mcp_server.py)│  │ API (:8080)    │  │  Engine       │ │ │   │
│   │  │  │  :4213 SSE      │  │ FastAPI        │  │  (cron/sensor)│ │ │   │
│   │  │  │  15 tools       │  │ CRUD + auth    │  │  detect→push  │ │ │   │
│   │  │  └───────┬─────────┘  └───────┬────────┘  └───────┬──────┘ │ │   │
│   │  │          │                     │                    │        │ │   │
│   │  │          ▼                     ▼                    ▼        │ │   │
│   │  │  ┌──────────────────────────────────────────────────────┐   │ │   │
│   │  │  │                    DATA LAYER                         │   │ │   │
│   │  │  │  ┌──────────┐  ┌───────────┐  ┌──────────────────┐  │   │ │   │
│   │  │  │  │ DuckDB   │  │ Postgres  │  │ MinIO S3         │  │   │ │   │
│   │  │  │  │ (DuckLake│  │ - DuckLake│  │ - Parquet files  │  │   │ │   │
│   │  │  │  │  via PG) │  │   catalog │  │ - 400M+ rows     │  │   │ │   │
│   │  │  │  │          │  │ - Subs DB │  │                   │  │   │ │   │
│   │  │  │  │          │  │ - Users   │  │                   │  │   │ │   │
│   │  │  │  └──────────┘  └───────────┘  └──────────────────┘  │   │ │   │
│   │  │  │  ┌──────────────────┐  ┌────────────────────────┐   │   │ │   │
│   │  │  │  │ Lance DB         │  │ Graph Cache (Parquet)  │   │   │ │   │
│   │  │  │  │ - 2.96M entity   │  │ - 40+ graph tables     │   │   │ │   │
│   │  │  │  │   embeddings     │  │ - ownership/corp/pol   │   │   │ │   │
│   │  │  │  └──────────────────┘  └────────────────────────┘   │   │ │   │
│   │  │  └──────────────────────────────────────────────────────┘   │ │   │
│   │  └────────────────────────────────────────────────────────────┘ │   │
│   └────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │              INGESTION (Mac Studio, Docker)                      │   │
│   │  Dagster → dlt → MinIO S3 → DuckLake                            │   │
│   │  260 datasets, daily refresh at 4 AM                             │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Shared Infrastructure

| Component | Shared by | Purpose |
|-----------|-----------|---------|
| **FastMCP Server** | CLI, Telegram, Website, MCP hosts | All data queries go through MCP tools |
| **Subscription API** | CLI (`cg watch`), Telegram bot | CRUD for watch subscriptions |
| **Subscription DB** (Postgres) | Sub API, Notification Engine | Single source of truth for subscriptions |
| **Notification Engine** | Telegram bot, CLI webhooks, email (future) | Detects changes, dispatches alerts |
| **DuckDB/DuckLake** | FastMCP tools (read), Dagster (write) | The data lake |
| **Postgres** | DuckLake catalog, Subscription DB, user identity | Single Postgres instance, multiple databases |
| **PostHog** | All clients (via MCP middleware) | Analytics, usage tracking |

### 5.3 User Identity

Anonymous but trackable — no login required:

| Surface | Identity | How assigned |
|---------|----------|-------------|
| MCP hosts (Claude, Cursor) | `mcp:{session_id}` | PostHog session, auto-assigned |
| Telegram | `telegram:{user_id}` | Telegram provides it |
| CLI | `apikey:{key_hash[:8]}` | User runs `cg config set api_key ...` |
| Website | `browser:{posthog_id}` | PostHog distinct_id from cookie |

If a user later wants to "link" identities (see CLI subs in Telegram), that's a future feature. For now, each surface has its own identity namespace.

### 5.4 Data Flow: Notification Engine

```
Dagster pipeline completes materialization
  → writes new rows to DuckLake
  → Dagster sensor fires (or cron job every 15 min)
  → Notification Engine:
      1. Query subscription DB for active subscriptions
      2. For each subscription, check if target has new data:
         - violations: "SELECT COUNT(*) FROM hpd_violations WHERE bbl = ? AND novissueddate > ?"
         - sales: "SELECT COUNT(*) FROM acris_master WHERE ... AND document_date > ?"
         - complaints: "SELECT COUNT(*) FROM n311_service_requests WHERE incident_zip = ? AND created_date > ?"
      3. If new data found:
         - Format notification message
         - Dispatch to channel (Telegram API, webhook, email)
         - Update subscription.last_notified
```

The engine connects to DuckDB directly (same container network) — no MCP overhead for internal queries.

### 5.5 New Services Required

| Service | Stack | Port | Purpose |
|---------|-------|------|---------|
| **Subscription API** | FastAPI + asyncpg | 8080 | REST CRUD for subscriptions |
| **Notification Engine** | Python + APScheduler or Dagster sensor | — | Change detection + dispatch |
| **Telegram Bot** | aiogram 3.x | — | Long-polling, inline queries, Mini App |

All three run as Docker containers in the existing `docker-compose.yml` on Hetzner.

---

## 6. Implementation Phases

### Phase 1: CLI MVP (1 week)
- Scaffold `cg-cli` with typer + httpx
- Implement `CGClient` wrapping FastMCP async client
- Map all 15 tools to CLI commands
- Rich table output + JSON/CSV modes
- Config management (`~/.cg/config.toml`)
- Publish to PyPI

### Phase 2: Subscription API (1 week)
- Add `subscriptions` table to Postgres
- FastAPI service with CRUD endpoints
- API key generation + validation
- Mount in docker-compose

### Phase 3: `cg watch` (3 days)
- CLI commands for subscription management
- Wire to Subscription API
- Webhook channel for CLI notifications

### Phase 4: Notification Engine (1 week)
- Change detection queries per event type
- Telegram dispatch (shared with Telegram bot)
- Webhook dispatch (for CLI users)
- Backoff + dedup logic

### Phase 5: Distribution (2 days)
- Homebrew tap
- Shell completion docs
- README + usage examples

---

## 7. Key Design Decisions

1. **CLI is a thin MCP client** — no DuckDB dependency, no data processing. Just formats args and renders responses. This means the CLI works from anywhere with internet access.

2. **FastMCP Client over raw HTTP** — reuses the same protocol the server speaks. No custom REST API needed for data queries.

3. **Subscription API is separate from MCP** — MCP is stateless (tool calls), subscriptions are stateful (CRUD). Different concerns, different services.

4. **Postgres for subscriptions, not DuckDB** — subscriptions need ACID transactions, row-level updates, and the existing Postgres is already running for DuckLake catalog. Just add a `subscriptions` database.

5. **API keys, not OAuth** — CLI users are power users/journalists. API keys are simpler, work in scripts, and fit the threat model (public data, no PII).

6. **LLM subscription parsing is server-side** — `POST /api/subscriptions/parse` uses Haiku 4.5 strict tool use (NOT the existing Gemini NL→SQL path). Both CLI and Telegram share the same endpoint. Pre-filter regex skips LLM for ~30-40% of requests. See NLP designer's full spec in `cg-subscription-parser-design.md`.
