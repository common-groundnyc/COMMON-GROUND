# MCP Production Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply March 2026 MCP best practices to the Common Ground FastMCP server — cursor-per-request concurrency, tool timeouts, annotation updates, INSTRUCTIONS trim, MCP resources, per-tool observability.

**Architecture:** All changes in one file (`mcp_server.py`). Six independent tasks: (1) replace thread lock with cursor-per-request, (2) add tool timeouts, (3) update annotations with openWorldHint, (4) trim INSTRUCTIONS, (5) add MCP resources for schema catalog, (6) add per-tool latency tracking to PostHog. Each task is independently deployable.

**Tech Stack:** Python, FastMCP 3.1.1, DuckDB 1.5.0, PostHog

---

## Codebase Context

**File:** `infra/duckdb-server/mcp_server.py` (~12,500 lines)
**Current state:**
- 60 tools, ~6K tokens of descriptions (appropriate for domain complexity)
- `_db_lock = threading.Lock()` serializes ALL queries (reads + writes)
- `_execute()` wraps every query in `with _db_lock:`
- ~20 other places use `with _db_lock:` directly
- `READONLY` and `ADMIN` ToolAnnotations — missing `openWorldHint`
- INSTRUCTIONS has 38 lines including redundant POWER FEATURES section
- PostHog enabled but no per-tool latency tracking
- No MCP resources defined
- No tool timeouts
- BM25SearchTransform with `always_visible` list already implements the 2026 "ability-loading" pattern

**What NOT to change:**
- Tool descriptions — cross-references help routing, 6K tokens is fine for 60 domain tools
- BM25SearchTransform — already implements lazy tool loading
- Stateless transport — correct for read-only server
- TOON format — responses capped at 20 rows, savings minimal

---

## Task 1: Replace `_db_lock` with cursor-per-request

The single biggest performance improvement. DuckDB handles concurrent SELECT queries internally via MVCC. The thread lock serializes ALL 60 tools through one bottleneck. Replace with `conn.cursor()` per query for reads, keep the lock only for the one write tool (`sql_admin`).

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Update `_execute()` to use cursor instead of lock**

Find `_execute` at line ~591. Replace:

```python
def _execute(conn, sql, params=None):
    with _db_lock:
        try:
            cur = conn.execute(sql, params or [])
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(MAX_QUERY_ROWS)
            return cols, rows
        except duckdb.Error as e:
            err = str(e)
            hint = ""
            if "does not exist" in err.lower():
                hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
            elif "not found" in err.lower():
                hint = " Use describe_table(schema, table) to check column names."
            elif "permission" in err.lower() or "read-only" in err.lower():
                hint = " Only SELECT queries are allowed. Use sql_query() for reads."
            raise ToolError(f"SQL error: {e}{hint}")
```

With:

```python
def _execute(conn, sql, params=None):
    """Execute read-only SQL using a cursor. DuckDB handles concurrent reads via MVCC."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(MAX_QUERY_ROWS)
        return cols, rows
    except duckdb.Error as e:
        err = str(e)
        hint = ""
        if "does not exist" in err.lower():
            hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
        elif "not found" in err.lower():
            hint = " Use describe_table(schema, table) to check column names."
        elif "permission" in err.lower() or "read-only" in err.lower():
            hint = " Only SELECT queries are allowed. Use sql_query() for reads."
        raise ToolError(f"SQL error: {e}{hint}")
    finally:
        cur.close()
```

- [ ] **Step 2: Remove `_db_lock` from all read-only tool call sites**

Search for all `with _db_lock:` occurrences. For each one:
- If it's inside a **read-only** context (SELECT, fetch, describe) → **remove the lock wrapper**, keep the code inside
- If it's inside a **write** context (sql_admin tool, DDL, CREATE TABLE) → **keep the lock**

The key heuristic: if the code only reads data, remove the lock. If it modifies state (CREATE TABLE, INSERT, DROP), keep it.

There are ~20 lock sites. Most are reads. The only write site is `sql_admin` (around line 2856).

For each read site, change:
```python
# Before:
with _db_lock:
    result = db.execute(sql).fetchall()

# After:
cur = db.cursor()
try:
    result = cur.execute(sql).fetchall()
finally:
    cur.close()
```

**IMPORTANT:** Do NOT remove `_db_lock` from:
- The `sql_admin` tool (line ~2856) — this is the only write tool
- The `gentrification_signals` function (line ~4106) — creates temp tables for forecasting
- The `export_data` tool (line ~3232) — uses `db.execute()` directly for Excel generation
- The lifespan setup code (line ~2655-2665) — runs at startup, not concurrent
- The `build_percentile_tables` calls — these create temp tables

Also update `lifespan_context["lock"]` at line ~2728 to reference `_write_lock` instead of `_db_lock`.

- [ ] **Step 3: Rename `_db_lock` to `_write_lock` for clarity**

```python
_write_lock = threading.Lock()  # Only used for sql_admin writes
```

Update the remaining references (sql_admin, build_percentile_tables).

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "perf(mcp): replace global _db_lock with cursor-per-request for read concurrency"
```

---

## Task 2: Add tool timeouts

Prevent runaway queries from tying up the connection. The arxiv paper (2603.13417) recommends adaptive timeout budgets for sequential tool chains.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add timeout to sql_query tool**

Find the `sql_query` tool decorator (line ~2824). Change:
```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
```
To:
```python
@mcp.tool(annotations=READONLY, tags={"discovery"}, timeout=30.0)
```

- [ ] **Step 2: Add timeout to heavy graph tools**

Find these tools and add `timeout=15.0`:
- `ownership_clusters` — graph WCC across all buildings
- `worst_landlords` — full slumlord ranking
- `entity_influence` — PageRank across graphs
- `shortest_path` — graph path finding
- `flipper_detector` — property flip scan

For each, change the decorator to include `timeout=15.0`:
```python
@mcp.tool(annotations=READONLY, tags={"graph"}, timeout=15.0)
```

- [ ] **Step 3: Verify timeout parameter is supported**

Check FastMCP 3.1.1 docs — the `timeout` parameter may be on the `@mcp.tool` decorator or need a different approach. If not supported directly, implement via:
```python
import asyncio

# In the tool function:
# FastMCP 3.x runs sync tools in a thread pool, so we can't use asyncio timeout.
# Instead, use DuckDB's PRAGMA statement_timeout:
cur = db.cursor()
cur.execute("SET statement_timeout='30s'")  # DuckDB 1.5.0+
```

**NOTE:** Verify DuckDB 1.5.0 supports `statement_timeout`. If not, skip this step and document as future work.

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "perf(mcp): add timeouts to sql_query (30s) and heavy graph tools (15s)"
```

---

## Task 3: Update annotations with `openWorldHint`

Tell clients our tools are local DB queries, not external API calls. This is from the MCP spec 2025-06-18.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Update READONLY and ADMIN constants**

Find line ~46. Replace:
```python
READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
ADMIN = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)
```

With:
```python
READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
ADMIN = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False)
```

**NOTE:** If FastMCP 3.1.1 / MCP types don't support `openWorldHint` yet, this will fail at import. Check `ToolAnnotations` signature first:
```python
python -c "from mcp.types import ToolAnnotations; help(ToolAnnotations)"
```
If not supported, skip this task.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add openWorldHint=False — signals tools are local DB only"
```

---

## Task 4: Trim INSTRUCTIONS

Cut the POWER FEATURES section (redundant — BM25SearchTransform surfaces tools on demand) and BBL/ZIP format hints (already in Annotated types). Saves ~150 tokens per session.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Replace INSTRUCTIONS**

Find the INSTRUCTIONS string (line ~2742). Replace the entire string with:

```python
INSTRUCTIONS = """\
NYC public records data lake for investigative research — 500+ tables, 12 schemas, 1B+ rows.

ROUTING — pick the FIRST match:
* BUILDING by BBL → building_profile, landlord_watchdog, building_story
* PERSON/COMPANY by name → entity_xray, person_crossref
* LANDLORD/OWNER → landlord_network, worst_landlords, llc_piercer
* SCHOOL by name/ZIP → school_search, then school_report
* DISTRICT by number → district_report
* NEIGHBORHOOD by ZIP → neighborhood_portrait, neighborhood_compare
* OFFICER/COP by name → cop_sheet
* JUDGE by name → judge_profile
* CRIME/SAFETY by precinct → safety_report
* ENVIRONMENT/CLIMATE by ZIP → climate_risk
* BACKGROUND CHECK by name → due_diligence
* POLITICAL MONEY by name → money_trail (broader), pay_to_play (city-level)
* VITAL RECORDS/GENEALOGY → vital_records, marriage_search
* CORRUPTION/INFLUENCE → pay_to_play, shell_detector, flipper_detector
* FIND DATA by keyword → data_catalog, then list_tables
* CUSTOM QUERY → sql_query (read-only, all tables in 'lake' database)
* EXPORT/DOWNLOAD → export_data (branded XLSX)
* DON'T KNOW → suggest_explorations

Too many tools? Use search_tools(query) to discover domain tools.
"""
```

**Changes from current:**
- Updated stats: "500+ tables, 12 schemas, 1B+ rows" (was "294 tables, 60M+ rows")
- Removed WHAT'S HERE paragraph (1 line of overview is enough)
- Removed POWER FEATURES section (6 lines — tools self-describe, BM25 finds them)
- Removed SUGGEST INTERESTING THINGS paragraph (already in routing table)
- Removed BBL/ZIP format hints (already in Annotated types on every parameter)
- Shortened SEMANTIC SEARCH and EXPORT entries
- Kept routing table intact — this is the primary value

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "docs(mcp): trim INSTRUCTIONS — remove redundant sections, update stats to 1B+ rows"
```

---

## Task 5: Add MCP Resources for schema catalog

Expose schema descriptions and table catalog as MCP resources. Clients can pre-load this context without calling tools, giving the LLM awareness of what data exists before the user asks anything.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add schema resource**

After the `mcp = FastMCP(...)` constructor (line ~2817), add:

```python
# ---------------------------------------------------------------------------
# MCP Resources — pre-loadable context for clients
# ---------------------------------------------------------------------------

@mcp.resource("data://schemas")
def all_schemas() -> str:
    """All 12 NYC data lake schemas with descriptions."""
    lines = ["NYC Data Lake — Schema Overview\n"]
    for schema, desc in sorted(SCHEMA_DESCRIPTIONS.items()):
        lines.append(f"  {schema}: {desc}")
    return "\n".join(lines)


@mcp.resource("data://schemas/{schema_name}")
def schema_detail(schema_name: str) -> str:
    """Description of a specific data lake schema."""
    desc = SCHEMA_DESCRIPTIONS.get(schema_name)
    if not desc:
        return f"Unknown schema: {schema_name}. Available: {', '.join(sorted(SCHEMA_DESCRIPTIONS.keys()))}"
    return f"{schema_name}: {desc}"


@mcp.resource("data://routing")
def routing_guide() -> str:
    """How to route questions to the right tools."""
    return INSTRUCTIONS
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add MCP resources — schema descriptions and routing guide"
```

---

## Task 6: Add per-tool latency tracking to PostHog

Track tool name + execution time so we can detect slow tools, silent degradation, and usage patterns. The March 2026 observability research found 60+ API calls failing silently for 48 hours.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add PostHog middleware for tool tracking**

FastMCP 3.x has middleware support. Add a `PostToolUse` middleware that logs tool calls. Find the middleware imports at the top of the file and add after the `mcp = FastMCP(...)` constructor:

The file already imports `Middleware` and `MiddlewareContext` at line 12. Use the `on_call_tool` hook (NOT `run`/`call_next` — those don't exist):

```python
class ToolTrackingMiddleware(Middleware):
    """Log every tool call to PostHog — tool name, latency, success."""

    async def on_call_tool(self, context, call_next):
        t0 = time.time()
        result = await call_next(context)
        elapsed_ms = round((time.time() - t0) * 1000)
        tool_name = context.message.name
        if posthog.project_api_key:
            posthog.capture(
                distinct_id="mcp-server",
                event="tool_call",
                properties={
                    "tool": tool_name,
                    "duration_ms": elapsed_ms,
                    "success": not getattr(result, 'isError', False),
                },
            )
        return result
```

This covers ALL 60 tools with zero per-tool changes.

- [ ] **Step 2: Register middleware on the FastMCP constructor**

In Task 7 we're already modifying the constructor. Add `middleware`:
```python
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    list_page_size=25,
    middleware=[ToolTrackingMiddleware()],
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
    ],
)
```

The middleware parameter may be named differently in FastMCP 3.1.1. Check the constructor signature. It may need to be passed via `mcp.add_middleware(ToolTrackingMiddleware())` after construction instead.

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add per-tool PostHog tracking — tool name, latency, success"
```

---

## Task 7: Add `list_page_size` to FastMCP constructor

Paginate the 60-tool list for smaller clients.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Update FastMCP constructor**

Find line ~2807. Change:
```python
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
    ],
)
```

To:
```python
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    list_page_size=25,
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
    ],
)
```

**NOTE:** Check if `list_page_size` is a valid FastMCP 3.1.1 parameter. If not supported, skip.

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add list_page_size=25 for tool list pagination"
```

---

## Task 8: Deploy and verify

- [ ] **Step 1: Deploy**

```bash
cd ~/Desktop/dagster-pipeline
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/mcp_server.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/mcp_server.py
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && sudo docker compose build --no-cache duckdb-server && sudo docker compose up -d duckdb-server"
```

Wait ~3 minutes for startup.

- [ ] **Step 2: Verify tools still work**

```bash
# Test search_tools
curl -s https://mcp.common-ground.nyc/api/catalog | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'{d[\"summary\"][\"tables\"]} tables')"

# Test MCP is responding
# (search_tools via MCP client)
```

- [ ] **Step 3: Check PostHog**

After a few tool calls, verify events appear in PostHog with `tool_call` event name and `tool`, `duration_ms`, `success` properties.

- [ ] **Step 4: Commit final state**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): production hardening complete — concurrency, timeouts, observability, resources"
```
