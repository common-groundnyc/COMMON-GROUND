# MCP Error Rate Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drive MCP tool call error rate from 52% toward <10% by fixing observed failure modes in PostHog 48h data (excluding the already-in-progress parquet orphan re-materialize).

**Architecture:** Five surgical fixes to `infra/duckdb-server/`. Most work concentrates in `tools/query.py` (one real bug + splitting a 7-mode super-tool into discrete tools), with smaller edits to `tools/__init__.py`, `mcp_server.py`, and new error-formatting helpers. Each task ships independently and is reversible.

**Tech Stack:** FastMCP 3.x, Pydantic v2, Python 3.13, pytest, DuckDB 1.5

---

## Context for the engineer

### Why this plan exists

PostHog shows 80 errors out of 155 tool calls in the last 48h (52% error rate). After excluding 16 infrastructure errors (lake parquet orphans — already being re-ingested), the remaining errors cluster into 5 root causes:

| Error class | Count | Root cause |
|---|---|---|
| `'str' object has no attribute 'content'` | 2 | Real bug in `tools/query.py:536` — `_schemas()`, `_admin()`, `_health()`, and `_sql(fmt='xlsx'/'csv')` return `str` instead of `ToolResult`, but the wrapper calls `.content` on them |
| "Unknown tool" (`list_tables`, `describe_table`, `list_schemas`, `get_schema`, `data_catalog`, `execute`, `search`, `owner_violations`, `building_context`, `suggest_explorations`) | 12 | Clients guess at tool names that don't exist. These names are so obvious that every LLM reaches for them |
| Parameter shape (`{"sql": ...}` vs `{"input": ...}`) | 2 | `query()` has a generic `input` parameter covering 4 different modes. Models guess `sql:` as the key |
| Binder errors (wrong column name) | ~8 | Model guesses column names because it doesn't call `describe` first |
| DDL rejected | 4 | Generic "Only SELECT..." message doesn't tell the model what to do instead |

### Sources informing this plan

- **[Anthropic — Writing Effective Tools for Agents](https://www.anthropic.com/engineering/writing-tools-for-agents)** — parameter naming (`user_id` not `user`), namespacing, meaningful context in tool responses
- **[Pamela Fox — Do Stricter MCP Tool Schemas Increase Reliability?](http://blog.pamelafox.org/2026/03/do-stricter-mcp-tool-schemas-increase.html)** — Literal/enum beats free text
- **[Mastra — Tool compat layer 15% → 3%](https://mastra.ai/blog/mcp-tool-compatibility-layer)** — strict schema → dramatic error reduction
- **[Alpic — Better MCP error responses](https://alpic.ai/blog/better-mcp-tool-call-error-responses-ai-recover-gracefully)** — tool errors go back into model context; treat them as recovery prompts
- **[Bhagya Rana — Tool Calls That Never Existed](https://medium.com/@bhagyarana80/tool-calls-that-never-existed-d010526ad9b8)** — "do not invent tools" system rule

### Repo orientation

- `infra/duckdb-server/tools/query.py` — the 539-line SQL/catalog/schemas/tables/describe/health/admin super-tool. This is most of the work.
- `infra/duckdb-server/tools/__init__.py` — aggregates tool imports; `mcp_server.py:1883-1903` imports and registers them with `mcp.tool(annotations=READONLY)(...)`.
- `infra/duckdb-server/mcp_server.py:1820-1864` — `INSTRUCTIONS` constant passed to FastMCP as `instructions=`. That text is what every MCP client receives at init.
- `infra/duckdb-server/tests/` — pytest with `uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v` (from `CLAUDE.md`). There is no existing `test_query.py`; you will create it.
- **Dagster Docker rule:** this server is NOT Dagster; normal Python tests are fine. Run tests in the `infra/duckdb-server/` directory.
- **Deploy:** `git push` locally → `git pull` + `docker compose restart duckdb-server` on Hetzner per `docs/runbooks/deploy-mcp-server.md`. Don't deploy inside this plan — let the user do it.

### What's intentionally out of scope

- **Parquet orphans** — already auto-re-ingesting via freshness sensor run `35c3d4c1`.
- **Eval harness** — Anthropic's post recommends a held-out eval set with Claude Code optimizing against it. Worth doing later; not in this plan.
- **Dashboard web instrumentation** — no PostHog JS on common-ground.nyc/explore. Separate concern.
- **`_health()` / `_admin()` output refactor** — they currently return bare `str`. We'll fix the wrapper bug that crashes on them (Task 1) rather than restructure them.

---

## File Structure

Files touched:

- **Modify** `infra/duckdb-server/tools/query.py` — fix `'str' object` bug (T1); add `did_you_mean` helpers for column errors (T3); upgrade DDL error message (T3). Legacy `query()` kept as a thin shim that delegates to the new discrete tools (T2).
- **Create** `infra/duckdb-server/tools/catalog_tools.py` — new file holding the 6 discrete tools: `list_schemas`, `list_tables`, `describe_table`, `catalog_search`, `health_check`, `query_sql`. Keeps `query.py` focused on private helpers (T2).
- **Modify** `infra/duckdb-server/tools/__init__.py` — export the 6 new tools (T2).
- **Modify** `infra/duckdb-server/mcp_server.py:1820-1864` — update `INSTRUCTIONS` to route to new tools and add anti-hallucination rule (T2, T4).
- **Modify** `infra/duckdb-server/mcp_server.py:1883-1903` — register the 6 new tools (T2).
- **Create** `infra/duckdb-server/tests/test_query_wrapper.py` — regression test for T1 bug.
- **Create** `infra/duckdb-server/tests/test_catalog_tools.py` — tests for the 6 new tools (T2).
- **Create** `infra/duckdb-server/tests/test_error_guidance.py` — tests for upgraded error messages (T3).

---

## Task 1: Fix `'str' object has no attribute 'content'` bug

**Why first:** It's a real server bug that crashes 2+ tool calls/day with no actionable error for the model. Smallest fix, highest certainty.

**Root cause (read `infra/duckdb-server/tools/query.py:462-539`):** The `query()` dispatcher wraps every mode's return value in a `ToolResult` at lines 535-539. But four branches return plain `str` instead of `ToolResult`:
- `_schemas()` → `str` (line 328)
- `_admin()` → `str` (line 227)
- `_health()` → `str` (line 420)
- `_sql()` with `fmt='xlsx'` or `fmt='csv'` → `str` (lines 194-215)

The wrapper at line 536 does `result.content` unconditionally, which raises `AttributeError: 'str' object has no attribute 'content'` when `result` is a plain string.

**Files:**
- Modify: `infra/duckdb-server/tools/query.py:534-539`
- Create: `infra/duckdb-server/tests/test_query_wrapper.py`

- [ ] **Step 1.1: Write the failing test**

Create `infra/duckdb-server/tests/test_query_wrapper.py`:

```python
"""Regression tests for query() wrapper bugs observed in PostHog.

Issue: four branches return str instead of ToolResult, crashing the wrapper.
PostHog error: "'str' object has no attribute 'content'"
"""
from unittest.mock import MagicMock

import pytest
from fastmcp.tools.tool import ToolResult

from tools.query import query


class _FakeCtx:
    def __init__(self, pool, catalog):
        self.lifespan_context = {
            "pool": pool,
            "catalog": catalog,
            "emb_conn": None,
            "embed_fn": None,
        }


def _fake_catalog():
    return {
        "housing": {
            "hpd_violations": {"row_count": 10_000_000, "column_count": 20, "description": ""},
        },
    }


def test_query_mode_schemas_returns_toolresult(monkeypatch):
    """mode='schemas' returns a str from _schemas; wrapper must not crash."""
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="", mode="schemas", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert isinstance(result.content, str)
    assert "housing" in result.content


def test_query_mode_health_returns_toolresult(monkeypatch):
    """mode='health' returns a str from _health; wrapper must not crash."""
    # _health executes SQL against the pool; stub it out.
    monkeypatch.setattr(
        "tools.query._health",
        lambda pool, schema_filter=None: "health: ok",
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="", mode="health", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "health: ok" in result.content


def test_query_mode_admin_returns_toolresult(monkeypatch):
    """mode='admin' returns a str from _admin; wrapper must not crash."""
    monkeypatch.setattr(
        "tools.query._admin",
        lambda pool, sql, ctx: "view created",
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="CREATE OR REPLACE VIEW v AS SELECT 1", mode="admin", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "view created" in result.content


def test_query_sql_xlsx_export_returns_toolresult(monkeypatch):
    """mode='sql' with format='xlsx' returns a str from _sql export path; wrapper must not crash."""
    monkeypatch.setattr(
        "tools.query._sql",
        lambda pool, sql, fmt, ctx: "Exported 100 rows to XLSX",
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="SELECT 1", mode="sql", format="xlsx", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "Exported" in result.content
```

- [ ] **Step 1.2: Run the test to confirm it fails**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_query_wrapper.py -v
```

Expected: 4 tests fail with `AttributeError: 'str' object has no attribute 'content'`.

- [ ] **Step 1.3: Fix the wrapper**

Replace `infra/duckdb-server/tools/query.py:534-539`:

```python
# Normalize: dispatch branches may return str OR ToolResult.
if isinstance(result, ToolResult):
    body = result.content if isinstance(result.content, str) else (
        "\n".join(str(c) for c in result.content) if result.content else ""
    )
    return ToolResult(
        content=directive + body,
        structured_content=result.structured_content,
        meta=result.meta,
    )
return ToolResult(content=directive + str(result))
```

- [ ] **Step 1.4: Run the test — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_query_wrapper.py -v
```

Expected: 4 passed.

- [ ] **Step 1.5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/query.py infra/duckdb-server/tests/test_query_wrapper.py
git -c commit.gpgsign=false commit -m "fix(mcp): query() wrapper crash on str returns

Four dispatch branches return str instead of ToolResult (schemas, admin,
health, sql export). The wrapper called .content unconditionally, raising
AttributeError in PostHog. Normalize to ToolResult in both cases."
```

---

## Task 2: Split `query()` into discrete, LLM-obvious tools

**Why:** 12 "unknown tool" errors in 48h are all the same pattern — clients calling `list_tables`, `describe_table`, `list_schemas`, `get_schema`, `data_catalog`, `suggest_explorations`. These names are so obvious that every LLM reaches for them. Collapsing them into `query(mode=...)` hides them. Anthropic's guidance: *"input parameters should be unambiguously named: instead of `user`, try `user_id`"* — same principle applies to `input` vs `sql`, `schema`, `table`.

**Strategy:**
- New file `tools/catalog_tools.py` with 6 discrete tools: `list_schemas`, `list_tables`, `describe_table`, `catalog_search`, `health_check`, `query_sql`.
- Each delegates to the existing private helpers in `tools/query.py` (`_schemas`, `_tables`, `_describe`, `_catalog`, `_health`, `_sql`) — no logic duplication.
- The existing `query()` tool stays as a **deprecated shim** that prints a deprecation notice and delegates. Do not remove it — external callers may still use it.
- `nl` mode stays on `query()` only (natural-language SQL is its own beast and not a 2026 hallucination vector).
- `admin` mode stays on `query()` only (rarely used; DDL is a footgun and should keep its friction).

**Files:**
- Create: `infra/duckdb-server/tools/catalog_tools.py`
- Modify: `infra/duckdb-server/tools/__init__.py`
- Modify: `infra/duckdb-server/mcp_server.py:1820-1864` (INSTRUCTIONS)
- Modify: `infra/duckdb-server/mcp_server.py:1883-1903` (tool registration)
- Create: `infra/duckdb-server/tests/test_catalog_tools.py`

- [ ] **Step 2.1: Write the failing test for the 6 new tools**

Create `infra/duckdb-server/tests/test_catalog_tools.py`:

```python
"""Tests for the 6 discrete catalog tools split out of query()."""
from unittest.mock import MagicMock

import pytest
from fastmcp.tools.tool import ToolResult

from tools.catalog_tools import (
    catalog_search,
    describe_table,
    health_check,
    list_schemas,
    list_tables,
    query_sql,
)


class _FakeCtx:
    def __init__(self, pool, catalog, embed_fn=None, emb_conn=None):
        self.lifespan_context = {
            "pool": pool,
            "catalog": catalog,
            "embed_fn": embed_fn,
            "emb_conn": emb_conn,
        }


def _fake_catalog():
    return {
        "housing": {
            "hpd_violations": {"row_count": 10_000_000, "column_count": 20, "description": "violations"},
            "hpd_complaints": {"row_count": 500_000, "column_count": 15, "description": "complaints"},
        },
        "public_safety": {
            "motor_vehicle_collisions": {"row_count": 2_000_000, "column_count": 30, "description": "crashes"},
        },
    }


def test_list_schemas_returns_schema_summary():
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = list_schemas(ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "housing" in result.content
    assert "public_safety" in result.content
    assert "2 tables" in result.content  # housing has 2


def test_list_tables_returns_tables_in_schema(monkeypatch):
    # _tables uses fuzzy matching via the pool, mock it
    monkeypatch.setattr(
        "tools.catalog_tools._tables",
        lambda pool, schema_input, catalog: ToolResult(
            content=f"Schema 'housing': 2 tables", structured_content=None, meta={}
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = list_tables(schema="housing", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "housing" in result.content


def test_describe_table_returns_columns(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._describe",
        lambda pool, table_input, catalog: ToolResult(
            content="lake.housing.hpd_violations: 10,000,000 rows, 20 columns",
            structured_content=None,
            meta={},
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = describe_table(schema="housing", table="hpd_violations", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "hpd_violations" in result.content


def test_query_sql_executes_select(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._sql",
        lambda pool, sql, fmt, ctx: ToolResult(
            content="Query returned 5 rows (42ms).",
            structured_content=None,
            meta={"query_time_ms": 42},
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query_sql(sql="SELECT * FROM lake.housing.hpd_violations LIMIT 5", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "5 rows" in result.content


def test_catalog_search_finds_tables(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._catalog",
        lambda pool, keyword, catalog, embed_fn, ctx: ToolResult(
            content="Found 2 tables matching 'eviction'", structured_content=None, meta={}
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = catalog_search(keyword="eviction", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "eviction" in result.content


def test_health_check_returns_health(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._health",
        lambda pool, schema_filter=None: "housing: 15 tables fresh",
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = health_check(ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "fresh" in result.content
```

- [ ] **Step 2.2: Run the test to confirm it fails**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_catalog_tools.py -v
```

Expected: `ImportError: cannot import name 'list_schemas' from 'tools.catalog_tools'`.

- [ ] **Step 2.3: Create the new `catalog_tools.py` file**

Create `infra/duckdb-server/tools/catalog_tools.py`:

```python
"""Discrete catalog tools split out of the legacy query() super-tool.

Each tool is thin — it delegates to the private helpers already defined in
tools/query.py. The motivation is agent ergonomics: LLMs consistently guess
tool names like `list_tables`, `describe_table`, `list_schemas`, so we expose
them directly instead of hiding them behind `query(mode='...')`.

See: docs/superpowers/plans/2026-04-08-mcp-error-rate-reduction.md
"""
from typing import Annotated

from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from tools.query import (
    _catalog,
    _describe,
    _health,
    _schemas,
    _sql,
    _tables,
)

_DIRECTIVE = (
    "PRESENTATION: Show the complete results as an interactive table. "
    "All rows, all columns. Do not summarize.\n\n"
)


def _wrap(result) -> ToolResult:
    """Normalize str|ToolResult into a ToolResult with the presentation directive."""
    if isinstance(result, ToolResult):
        body = result.content if isinstance(result.content, str) else (
            "\n".join(str(c) for c in result.content) if result.content else ""
        )
        return ToolResult(
            content=_DIRECTIVE + body,
            structured_content=result.structured_content,
            meta=result.meta,
        )
    return ToolResult(content=_DIRECTIVE + str(result))


def list_schemas(ctx: Context = None) -> ToolResult:
    """List every schema in the NYC data lake with table counts and row totals.

    Use this first when you don't know where a concept lives. Returns schemas
    like 'housing', 'public_safety', 'education', 'health', 'city_government'.
    """
    catalog = ctx.lifespan_context.get("catalog", {})
    return _wrap(_schemas(catalog))


def list_tables(
    schema: Annotated[
        str,
        Field(description="Schema name, e.g. 'housing', 'public_safety', 'health'. Fuzzy matched."),
    ],
    ctx: Context = None,
) -> ToolResult:
    """List every table inside a schema, ranked by row count.

    Returns table_name, row_count, column_count. Call list_schemas() first
    if you don't know which schema to ask about.
    """
    pool = ctx.lifespan_context["pool"]
    catalog = ctx.lifespan_context.get("catalog", {})
    return _wrap(_tables(pool, schema, catalog))


def describe_table(
    schema: Annotated[
        str,
        Field(description="Schema name, e.g. 'housing'. Fuzzy matched."),
    ],
    table: Annotated[
        str,
        Field(description="Table name inside the schema, e.g. 'hpd_violations'. Fuzzy matched."),
    ],
    ctx: Context = None,
) -> ToolResult:
    """Show columns, types, and comments for one table.

    ALWAYS call this before writing custom SQL against an unfamiliar table —
    column names in the lake often differ from what you'd guess (e.g.
    'lic_expir_dd', not 'lic_expiry_dd').
    """
    pool = ctx.lifespan_context["pool"]
    catalog = ctx.lifespan_context.get("catalog", {})
    qualified = f"{schema}.{table}"
    return _wrap(_describe(pool, qualified, catalog))


def catalog_search(
    keyword: Annotated[
        str,
        Field(description="Keyword or concept, e.g. 'eviction', 'noise', 'restaurant inspection'."),
    ],
    ctx: Context = None,
) -> ToolResult:
    """Search the data lake catalog by keyword and table/column descriptions.

    Use this when you don't know which table holds the data you want. Returns
    matching tables ranked by relevance.
    """
    pool = ctx.lifespan_context["pool"]
    catalog = ctx.lifespan_context.get("catalog", {})
    embed_fn = ctx.lifespan_context.get("embed_fn")
    return _wrap(_catalog(pool, keyword, catalog, embed_fn, ctx=ctx))


def health_check(
    schema: Annotated[
        str,
        Field(description="Optional: restrict to one schema, e.g. 'housing'. Empty = all schemas.", default=""),
    ] = "",
    ctx: Context = None,
) -> ToolResult:
    """Data lake health — row counts, null rates, freshness per table.

    Use this to check whether a table is fresh before trusting its data.
    """
    pool = ctx.lifespan_context["pool"]
    return _wrap(_health(pool, schema.strip() or None))


def query_sql(
    sql: Annotated[
        str,
        Field(
            description=(
                "Read-only SQL. Tables are in `lake.<schema>.<table>` format. "
                "Only SELECT/WITH/EXPLAIN/DESCRIBE/SHOW/PRAGMA allowed."
            ),
            examples=[
                "SELECT borough, COUNT(*) FROM lake.housing.hpd_violations GROUP BY borough",
                "SELECT * FROM lake.public_safety.motor_vehicle_collisions WHERE crash_date > '2025-01-01' LIMIT 20",
            ],
        ),
    ],
    format: Annotated[
        str,
        Field(
            description="'text' for in-conversation results, 'xlsx' for Excel download, 'csv' for CSV download.",
            default="text",
        ),
    ] = "text",
    ctx: Context = None,
) -> ToolResult:
    """Run custom read-only SQL against the NYC data lake.

    Before writing SQL against an unfamiliar table, call describe_table(schema, table)
    to see the real column names. Column names are case-sensitive and often
    abbreviated (e.g. 'lic_expir_dd' not 'license_expiry_date').
    """
    pool = ctx.lifespan_context["pool"]
    return _wrap(_sql(pool, sql, format, ctx))
```

- [ ] **Step 2.4: Export the new tools from `tools/__init__.py`**

Modify `infra/duckdb-server/tools/__init__.py`:

```python
from tools.address_report import address_report
from tools.building import building
from tools.catalog_tools import (
    catalog_search,
    describe_table,
    health_check,
    list_schemas,
    list_tables,
    query_sql,
)
from tools.civic import civic
from tools.entity import entity
from tools.health import health
from tools.legal import legal
from tools.neighborhood import neighborhood
from tools.network import network
from tools.query import query
from tools.safety import safety
from tools.school import school
from tools.semantic_search import semantic_search
from tools.services import services
from tools.suggest import suggest
from tools.transit import transit
```

- [ ] **Step 2.5: Run the catalog_tools tests — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_catalog_tools.py -v
```

Expected: 6 passed.

- [ ] **Step 2.6: Register the new tools in `mcp_server.py`**

Modify `infra/duckdb-server/mcp_server.py` around line 1883. Change the import block and the registration block:

```python
from tools import (
    address_report,
    building,
    catalog_search,
    civic,
    describe_table,
    entity,
    health,
    health_check,
    legal,
    list_schemas,
    list_tables,
    neighborhood,
    network,
    query,
    query_sql,
    safety,
    school,
    semantic_search,
    services,
    suggest,
    transit,
)

# Domain super-tools
mcp.tool(annotations=READONLY)(address_report)
mcp.tool(annotations=READONLY)(building)
mcp.tool(annotations=READONLY)(entity)
mcp.tool(annotations=READONLY)(neighborhood)
mcp.tool(annotations=READONLY)(network)
mcp.tool(annotations=READONLY)(school)
mcp.tool(annotations=READONLY)(semantic_search)
mcp.tool(annotations=READONLY)(safety)
mcp.tool(annotations=READONLY)(health)
mcp.tool(annotations=READONLY)(legal)
mcp.tool(annotations=READONLY)(civic)
mcp.tool(annotations=READONLY)(transit)
mcp.tool(annotations=READONLY)(services)
mcp.tool(annotations=READONLY)(suggest)

# Catalog + SQL tools (split from the legacy query() super-tool — see
# docs/superpowers/plans/2026-04-08-mcp-error-rate-reduction.md)
mcp.tool(annotations=READONLY)(list_schemas)
mcp.tool(annotations=READONLY)(list_tables)
mcp.tool(annotations=READONLY)(describe_table)
mcp.tool(annotations=READONLY)(catalog_search)
mcp.tool(annotations=READONLY)(health_check)
mcp.tool(annotations=READONLY)(query_sql)

# Legacy query() kept for backwards compatibility with existing clients
mcp.tool(annotations=READONLY)(query)
```

- [ ] **Step 2.7: Update INSTRUCTIONS to route to new tools**

Modify `infra/duckdb-server/mcp_server.py:1820-1864`. Replace the `INSTRUCTIONS` block:

```python
INSTRUCTIONS = """Common Ground -- NYC open data lake. 294 tables, 60M+ rows, 14 schemas.

ROUTING -- pick the FIRST match:
* Street address or "where I live"       -> address_report()
* Address + specific question             -> building()
* BBL (10-digit number)                   -> building()
* Person or company name                  -> entity()
* Cop by name                             -> entity(role="cop")
* Judge by name                           -> entity(role="judge")
* Birth/death/marriage records            -> entity(role="vitals")
* ZIP code or "this neighborhood"         -> neighborhood()
* Compare ZIPs                            -> neighborhood(view="compare")
* Restaurants in an area                  -> neighborhood(view="restaurants")
* Landlord portfolio or slumlord score    -> network(type="ownership")
* LLC piercing or shell companies         -> network(type="corporate")
* Campaign donations or lobbying          -> network(type="political")
* Worst landlords ranking                 -> network(type="worst")
* School by name, DBN, or ZIP             -> school()
* "Find complaints about X" or concepts   -> semantic_search()
* Crime, crashes, shootings               -> safety()
* Health data, COVID, hospitals           -> health()
* Court cases, settlements, hearings      -> legal()
* City contracts, permits, jobs, budget   -> civic()
* Parking tickets, MTA, traffic           -> transit()
* Childcare, shelters, food pantries      -> services()
* "What can I explore?" or unsure         -> suggest()

EXPLORING THE LAKE (when the domain tools don't cover the question):
* "What schemas exist?"                   -> list_schemas()
* "What tables are in X?"                 -> list_tables(schema="X")
* "What columns does T have?"             -> describe_table(schema="X", table="T")
* "Which tables mention <concept>?"       -> catalog_search(keyword="...")
* "Is this table fresh?"                  -> health_check()
* Custom SQL                              -> query_sql(sql="SELECT ...")
* Download/export data                    -> query_sql(sql="...", format="xlsx")

BEFORE WRITING CUSTOM SQL: call describe_table(schema, table) to get real
column names. The lake often uses abbreviated names (e.g. 'lic_expir_dd',
not 'lic_expiry_dd'). Guessing column names causes Binder Errors.

WORKFLOWS -- chain tools for deep investigations:
* Landlord investigation: building() -> entity() -> network(type="ownership")
* Follow the money: entity() -> network(type="political") -> civic(view="contracts")
* School comparison: school("02M475,02M001")
* Health equity: health("10456") -> services("10456") -> neighborhood("10456")
* Unknown table: catalog_search("eviction") -> describe_table(...) -> query_sql(...)

PRESENTATION -- how to show Common Ground data to users:
* Every tool returns a complete, pre-formatted report. Present ALL sections.
* Do NOT summarize, compress, or omit sections. Each section has independent data.
* Use interactive visualizations for numeric comparisons, rankings, or percentiles.
* Preserve the tool's section headers and structure.
* When a report has a "Drill deeper" footer, show it — those are actionable tool calls.
* If the response includes a PRESENTATION directive, follow it exactly.

TOOL DISCIPLINE:
* Use only the tools listed above. If no available tool can complete the
  request, say so clearly. Do NOT invent tool names, simulate tool results,
  or guess at tools that "should" exist.
* The legacy query() tool is deprecated — prefer the discrete catalog tools
  (list_schemas, list_tables, describe_table, catalog_search, health_check,
  query_sql) for clarity.

This is NYC-only data. Do not query for national/federal statistics."""
```

- [ ] **Step 2.8: Run the full server test suite — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/ -v -x
```

Expected: all tests pass. If any pre-existing test fails because it imported the old tool list, adjust the import but do not change assertions.

- [ ] **Step 2.9: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/catalog_tools.py \
        infra/duckdb-server/tools/__init__.py \
        infra/duckdb-server/mcp_server.py \
        infra/duckdb-server/tests/test_catalog_tools.py
git -c commit.gpgsign=false commit -m "feat(mcp): split query() into 6 discrete tools

PostHog shows 12 'unknown tool' errors in 48h from clients calling
list_tables, describe_table, list_schemas, get_schema, data_catalog,
suggest_explorations — names every LLM guesses first.

Expose them as real tools instead of hiding them behind query(mode=...).
Legacy query() stays as a back-compat shim. Updates INSTRUCTIONS to route
to the new tools and adds a 'do not invent tool names' discipline rule.

Refs: docs/superpowers/plans/2026-04-08-mcp-error-rate-reduction.md"
```

---

## Task 3: Upgrade error responses with recovery guidance

**Why:** Per Alpic (2025) and Anthropic (2025), tool errors are re-injected into the model's context — treat them as second prompts, not dead ends. Today our `Binder Error` just says "column X not found"; we can suggest the real column names. Our DDL rejection just says "Only SELECT allowed"; we can point the model at `query(mode='admin')`.

**Scope:**
- Wrap column-not-found errors in `_sql` to include a "did you mean" list from `describe_table`.
- Rewrite the DDL rejection error with guidance.
- **Not in scope:** all DB errors. Just these two high-signal classes.

**Files:**
- Modify: `infra/duckdb-server/tools/query.py:163-176` (`_sql` function — wrap Binder Errors)
- Modify: `infra/duckdb-server/tools/safety.py` if it's the SQL validator, otherwise check `sql_utils.py` (DDL rejection message)
- Create: `infra/duckdb-server/tests/test_error_guidance.py`

- [ ] **Step 3.1: Find where the DDL error is raised**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
grep -rn "Only SELECT" infra/duckdb-server/ --include="*.py"
```

Expected: identifies the file and line. It's likely `infra/duckdb-server/sql_utils.py` or `tools/safety.py`.

Read the file. Note the exact function name and line where the error is raised.

- [ ] **Step 3.2: Write the failing test for upgraded errors**

Create `infra/duckdb-server/tests/test_error_guidance.py`:

```python
"""Tests that error messages guide the model toward a working next action."""
import pytest
from fastmcp.exceptions import ToolError

from sql_utils import validate_sql  # adjust if step 3.1 found a different location


def test_ddl_error_points_at_admin_mode():
    """Rejected DDL should tell the model to use query(mode='admin') or query_sql alternatives."""
    with pytest.raises(ToolError) as exc:
        validate_sql("CREATE TABLE foo AS SELECT 1")

    msg = str(exc.value)
    # The original short message
    assert "SELECT" in msg
    # New guidance
    assert "admin" in msg.lower() or "read-only" in msg.lower()
    assert "describe_table" in msg or "query_sql" in msg


def test_ddl_error_explains_what_to_do_instead():
    """The error should be actionable, not just a refusal."""
    with pytest.raises(ToolError) as exc:
        validate_sql("INSERT INTO foo VALUES (1)")

    msg = str(exc.value)
    assert "read-only" in msg.lower()
```

- [ ] **Step 3.3: Run the test to confirm it fails**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_error_guidance.py -v
```

Expected: fails — the current error message is "Only SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, and PRAGMA queries are allowed." with no guidance.

- [ ] **Step 3.4: Upgrade the DDL error message**

In the file found in step 3.1, replace the DDL error message with:

```python
raise ToolError(
    "This tool is read-only and only accepts SELECT, WITH, EXPLAIN, DESCRIBE, "
    "SHOW, and PRAGMA statements. For DDL (CREATE VIEW, etc.), use "
    "query(mode='admin', input='CREATE OR REPLACE VIEW ...'). "
    "To explore schema structure, use list_schemas(), list_tables(schema), "
    "or describe_table(schema, table) instead."
)
```

- [ ] **Step 3.5: Run the error guidance test — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_error_guidance.py -v
```

Expected: 2 passed.

- [ ] **Step 3.6: Add Binder Error wrapping in `_sql()`**

Modify `infra/duckdb-server/tools/query.py`. In `_sql()` (around line 163), wrap the `execute()` call with a try/except that catches column-not-found errors and augments them with a "did you mean" pointer. Replace lines 163-176:

```python
def _sql(pool: CursorPool, sql: str, fmt: str, ctx: Context) -> ToolResult | str:
    """Execute a read-only SQL query, optionally exporting to xlsx/csv."""
    validate_sql(sql)
    t0 = time.time()
    try:
        cols, rows = execute(pool, sql.strip().rstrip(";"))
    except Exception as exc:
        msg = str(exc)
        # Binder errors include a candidate list from DuckDB itself — surface
        # actionable guidance pointing the model at describe_table.
        if "not found in FROM clause" in msg or "Binder Error" in msg:
            hint = (
                "\n\nHINT: Call describe_table(schema, table) to see the real "
                "column names before writing SQL. Column names in the lake are "
                "often abbreviated (e.g. 'lic_expir_dd' not 'license_expiry_date')."
            )
            raise ToolError(f"SQL error: {msg}{hint}") from exc
        raise ToolError(f"SQL error: {msg}") from exc
    elapsed = round((time.time() - t0) * 1000)

    if fmt == "text":
        return make_result(
            f"Query returned {len(rows)} rows ({elapsed}ms).",
            cols,
            rows,
            {"query_time_ms": elapsed},
        )
```

Leave the rest of `_sql()` (export path) unchanged.

- [ ] **Step 3.7: Add a regression test for the Binder Error wrapper**

Append to `infra/duckdb-server/tests/test_error_guidance.py`:

```python
from unittest.mock import MagicMock

from fastmcp import Context
from tools.query import _sql


def test_binder_error_includes_describe_table_hint(monkeypatch):
    """A Binder Error from DuckDB should be wrapped with actionable guidance."""
    def _raise(pool, sql):
        raise Exception(
            'Binder Error: Referenced column "foo_dd" not found in FROM clause!\n'
            'Candidate bindings: "foo_id", "foo_date"'
        )

    monkeypatch.setattr("tools.query.execute", _raise)
    monkeypatch.setattr("tools.query.validate_sql", lambda sql: None)

    ctx = MagicMock(spec=Context)
    with pytest.raises(ToolError) as exc:
        _sql(MagicMock(), "SELECT foo_dd FROM t", "text", ctx)

    msg = str(exc.value)
    assert "Binder Error" in msg
    assert "describe_table" in msg
    assert "foo_id" in msg or "foo_date" in msg  # DuckDB's own candidates preserved
```

- [ ] **Step 3.8: Run the full error guidance test file — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_error_guidance.py -v
```

Expected: 3 passed.

- [ ] **Step 3.9: Run the full test suite to make sure nothing regressed**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 3.10: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/query.py \
        infra/duckdb-server/tests/test_error_guidance.py
# sql_utils.py or tools/safety.py depending on where step 3.1 found the DDL error
git add infra/duckdb-server/sql_utils.py 2>/dev/null || git add infra/duckdb-server/tools/safety.py
git -c commit.gpgsign=false commit -m "feat(mcp): upgrade error messages with recovery guidance

Per Alpic/Anthropic guidance: tool errors are re-injected into the model
context, so treat them as second prompts.

- Binder errors now include a describe_table() hint alongside DuckDB's
  own candidate column list
- DDL rejection points at query(mode='admin') and list_*/describe_table
  for exploration

Refs: docs/superpowers/plans/2026-04-08-mcp-error-rate-reduction.md"
```

---

## Task 4: Verify anti-hallucination rule is live

Task 2 step 2.7 already adds the "TOOL DISCIPLINE" block to `INSTRUCTIONS`, but this task is a belt-and-braces check that the rule is actually being served by the MCP server.

**Files:**
- Modify: `infra/duckdb-server/tests/test_explore_routes.py` (or create a new small test file if that one is unrelated)

- [ ] **Step 4.1: Add a test that asserts the instruction is in the served metadata**

Create `infra/duckdb-server/tests/test_server_instructions.py`:

```python
"""Sanity check: the MCP server's instructions include the tool discipline rule."""
import mcp_server


def test_instructions_include_tool_discipline():
    text = mcp_server.INSTRUCTIONS
    assert "TOOL DISCIPLINE" in text
    assert "do NOT invent tool names" in text.lower() or "do not invent tool names" in text.lower()


def test_instructions_route_to_new_catalog_tools():
    text = mcp_server.INSTRUCTIONS
    assert "list_schemas()" in text
    assert "describe_table(" in text
    assert "query_sql(" in text
```

- [ ] **Step 4.2: Run the test — expect pass**

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_server_instructions.py -v
```

Expected: 2 passed (because Task 2 already updated INSTRUCTIONS).

- [ ] **Step 4.3: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tests/test_server_instructions.py
git -c commit.gpgsign=false commit -m "test(mcp): assert INSTRUCTIONS contain tool discipline rule"
```

---

## Task 5: Stage for deploy

Don't deploy from this plan — the user has a documented deploy runbook and will pull the changes themselves. Just make the branch ready.

- [ ] **Step 5.1: Run the full test suite one final time**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5.2: Show the user the summary of what changed**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git log --oneline main..HEAD
git diff --stat main..HEAD
```

Expected: 4 commits (T1 fix, T2 feat, T3 feat, T4 test), modifying `tools/query.py`, `tools/__init__.py`, `tools/catalog_tools.py` (new), `mcp_server.py`, `sql_utils.py` or `tools/safety.py`, and 4 new test files.

- [ ] **Step 5.3: Post a hand-off message to the user**

Summarise:
- What each task did (in one sentence each)
- Expected error-rate reduction (per the plan's intro: ~16 errors eliminated from the 80, plus ongoing recovery from upgraded error messages)
- Pointer to `docs/runbooks/deploy-mcp-server.md` for deploy
- Suggestion: after deploy, re-run the PostHog 48h query from this session to confirm error rate dropped

---

## Out-of-scope follow-ups (deliberately not in this plan)

1. **Literal/enum audit of every tool parameter** — Pamela Fox's strict-schema finding. Worth a separate plan; touches every tool file.
2. **Tool-use eval harness** — Anthropic's eval-driven optimization loop. 30-50 real-world prompts with verifier. Separate plan.
3. **PostHog web SDK on common-ground.nyc/explore** — needed to understand actual dashboard traffic. Separate concern (different repo).
4. **Parquet orphan re-materialize** — already in progress via freshness sensor.
5. **Removing the legacy `query()` tool** — wait one release cycle for external callers to migrate.
