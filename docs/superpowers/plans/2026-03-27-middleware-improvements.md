# Middleware Improvements — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden and extend the 4-layer middleware stack — sync skip lists, add source citation to every response, add data freshness warnings, and extend percentile coverage to new entity types.

**Architecture:** All middleware files are in `infra/duckdb-server/`. The stack runs in order: ResponseLimiting → OutputFormatter → Percentile → PostHog. Each task modifies one file. Independent and parallelizable.

**Tech Stack:** Python, FastMCP 3.1.1 middleware API (`on_call_tool(context, call_next)`), DuckDB

---

## Current State

### Middleware stack (applied in order)
| # | File | Class | What it does |
|---|------|-------|-------------|
| 1 | FastMCP built-in | `ResponseLimitingMiddleware(50_000)` | Caps response at 50KB |
| 2 | `response_middleware.py` | `OutputFormatterMiddleware` | Auto-formats tabular data: kv (1-3 rows), markdown (4-50), TOON (51+) |
| 3 | `percentile_middleware.py` | `PercentileMiddleware` | Injects percentile rankings for buildings/restaurants/ZIPs/precincts/owners |
| 4 | `mcp_server.py` | `PostHogMiddleware` | Tracks tool calls with client identity |

### Skip list inconsistency
`OutputFormatterMiddleware._SKIP_TOOLS`:
```python
{"list_schemas", "search_tools", "data_catalog", "sql_admin"}
```

`PercentileMiddleware._SKIP_TOOLS`:
```python
{"list_schemas", "list_tables", "describe_table", "data_catalog", "search_tools",
 "call_tool", "sql_query", "sql_admin", "suggest_explorations", "export_csv", "text_search"}
```

The OutputFormatter skips 4 tools; the PercentileMiddleware skips 11. Neither includes the new tools we built today (school_search, cop_sheet, due_diligence, etc.) — but that's correct since they need formatting.

### What's missing
1. **Source citation** — no middleware appends "Data from: lake.housing.hpd_violations" to responses
2. **Freshness warnings** — no middleware warns when data is stale (last_run_at > 7 days)
3. **Percentile coverage** — only 5 entity types (building, restaurant, ZIP, precinct, owner). Missing: school (DBN), officer, district
4. **Unified skip list** — two different hardcoded sets

---

## File Structure

- Modify: `infra/duckdb-server/response_middleware.py` — sync skip list, add source citation
- Modify: `infra/duckdb-server/percentile_middleware.py` — sync skip list
- Create: `infra/duckdb-server/freshness_middleware.py` — warn on stale data
- Modify: `infra/duckdb-server/mcp_server.py` — register freshness middleware, define shared skip list

---

## Task 1: Unify skip lists into a shared constant

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`
- Modify: `infra/duckdb-server/response_middleware.py`
- Modify: `infra/duckdb-server/percentile_middleware.py`

- [ ] **Step 1: Add shared constant to mcp_server.py**

After the existing constants block (after `_db_lock` around line 60), add:

```python
# Tools that should not be post-processed by formatting/percentile middleware.
# Discovery tools return schema metadata, not domain data.
MIDDLEWARE_SKIP_TOOLS = frozenset({
    "list_schemas",
    "list_tables",
    "describe_table",
    "data_catalog",
    "search_tools",
    "call_tool",
    "sql_admin",
    "suggest_explorations",
    "graph_health",
    "lake_health",
    "name_variants",
})
```

Note: `sql_query` is NOT in this list — its output SHOULD be formatted by OutputFormatterMiddleware (tabular results). But it IS skipped by PercentileMiddleware since arbitrary SQL doesn't map to an entity type.

- [ ] **Step 2: Update OutputFormatterMiddleware**

In `response_middleware.py`, change:
```python
_SKIP_TOOLS = {"list_schemas", "search_tools", "data_catalog", "sql_admin"}
```
To:
```python
from mcp_server import MIDDLEWARE_SKIP_TOOLS
_SKIP_TOOLS = MIDDLEWARE_SKIP_TOOLS
```

**IMPORTANT:** Check for circular imports. If `response_middleware.py` is imported by `mcp_server.py`, this creates a cycle. If so, define the constant in a separate `constants.py` file and import from there in both files.

- [ ] **Step 3: Update PercentileMiddleware**

In `percentile_middleware.py`, change the `_SKIP_TOOLS` set to:
```python
from mcp_server import MIDDLEWARE_SKIP_TOOLS
_SKIP_TOOLS = MIDDLEWARE_SKIP_TOOLS | {"sql_query", "text_search", "export_data"}
```

The percentile middleware adds `sql_query`, `text_search`, and `export_data` to the skip list because these tools don't return a single entity to look up percentiles for.

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/response_middleware.py infra/duckdb-server/percentile_middleware.py
git commit -m "refactor(middleware): unify skip lists into shared MIDDLEWARE_SKIP_TOOLS constant"
```

---

## Task 2: Add source citation middleware

Every tool response should cite which tables it queried — "Data from: lake.housing.hpd_violations (10.9M rows), lake.housing.hpd_complaints (16M rows)". This is core to the Common Ground brand: "show the receipts."

**Files:**
- Create: `infra/duckdb-server/citation_middleware.py`
- Modify: `infra/duckdb-server/mcp_server.py` — register it

- [ ] **Step 1: Create the citation middleware**

```python
"""
CitationMiddleware — appends data source citations to every tool response.

Detects which lake tables were queried by scanning the tool's SQL constants
(via a pre-built mapping of tool_name → table_names), then appends a
"Sources:" footer with table names and row counts from the cached catalog.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

# Map tool names to the lake tables they query.
# Built by scanning the SQL constants in mcp_server.py.
# Only include tools with known, fixed table dependencies.
TOOL_SOURCES = {
    "building_profile": ["housing.hpd_jurisdiction", "housing.hpd_violations", "housing.hpd_complaints"],
    "landlord_watchdog": ["housing.hpd_registration_contacts", "housing.hpd_violations", "housing.evictions", "housing.hpd_litigations"],
    "owner_violations": ["housing.hpd_violations", "housing.dob_ecb_violations"],
    "enforcement_web": ["housing.hpd_violations", "housing.dob_ecb_violations", "housing.fdny_violations", "city_government.oath_hearings"],
    "property_history": ["housing.acris_master", "housing.acris_parties", "housing.acris_legals"],
    "neighborhood_portrait": ["health.restaurant_inspections", "housing.hpd_jurisdiction", "social_services.n311_service_requests", "business.issued_licenses"],
    "safety_report": ["public_safety.nypd_arrests", "public_safety.nypd_complaints", "public_safety.nypd_shooting_incidents"],
    "school_report": ["education.demographics_2020", "education.ela_results", "education.math_results", "education.chronic_absenteeism", "education.school_safety"],
    "school_search": ["education.demographics_2020", "education.school_safety"],
    "cop_sheet": ["federal.nypd_ccrb_complaints", "federal.nypd_ccrb_officers_current", "federal.police_settlements_538", "federal.cl_nypd_cases_sdny"],
    "due_diligence": ["financial.nys_attorney_registrations", "financial.nys_re_brokers", "financial.nys_tax_warrants", "financial.nys_child_support_warrants"],
    "climate_risk": ["environment.heat_vulnerability", "environment.lead_service_lines", "environment.ll84_energy_2023", "environment.street_trees"],
    "money_trail": ["federal.nys_campaign_finance", "federal.fec_contributions", "city_government.campaign_contributions", "city_government.contract_awards"],
    "judge_profile": ["federal.cl_judges", "federal.cl_financial_disclosures"],
    "vital_records": ["city_government.death_certificates_1862_1948", "city_government.marriage_certificates_1866_1937", "city_government.marriage_licenses_1950_2017"],
    "entity_xray": ["business.nys_corporations", "housing.acris_parties", "city_government.campaign_contributions", "city_government.citywide_payroll"],
    "pay_to_play": ["city_government.campaign_contributions", "city_government.nys_lobbyist_registration", "city_government.contract_awards"],
}

# Tools that should NOT get citations (discovery/meta tools)
_SKIP = frozenset({
    "list_schemas", "list_tables", "describe_table", "data_catalog",
    "search_tools", "call_tool", "sql_admin", "suggest_explorations",
    "graph_health", "lake_health", "sql_query",
})


def _format_citation(tables: list[str], catalog: dict) -> str:
    """Build a 'Sources:' line with table names and row counts."""
    parts = []
    for t in tables:
        schema, table = t.split(".", 1) if "." in t else ("", t)
        row_count = None
        for entry in catalog.get("tables", []):
            if entry.get("schema") == schema and entry.get("table") == table:
                row_count = entry.get("rows")
                break
        if row_count and row_count > 0:
            if row_count >= 1_000_000:
                parts.append(f"{t} ({row_count / 1_000_000:.1f}M rows)")
            elif row_count >= 1_000:
                parts.append(f"{t} ({row_count / 1_000:.0f}K rows)")
            else:
                parts.append(f"{t} ({row_count:,} rows)")
        else:
            parts.append(t)
    return "Sources: " + ", ".join(parts)


class CitationMiddleware(Middleware):
    """Append data source citations to tool responses."""

    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP:
            return result

        sources = TOOL_SOURCES.get(tool_name)
        if not sources:
            return result

        try:
            lifespan = context.fastmcp_context.lifespan_context
            catalog = lifespan.get("catalog_json", {})

            citation = _format_citation(sources, catalog)

            original_text = ""
            if isinstance(result.content, list) and result.content:
                original_text = result.content[0].text
            elif isinstance(result.content, str):
                original_text = result.content

            new_text = original_text + "\n\n" + citation

            return ToolResult(
                content=new_text,
                structured_content=result.structured_content,
                meta=result.meta,
            )
        except Exception:
            return result
```

- [ ] **Step 2: Store catalog JSON in lifespan context**

In `mcp_server.py`, find where the catalog is built during startup (search for `"catalog"` in the lifespan function). After the catalog is cached, also store the JSON version:

```python
# After: ctx.lifespan_context["catalog"] = catalog
# Add:
import json
ctx.lifespan_context["catalog_json"] = {
    "tables": [{"schema": k.split(".")[0] if "." in k else "", "table": k.split(".")[-1], "rows": v.get("estimated_size", 0)} for k, v in catalog.items()]
}
```

**NOTE:** Check the actual catalog structure first — it may already be a dict of `{schema.table: {estimated_size, ...}}` or a different format. Adapt the conversion accordingly.

- [ ] **Step 3: Register the middleware**

In `mcp_server.py`, after the existing `mcp.add_middleware(...)` calls:

```python
from citation_middleware import CitationMiddleware
mcp.add_middleware(CitationMiddleware())
```

Insert it AFTER OutputFormatterMiddleware and BEFORE PostHogMiddleware so the citation is formatted text, not double-processed.

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/citation_middleware.py infra/duckdb-server/mcp_server.py
git commit -m "feat(middleware): add CitationMiddleware — appends data source citations to every response"
```

---

## Task 3: Add freshness warning middleware

Warn the LLM when queried data is stale (last pipeline run > 7 days ago). Uses `_pipeline_state` cursor data.

**Files:**
- Create: `infra/duckdb-server/freshness_middleware.py`
- Modify: `infra/duckdb-server/mcp_server.py` — register it

- [ ] **Step 1: Create the freshness middleware**

```python
"""
FreshnessMiddleware — warns when queried data is stale.

Checks _pipeline_state.last_run_at for the tables a tool queries.
If any table hasn't been refreshed in > 7 days, appends a warning.
"""

import time
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

# Reuse the same tool→table mapping from citation middleware
from citation_middleware import TOOL_SOURCES

STALE_THRESHOLD_DAYS = 7

_SKIP = frozenset({
    "list_schemas", "list_tables", "describe_table", "data_catalog",
    "search_tools", "call_tool", "sql_admin", "suggest_explorations",
    "graph_health", "lake_health", "sql_query", "vital_records",
})


class FreshnessMiddleware(Middleware):
    """Warn when data hasn't been refreshed recently."""

    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP:
            return result

        sources = TOOL_SOURCES.get(tool_name)
        if not sources:
            return result

        try:
            lifespan = context.fastmcp_context.lifespan_context
            pipeline_state = lifespan.get("pipeline_state", {})
            now = time.time()

            stale_tables = []
            for table_key in sources:
                ps = pipeline_state.get(table_key)
                if not ps or not ps.get("last_run_at"):
                    continue
                # Parse last_run_at timestamp
                try:
                    from datetime import datetime
                    last_run = datetime.fromisoformat(ps["last_run_at"].replace("Z", "+00:00"))
                    age_days = (now - last_run.timestamp()) / 86400
                    if age_days > STALE_THRESHOLD_DAYS:
                        stale_tables.append((table_key, int(age_days)))
                except (ValueError, TypeError):
                    pass

            if not stale_tables:
                return result

            warning_parts = [f"{t} ({d}d old)" for t, d in stale_tables]
            warning = f"Data freshness warning: {', '.join(warning_parts)} — last refreshed over {STALE_THRESHOLD_DAYS} days ago."

            original_text = ""
            if isinstance(result.content, list) and result.content:
                original_text = result.content[0].text
            elif isinstance(result.content, str):
                original_text = result.content

            new_text = original_text + "\n\n" + warning

            return ToolResult(
                content=new_text,
                structured_content=result.structured_content,
                meta=result.meta,
            )
        except Exception:
            return result
```

- [ ] **Step 2: Store pipeline_state in lifespan context**

In `mcp_server.py`, during the startup/lifespan, after querying `_pipeline_state` for the catalog endpoint, also store it in the lifespan context:

```python
# Query pipeline state and store for middleware use
pipeline_state = {}
try:
    _, ps_rows = _execute(conn, "SELECT dataset_name, last_updated_at, row_count, last_run_at FROM lake._pipeline_state")
    for psr in ps_rows:
        pipeline_state[psr[0]] = {"cursor": str(psr[1]), "rows_written": psr[2], "last_run_at": str(psr[3])}
except Exception:
    pass

# Add to lifespan context:
yield {
    ...existing keys...,
    "pipeline_state": pipeline_state,
}
```

**NOTE:** Check the actual lifespan yield structure. It may use dict assignment rather than yield. Adapt accordingly.

- [ ] **Step 3: Register the middleware**

In `mcp_server.py`, after CitationMiddleware:

```python
from freshness_middleware import FreshnessMiddleware
mcp.add_middleware(FreshnessMiddleware())
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/freshness_middleware.py infra/duckdb-server/mcp_server.py
git commit -m "feat(middleware): add FreshnessMiddleware — warns when data is stale (>7 days)"
```

---

## Task 4: Deploy and verify

- [ ] **Step 1: Deploy**

```bash
cd ~/Desktop/dagster-pipeline
scp -i ~/.ssh/id_ed25519_hetzner infra/duckdb-server/*.py fattie@178.156.228.119:/opt/common-ground/duckdb-server/
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && sudo docker compose build --no-cache duckdb-server && sudo docker compose up -d duckdb-server"
```

- [ ] **Step 2: Verify citations appear**

Call `building_profile("1000670001")` via MCP. Response should end with:
```
Sources: housing.hpd_jurisdiction (377K rows), housing.hpd_violations (10.9M rows), housing.hpd_complaints (16M rows)
```

- [ ] **Step 3: Verify freshness warnings**

If any table hasn't been refreshed in >7 days, the response should include:
```
Data freshness warning: housing.hpd_violations (12d old) — last refreshed over 7 days ago.
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/
git commit -m "feat(middleware): citation + freshness middleware deployed and verified"
```
