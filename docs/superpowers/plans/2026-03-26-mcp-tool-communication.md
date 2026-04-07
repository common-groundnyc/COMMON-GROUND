# MCP Tool Communication Overhaul

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the FastMCP server's tool descriptions, instructions, and always-visible set so LLMs reliably pick the right tool — even when the `instructions` field is silently dropped (confirmed Claude Code bug #23808).

**Architecture:** Move all routing logic from `INSTRUCTIONS` into individual tool descriptions. Reduce always-visible tools from 26 to ~10 entry points. Add `Annotated[str, Field(...)]` parameter descriptions to every tool. Add `tags` for semantic grouping. Restructure `INSTRUCTIONS` as a compact fallback.

**Tech Stack:** FastMCP 3.1.1, Python 3.12, Pydantic `Field` + `Annotated`

**Research basis:** March 2026 best practices — Capability Triangle framework, GitHub Copilot 40→13, Block 30+→2, Harness 130→11. Key article: "MCP Tool Design: Why Your AI Agent Is Failing" (dev.to, 2026-03-18).

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `infra/duckdb-server/mcp_server.py` | Modify | All changes — instructions, descriptions, params, always-visible, tags |

Single file, ~9826 lines. All changes are to constants and function signatures/docstrings. No logic changes. No new files.

---

### Task 1: Restructure INSTRUCTIONS as compact fallback

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2288-2338` (INSTRUCTIONS constant)

Since Claude Code drops `instructions`, this text is only a fallback for clients that DO inject it (Claude Desktop, VS Code Copilot). Keep it short — the real routing lives in tool descriptions (Task 3).

- [ ] **Step 1: Replace INSTRUCTIONS constant**

Replace lines 2288-2338 with:

```python
INSTRUCTIONS = """\
NYC open data lake — 294 tables, 12 schemas, 60M+ rows.

ROUTING — pick the FIRST match:
• BUILDING by BBL → building_profile, landlord_watchdog, building_story
• PERSON/COMPANY by name → entity_xray, person_crossref
• LANDLORD/OWNER → landlord_network, worst_landlords, llc_piercer
• NEIGHBORHOOD by ZIP → neighborhood_portrait, neighborhood_compare
• CRIME/SAFETY by precinct → safety_report
• CORRUPTION/INFLUENCE → pay_to_play, shell_detector, flipper_detector
• FIND DATA by keyword → data_catalog, then list_tables
• SPECIFIC SQL → sql_query (read-only, all tables in 'lake' database)

Too many tools? Use search_tools(query) to discover domain tools.
(Note: search_tools is a synthetic tool injected by BM25SearchTransform — not defined in server code.)
BBL: 10 digits = borough(1) + block(5) + lot(4). Example: 1000670001
ZIP: 5 digits. Manhattan 100xx, Brooklyn 112xx, Bronx 104xx, Queens 11xxx, SI 103xx.
SQL example: SELECT * FROM lake.housing.hpd_violations LIMIT 5
"""
```

- [ ] **Step 2: Verify the constant is syntactically valid**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline && python -c "exec(open('infra/duckdb-server/mcp_server.py').read().split('mcp = FastMCP')[0])"`
Expected: No syntax errors (exits cleanly)

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "refactor: compact INSTRUCTIONS to routing-only fallback

Claude Code bug #23808 drops instructions — real routing moves to tool descriptions."
```

---

### Task 2: Reduce ALWAYS_VISIBLE from 26 to 10

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:2344-2374` (ALWAYS_VISIBLE list)

Research consensus: LLMs reason well about ~10-15 tools. The BM25SearchTransform handles discovery for the rest. The 10 tools below are the entry points that cover every user intent category.

- [ ] **Step 1: Replace ALWAYS_VISIBLE list**

Replace lines 2344-2374 with:

```python
ALWAYS_VISIBLE = [
    # Data discovery (always needed)
    "sql_query",
    "data_catalog",
    "list_schemas",
    # Building-centric (most common user intent)
    "building_profile",
    "landlord_watchdog",
    # Person/entity investigation
    "entity_xray",
    "person_crossref",
    # Neighborhood/area
    "neighborhood_portrait",
    "safety_report",
    # Graph power tools
    "landlord_network",
]
```

The other 33 tools remain fully functional — BM25 surfaces them when the user's query matches. Example: "restaurant grades" triggers `restaurant_lookup`; "gentrification east village" triggers `gentrification_tracker`.

- [ ] **Step 2: Verify BM25 covers the removed tools**

This is a manual verification step. After deploying, test these queries against `search_tools`:
- "restaurant inspection" → should return `restaurant_lookup`
- "environmental racism" → should return `environmental_justice`
- "school performance" → should return `school_report`
- "property flip profit" → should return `flipper_detector`
- "LLC shell company" → should return `corporate_web` or `shell_detector`

If any fail, add that tool back to ALWAYS_VISIBLE or improve its docstring (Task 3).

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "refactor: reduce ALWAYS_VISIBLE from 26 to 10 entry points

BM25SearchTransform handles discovery for the other 33 tools.
Research: LLMs degrade past 15-20 visible tools."
```

---

### Task 3: Rewrite tool descriptions with routing guidance

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — docstrings on all 43 `@mcp.tool` functions

Since `instructions` is dropped by Claude Code, each tool's docstring is the ONLY text the LLM sees. Every description must follow the six-component pattern:

1. **Purpose** — what it does (1 sentence)
2. **Guidelines** — when to use it vs alternatives (1 sentence)
3. **Limitations** — what it can't do (only if non-obvious)
4. **Parameters** — input format with examples
5. **Examples** — concrete invocations
6. **Cross-references** — "See also: X for Y"

The always-visible tools (Task 2) carry extra routing weight — their descriptions must guide the LLM to the right domain tool.

- [ ] **Step 1: Rewrite the 10 always-visible tool descriptions**

These are the highest-impact rewrites. The LLM sees these on every request.

**sql_query:**
```python
@mcp.tool(annotations=READONLY)
def sql_query(sql: str, ctx: Context) -> ToolResult:
    """Execute a read-only SQL query against the NYC data lake (294 tables, 60M+ rows).
    Use this for custom queries when no domain tool fits. All tables in 'lake' database.
    Only SELECT/WITH/EXPLAIN/DESCRIBE allowed. Example: SELECT * FROM lake.housing.hpd_violations LIMIT 10.
    Start with data_catalog(keyword) or list_schemas() to discover tables."""
```

**data_catalog:**
```python
@mcp.tool(annotations=READONLY)
def data_catalog(ctx: Context, keyword: str = "") -> ToolResult:
    """Search the NYC data lake for tables matching a keyword — fuzzy matching, typos OK.
    START HERE if you don't know which tool to use. Searches table names, descriptions, and column names.
    Call with no keyword for a schema overview. Examples: 'eviction', 'restaurant', 'arrest', 'school', 'rat'.
    After finding a table, use describe_table(schema, table) for columns, then sql_query() to query it."""
```

**building_profile:**
```python
@mcp.tool(annotations=READONLY)
def building_profile(bbl: str, ctx: Context) -> ToolResult:
    """Quick building dossier by BBL — address, units, stories, plus violation and complaint counts.
    Use this for a fast overview. For deeper investigation, follow up with:
    - landlord_watchdog(bbl) for the owner's full portfolio and slumlord score
    - enforcement_web(bbl) for multi-agency enforcement actions
    - property_history(bbl) for ACRIS transaction chain since 1966
    - building_story(bbl) for historical narrative
    BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
```

**landlord_watchdog:**
```python
@mcp.tool(annotations=READONLY)
def landlord_watchdog(bbl: str, ctx: Context) -> ToolResult:
    """Investigate a landlord's FULL portfolio — all buildings, violations, complaints, evictions,
    enforcement flags (AEP, CONH, Underlying Conditions), and slumlord score.
    Use this when a user asks about a landlord, building owner, or tenant rights.
    For ownership NETWORK analysis (graph traversal, shell companies), use landlord_network(bbl).
    For LLC piercing, use llc_piercer(entity_name).
    BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001
    Returns actionable next steps: hotlines, tenant rights, legal aid contacts."""
```

**entity_xray:**
```python
@mcp.tool(annotations=READONLY)
def entity_xray(name: str, ctx: Context) -> ToolResult:
    """Full X-ray of a person or entity across ALL 23 NYC datasets — corporations, businesses,
    restaurants, campaign donations, expenditures, OATH hearings, ACRIS transactions, property,
    DOB permits, payroll, marriage records, attorney registrations, and more.
    USE THIS when someone asks about a person or company by name.
    Handles name variations — 'Barton Perlbinder' matches 'PERLBINDER, BARTON M'.
    For probabilistic cross-referencing with Splink matching, use person_crossref(name).
    For political influence specifically, use pay_to_play(name)."""
```

**person_crossref:**
```python
@mcp.tool(annotations=READONLY)
def person_crossref(name: str, ctx: Context) -> ToolResult:
    """Find every appearance of a person across the NYC data lake using Splink probabilistic matching.
    Matches the same person across 44 source tables even when name formats differ
    (e.g., 'Barton Perlbinder' matches 'PERLBINDER, BARTON M').
    Use this for thorough cross-referencing. For a quick entity scan, use entity_xray(name).
    Returns: all source tables, record counts, addresses, cities, zips, and cluster details."""
```

**neighborhood_portrait:**
```python
@mcp.tool(annotations=READONLY)
def neighborhood_portrait(zipcode: str, ctx: Context) -> ToolResult:
    """What makes a NYC neighborhood distinctive — cuisine fingerprint, building stock, noise level,
    business mix, and how it compares to the city. Use this for neighborhood questions by ZIP.
    For comparing multiple ZIPs side-by-side, use neighborhood_compare([zips]).
    For environmental justice analysis, use environmental_justice(zipcode).
    For gentrification tracking, use gentrification_tracker([zips]).
    ZIP: 5 digits. Example: 10003 (East Village), 11201 (Downtown Brooklyn)."""
```

**safety_report:**
```python
@mcp.tool(annotations=READONLY)
def safety_report(precinct: int, ctx: Context) -> ToolResult:
    """Comprehensive safety & policing report for an NYPD precinct (1-123).
    Crime trends, arrest demographics, enforcement intensity, shootings, summons,
    hate crimes, CCRB misconduct, Use of Force, and officer discipline.
    Use this for crime/safety/policing questions. Need a ZIP instead of precinct?
    Use neighborhood_portrait(zip) which includes safety context.
    Examples: safety_report(75) for East New York, safety_report(14) for Midtown South."""
```

**landlord_network:**
```python
@mcp.tool(annotations=READONLY)
def landlord_network(bbl: str, ctx: Context) -> ToolResult:
    """Discover the full ownership NETWORK around a building using DuckPGQ graph traversal.
    Finds the owner, ALL their other buildings, and aggregated violation stats across the portfolio.
    Use this for ownership graph analysis. For a simpler portfolio view, use landlord_watchdog(bbl).
    For hidden ownership clusters (WCC algorithm), use ownership_clusters().
    For worst landlords ranking, use worst_landlords().
    BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
```

**list_schemas:**
```python
@mcp.tool(annotations=READONLY)
def list_schemas(ctx: Context) -> str:
    """List all 12 schemas in the NYC data lake with table counts and total rows.
    Schemas: housing, public_safety, health, social_services, financial, environment,
    recreation, education, business, transportation, city_government, census.
    Use list_tables(schema) next to see individual tables in a schema."""
```

- [ ] **Step 2: Rewrite the remaining 33 BM25-discoverable tool descriptions**

Apply the same six-component pattern. Key principle: each description must be **BM25-searchable** — include the keywords users would naturally type. Focus on:

- `restaurant_lookup` — add "grade inspection health department violation"
- `environmental_justice` — add "pollution air quality asthma environmental racism EJ"
- `school_report` — add "school performance test scores enrollment DBN"
- `gentrification_tracker` — add "gentrification displacement rent housing pressure"
- `flipper_detector` — add "flip investor speculation profit property"
- `resource_finder` — add "food pantry SNAP benefits clinic shelter legal aid childcare"
- `area_snapshot` — add "nearby location lat lng spatial radius what's around"
- `commercial_vitality` — add "business licenses permits commercial economic activity"
- `pay_to_play` — add "corruption donation lobbying contract political influence"
- `corporate_web` — add "shell company LLC corporate network shared officers"
- `marriage_search` — add "marriage record bride groom certificate family"

**Full list of 33 BM25-discoverable tools** (grouped by tag for systematic rewrite):

**discovery:** `sql_admin`, `list_tables`, `describe_table`, `text_search`
**building:** `building_story`, `building_context`, `nyc_twins`, `block_timeline`
**housing:** `owner_violations`, `complaints_by_zip`
**investigation:** `llc_piercer`, `marriage_search`, `top_crossrefs`
**neighborhood:** `neighborhood_compare`, `area_snapshot`, `gentrification_tracker`
**environment:** `environmental_justice`
**services:** `resource_finder`, `school_report`, `restaurant_lookup`, `commercial_vitality`
**graph:** `ownership_graph`, `ownership_clusters`, `ownership_cliques`, `worst_landlords`, `transaction_network`, `corporate_web`, `shell_detector`, `contractor_network`
**finance:** `property_history`, `enforcement_web`, `flipper_detector`, `pay_to_play`

**Confusable tools — descriptions MUST differentiate these:**
- `building_profile` (quick stats) vs `building_story` (narrative history) vs `building_context` (era/contemporaries)
- `entity_xray` (23-source keyword scan) vs `person_crossref` (Splink probabilistic matching)
- `landlord_watchdog` (portfolio + slumlord score) vs `landlord_network` (graph traversal)
- `ownership_graph` (BFS from one BBL) vs `ownership_clusters` (WCC across all buildings) vs `ownership_cliques` (clustering coefficient)

For each tool, ensure the docstring answers: "If a user asks X, would this description's keywords match via BM25?"

**Note:** `search_tools` and `call_tool` are synthetic tools injected by `BM25SearchTransform` — they have no function definitions in the server file and cannot be annotated.

Pattern for BM25-discoverable tools:
```python
def example_tool(...) -> ToolResult:
    """[Purpose — 1 sentence with key domain terms for BM25 matching].
    [When to use — 1 sentence differentiating from similar tools].
    [Input format with example].
    See also: [related tool] for [different angle]."""
```

- [ ] **Step 3: Verify all 43 tools parse correctly**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -c "from mcp_server import mcp; print(f'{len(mcp._tool_manager._tools)} tools loaded')"`
Expected: `43 tools loaded` (verifies imports, type resolution, and FastMCP registration — not just syntax)

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "refactor: rewrite all 43 tool descriptions with routing guidance

Each tool now includes purpose, guidelines, cross-references, and BM25 keywords.
Compensates for Claude Code bug #23808 (instructions silently dropped)."
```

---

### Task 4: Add Annotated parameter descriptions

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:1-17` (imports)
- Modify: `infra/duckdb-server/mcp_server.py` — all 43 tool function signatures

Parameter descriptions appear in the JSON schema that every MCP client receives. Without them, the LLM sees `bbl: str` with no context. With them, it sees `bbl: str — "10-digit BBL: borough(1) + block(5) + lot(4). Example: 1000670001"`.

- [ ] **Step 1: Add imports**

Add after the existing imports block (after line 17, the `BM25SearchTransform` import):
```python
from typing import Annotated
from pydantic import Field
```

- [ ] **Step 2: Define reusable type aliases and update parameters**

Define reusable type aliases near the top (after imports, before constants):
```python
BBL = Annotated[str, Field(description="10-digit BBL: borough(1) + block(5) + lot(4). Example: 1000670001")]
ZIP = Annotated[str, Field(description="5-digit NYC ZIP code. Example: 10003, 11201, 10456")]
NAME = Annotated[str, Field(description="Person or company name. Fuzzy matched. Example: 'Barton Perlbinder', 'BLACKSTONE'")]
```

Then replace `bbl: str` with `bbl: BBL` in these functions:
- `building_profile`
- `owner_violations`
- `landlord_watchdog`
- `building_story`
- `nyc_twins`
- `block_timeline`
- `building_context`
- `landlord_network`
- `ownership_graph`
- `property_history`
- `enforcement_web`

And replace `zipcode: str` / `zip_code: str` with `zipcode: ZIP` / `zip_code: ZIP` in:
- `complaints_by_zip`
- `environmental_justice`
- `resource_finder`
- `neighborhood_portrait`
- `commercial_vitality`

- [ ] **Step 3: Add Field descriptions to other parameters**

Key parameters that need descriptions:

```python
# sql_query
def sql_query(sql: Annotated[str, Field(description="Read-only SQL. Tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations LIMIT 10")], ctx: Context) -> ToolResult:

# data_catalog
def data_catalog(ctx: Context, keyword: Annotated[str, Field(description="Search term — fuzzy matched against table names, descriptions, column names. Examples: 'eviction', 'restaurant', 'arrest'")] = "") -> ToolResult:

# neighborhood_compare
def neighborhood_compare(zip_codes: Annotated[list[str], Field(description="2-7 NYC ZIP codes to compare. Example: ['10003', '11201', '11215']")], ctx: Context) -> ToolResult:

# safety_report
def safety_report(precinct: Annotated[int, Field(description="NYPD precinct number 1-123. Example: 75 (East New York), 14 (Midtown South)", ge=1, le=123)], ctx: Context) -> ToolResult:

# entity_xray, person_crossref, transaction_network — use `name: NAME`
# IMPORTANT: llc_piercer, corporate_web, pay_to_play use `entity_name` (not `name`)!
# Use `entity_name: NAME` for those three.
name: NAME  # for entity_xray, person_crossref, transaction_network
entity_name: NAME  # for llc_piercer, corporate_web, pay_to_play

# restaurant_lookup
name: Annotated[str, Field(description="Restaurant name — fuzzy matched, typos OK. Example: 'Wo Hop', 'joes pizza'")]

# resource_finder
need: Annotated[str, Field(description="What you're looking for: 'food', 'health', 'childcare', 'youth', 'benefits', 'legal', 'shelter', 'wifi', 'senior', or 'all'")]

# area_snapshot
lat: Annotated[float, Field(description="Latitude within NYC (40.4-41.0). Example: 40.7128", ge=40.4, le=41.0)]
lng: Annotated[float, Field(description="Longitude within NYC (-74.3 to -73.6). Example: -74.0060", ge=-74.3, le=-73.6)]
radius_m: Annotated[int, Field(description="Search radius in meters (50-2000). Default: 500", ge=50, le=2000)] = 500

# flipper_detector
borough: Annotated[str, Field(description="Borough name or code 1-5. Empty for all. Example: 'Brooklyn' or '3'")] = ""
months: Annotated[int, Field(description="Max months between buy and sell. Default: 24", ge=1, le=120)] = 24
min_profit_pct: Annotated[int, Field(description="Min price increase percentage. Default: 20", ge=1, le=1000)] = 20

# school_report
dbn: Annotated[str, Field(description="School DBN: district(2) + borough letter(1) + number(3). Example: 02M001. Borough: M=Manhattan, X=Bronx, K=Brooklyn, Q=Queens, R=Staten Island")]

# worst_landlords
top_n: Annotated[int, Field(description="Number of results. Default: 25", ge=1, le=100)] = 25

# text_search
corpus: Annotated[str, Field(description="Which data to search: 'financial' (CFPB complaints), 'restaurants' (inspection violations), or 'all' (both)")] = "all"

# marriage_search
surname: Annotated[str, Field(description="Last name to search. Matches both bride and groom. Example: 'SMITH'")]
first_name: Annotated[str, Field(description="Optional first name filter. Example: 'JOHN'")] = ""

# gentrification_tracker
zip_codes: Annotated[list[str], Field(description="1-5 NYC ZIP codes to track. Example: ['10009', '11216']")]

# ownership_graph — ALSO normalize ctx: Context = None → ctx: Context (like all other tools)
depth: Annotated[int, Field(description="Hops through ownership network (1-6). 1 = direct shared owners, 2+ = indirect. Default: 2", ge=1, le=6)] = 2

# marriage_search — ALSO normalize ctx: Context = None → ctx: Context (like all other tools)

# ownership_clusters
min_buildings: Annotated[int, Field(description="Min buildings per cluster. Default: 5", ge=2, le=100)] = 5

# ownership_cliques
top_n: Annotated[int, Field(description="Number of results. Default: 20", ge=1, le=100)] = 20

# contractor_network
name_or_license: Annotated[str, Field(description="Contractor name or license number. Example: 'NACHMAN PLUMBING' or '0042962'")]
```

- [ ] **Step 4: Verify syntax**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -c "from mcp_server import mcp; print(f'{len(mcp._tool_manager._tools)} tools loaded')"`
Expected: `43 tools loaded` (verifies imports, type resolution, and FastMCP registration — not just syntax)

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add Annotated[..., Field(...)] parameter descriptions to all 43 tools

LLMs now see input format, constraints, and examples in the JSON schema."
```

---

### Task 5: Add tags for semantic grouping

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — all 43 `@mcp.tool()` decorators

Tags enable future per-session visibility and help organize tools logically. They don't change current behavior but future-proof for FastMCP 3.x features like `ctx.enable_components(tags={"premium"})`.

- [ ] **Step 1: Define tag groups and apply to all tools**

| Tag | Tools |
|-----|-------|
| `discovery` | `sql_query`, `sql_admin`, `list_schemas`, `list_tables`, `describe_table`, `data_catalog`, `text_search` |
| `building` | `building_profile`, `building_story`, `building_context`, `nyc_twins`, `block_timeline` |
| `housing` | `landlord_watchdog`, `owner_violations`, `complaints_by_zip` |
| `investigation` | `entity_xray`, `person_crossref`, `top_crossrefs`, `llc_piercer`, `marriage_search` |
| `neighborhood` | `neighborhood_portrait`, `neighborhood_compare`, `area_snapshot`, `gentrification_tracker` |
| `safety` | `safety_report` |
| `environment` | `environmental_justice` |
| `services` | `resource_finder`, `school_report`, `restaurant_lookup`, `commercial_vitality` |
| `graph` | `landlord_network`, `ownership_graph`, `ownership_clusters`, `ownership_cliques`, `worst_landlords`, `transaction_network`, `corporate_web`, `shell_detector`, `contractor_network` |
| `finance` | `property_history`, `enforcement_web`, `flipper_detector`, `pay_to_play` |

Apply like:
```python
@mcp.tool(annotations=READONLY, tags={"building", "housing"})
def building_profile(bbl: BBL, ctx: Context) -> ToolResult:
```

- [ ] **Step 2: Verify syntax**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -c "from mcp_server import mcp; print(f'{len(mcp._tool_manager._tools)} tools loaded')"`
Expected: `43 tools loaded` (verifies imports, type resolution, and FastMCP registration — not just syntax)

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add semantic tags to all 43 tools

Tags: discovery, building, housing, investigation, neighborhood, safety,
environment, services, graph, finance. Enables future per-session visibility."
```

---

### Task 6: Deploy and validate

**Files:**
- No file changes — deployment and testing only

- [ ] **Step 1: Build the Docker image**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server"
```
Expected: Build succeeds

- [ ] **Step 2: Deploy**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d duckdb-server"
```
Expected: Container starts, logs show "Catalog cached: N tables across 12 schemas"

- [ ] **Step 3: Validate tool list via MCP**

Connect to `https://mcp.common-ground.nyc/mcp` and verify:
- `list_tools` returns exactly 10 always-visible tools + `search_tools` + `call_tool` (BM25 synthetic tools)
- `search_tools("restaurant inspection")` returns `restaurant_lookup`
- `search_tools("school performance")` returns `school_report`
- `search_tools("gentrification displacement")` returns `gentrification_tracker`
- `search_tools("shell company LLC")` returns `corporate_web` or `shell_detector`

- [ ] **Step 4: Validate parameter schemas**

Call `list_tools` and inspect the JSON schema for `building_profile` — should include:
```json
{"bbl": {"type": "string", "description": "10-digit BBL: borough(1) + block(5) + lot(4). Example: 1000670001"}}
```

- [ ] **Step 5: End-to-end test with Claude Code**

In Claude Code, with the MCP server connected, test these natural language queries:
1. "Tell me about the building at 1000670001" → should pick `building_profile`
2. "Who is Barton Perlbinder?" → should pick `entity_xray`
3. "Is 10009 gentrifying?" → should pick `gentrification_tracker` (via BM25)
4. "What restaurants are near me in 10003?" → should pick `restaurant_lookup` (via BM25)
5. "Show me crime stats for precinct 75" → should pick `safety_report`

- [ ] **Step 6: If any BM25 tests fail, add tool back to ALWAYS_VISIBLE and redeploy**

If a critical tool is not surfaced by BM25 for its expected query, add it to ALWAYS_VISIBLE (1-line change), rebuild, and redeploy. Do not proceed past this task with broken discovery.

---

## Token Impact Estimate

| Metric | Before | After |
|--------|--------|-------|
| Always-visible tools | 26 | 10 |
| Tool schema tokens (est.) | ~15-20K | ~5-8K |
| Parameter descriptions | none | all 43 tools |
| Routing guidance in descriptions | minimal | full cross-references |
| `instructions` dependency | critical | fallback only |

## Risk Mitigation

- **BM25 misses a critical tool?** Add it back to ALWAYS_VISIBLE. The list is a constant — 1-line change.
- **Descriptions too long?** BM25SearchTransform strips `outputSchema` from search results to save tokens. Tool descriptions themselves are compact (~100 words each).
- **Breaking change?** Zero logic changes. Only docstrings, decorators, and constants. Rollback = revert one commit.
