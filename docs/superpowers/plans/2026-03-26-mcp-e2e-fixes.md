# MCP End-to-End Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 6 bugs found during end-to-end MCP server testing — stale graph cache, unreachable tool, hidden schema noise, empty neighborhood portrait, noisy catalog, and broken exploration queries.

**Architecture:** Most fixes are in `mcp_server.py`. The root cause of P0 bugs (worst_landlords wrong data, suggest_explorations unreachable) is a **stale graph cache** on the server — the parquet cache has name-based owner_ids from an old rebuild, but the code now builds registrationid-based owner_ids. Invalidating the cache fixes worst_landlords. The suggest_explorations visibility issue is likely a tool registration ordering problem with BM25SearchTransform. The remaining fixes are filtering (hide staging schemas) and SQL corrections.

**Tech Stack:** FastMCP 3.1.1, DuckDB, Python 3.12, SSH to Hetzner

---

## Root Cause Analysis

| Bug | Root Cause |
|-----|-----------|
| `worst_landlords` shows 61K buildings per owner | **Stale graph cache**: parquet files have `owner_id = owner_name` (name-based, old build), code expects `owner_id = registrationid`. All buildings sharing an owner name get merged. |
| `suggest_explorations` not discoverable | **Tool registration order**: BM25SearchTransform is configured at `FastMCP()` construction (line 2453), but `suggest_explorations` is defined later via `@mcp.tool`. The transform snapshots the tool list and may not see late-registered tools. |
| Staging/test schemas visible | **No filter**: `list_schemas` and `data_catalog` query all schemas without excluding `*_staging` and `test_*` patterns. |
| `neighborhood_portrait` empty/crashes | **OOM on heavy queries**: The tool runs 8+ sub-queries (cuisine, crime, building stock, etc.) against large tables without memory guards. For some ZIPs it OOMs and kills the container. |
| `data_catalog` noisy results | **No schema filter + equal scoring**: column matches in test/staging tables score the same as real table matches. |
| `suggest_explorations` queries fail | **Wrong column names**: 3 of 5 exploration queries use wrong column names (violationstatus→status, no bbl in acris_master, no current_entity_status in nys_corporations). Already partially fixed but need completion. |

---

## File Structure

| File | Changes |
|------|---------|
| `infra/duckdb-server/mcp_server.py` | All 6 fixes (schema filter, tool order, exploration SQL, portrait memory guard) |
| Server-side | Invalidate graph cache (delete parquet files, restart container) |

---

## Task 1: Invalidate stale graph cache on server (P0)

**Priority:** Highest — fixes worst_landlords data corruption.

**Files:** Server-side only (SSH commands)

- [ ] **Step 1: Research — verify the cache is the problem**

```bash
ssh hetzner "docker exec common-ground-duckdb-server-1 python3 -c \"
import duckdb
db = duckdb.connect()
db.execute('CREATE TABLE t AS SELECT * FROM read_parquet(\\\"/data/common-ground/graph_cache/graph_owners.parquet\\\") LIMIT 3')
print(db.execute('SELECT * FROM t').fetchall())
\""
```

Expected: `owner_id` values are owner NAMES (strings like "TONY HOLDING LLC"), not registrationid numbers. This confirms the cache is stale.

- [ ] **Step 2: Delete the stale cache**

```bash
ssh hetzner "sudo rm -rf /mnt/data/common-ground/graph_cache/_built_at.txt"
```

Removing `_built_at.txt` makes `_graph_cache_fresh()` return False, forcing a full rebuild on next restart.

- [ ] **Step 3: Restart the container to trigger rebuild**

```bash
ssh hetzner "cd /opt/common-ground && sudo docker compose restart duckdb-server"
```

The rebuild takes 2-5 minutes. Watch logs:

```bash
ssh hetzner "sleep 10 && docker logs common-ground-duckdb-server-1 2>&1 | grep -i 'graph\|owner\|cache'"
```

Expected: "Building graph tables from lake (no cache or stale)..." followed by owner/building/violation counts with registrationid-based owner_ids.

- [ ] **Step 4: Verify worst_landlords returns sane data**

After rebuild completes, test via MCP:

```bash
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"worst_landlords","arguments":{"borough":"Brooklyn","top_n":3}}}'
```

Expected: Top landlords with <1000 buildings each (not 61K). Building counts should be realistic.

---

## Task 2: Fix suggest_explorations tool visibility (P0)

**Priority:** High — tool exists but LLMs can't find it.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Research — BM25SearchTransform tool registration timing**

Search with Exa:
1. `fastmcp BM25SearchTransform always_visible "not found" 2026` — known issues
2. `fastmcp transforms tool registration order "late" 2026` — tool added after transform

The hypothesis: BM25SearchTransform is applied at `FastMCP()` construction with the `transforms=[]` param, but `@mcp.tool` decorators run after construction. The transform may snapshot the tool list at construction time, missing later-registered tools.

- [ ] **Step 2: Check if moving ALWAYS_VISIBLE after tool definitions helps**

Read the FastMCP source to understand when transforms snapshot the tool list. If transforms are lazy (build index on first search), the tool should be found. If they snapshot at construction, we need a different approach.

Alternative fix: Move the `FastMCP()` construction to AFTER all `@mcp.tool` definitions. This is a big change since decorators reference `mcp`.

Simpler alternative: The `always_visible` list contains tool names as strings. If BM25SearchTransform looks up tools by name at query time (not at construction), the tool should be visible. The issue might be that Claude Code's tool listing uses a different MCP method than `search_tools`.

- [ ] **Step 3: Test if the tool is callable directly (not via search)**

From Claude Code, try calling it directly:

```
mcp__duckdb__suggest_explorations()
```

If this works, the tool IS registered but just not in the deferred tools list. This means the BM25SearchTransform correctly hides it from `tools/list` but it should be in `always_visible`.

- [ ] **Step 4: Fix by verifying always_visible spelling matches tool name exactly**

Read the current ALWAYS_VISIBLE list and the tool's registered name. The tool is defined as:

```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
def suggest_explorations(ctx: Context) -> str:
```

Its name will be `"suggest_explorations"`. Verify this matches the ALWAYS_VISIBLE entry exactly (no typos, no extra spaces).

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import ALWAYS_VISIBLE
print('suggest_explorations' in ALWAYS_VISIBLE)
print(ALWAYS_VISIBLE)
"
```

If it's there, the issue is in BM25SearchTransform. File a workaround: add the tool to the `instructions` with explicit mention that it exists, and document the BM25 bug.

- [ ] **Step 5: Workaround — register suggest_explorations before BM25SearchTransform**

If the transform snapshots at construction, the simplest fix is to define `suggest_explorations` as a regular function and register it with `mcp.add_tool()` AFTER `FastMCP()` construction but BEFORE any requests come in:

```python
# After mcp = FastMCP(...)
# Manually register the tool so BM25SearchTransform sees it

def _suggest_explorations(ctx: Context) -> str:
    """..."""
    ...

mcp.add_tool(_suggest_explorations, name="suggest_explorations", annotations=READONLY, tags={"discovery"})
```

But this may not help if the transform is lazy. The real debugging needs to happen on a running server.

- [ ] **Step 6: Commit whatever fix works**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: make suggest_explorations discoverable via BM25SearchTransform"
```

---

## Task 3: Filter staging and test schemas from discovery (P1)

**Priority:** High — staging/test schemas confuse LLMs and pollute results.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add a hidden schemas constant**

After the `SCHEMA_DESCRIPTIONS` dict, add:

```python
_HIDDEN_SCHEMAS = frozenset({
    s for s in ("business_staging", "city_government_staging", "education_staging",
                "environment_staging", "federal_staging", "financial_staging",
                "health_staging", "housing_staging", "public_safety_staging",
                "recreation_staging", "social_services_staging", "transportation_staging",
                "test_curl", "test_direct", "test_pure")
})
```

- [ ] **Step 2: Filter `list_schemas` output**

Find the `list_schemas` function. Add a filter to exclude hidden schemas:

```python
for schema in sorted(catalog):
    if schema in _HIDDEN_SCHEMAS:
        continue
    ...
```

- [ ] **Step 3: Filter `data_catalog` results**

The `data_catalog` tool uses `DATA_CATALOG_SQL` to search. The SQL returns a `schema_name` column. Add a post-filter:

After `cols, rows = _execute(db, DATA_CATALOG_SQL, [kw, kw, kw])`, add:

```python
    schema_idx = cols.index("schema_name")
    rows = [r for r in rows if r[schema_idx] not in _HIDDEN_SCHEMAS]
```

Also filter the no-keyword overview:

```python
    if not keyword.strip():
        catalog = ctx.lifespan_context["catalog"]
        lines = []
        for schema in sorted(catalog):
            if schema in _HIDDEN_SCHEMAS:
                continue
            ...
```

- [ ] **Step 4: Verify**

```bash
# Test list_schemas doesn't show staging
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_schemas","arguments":{}}}' | grep staging
```

Expected: No output (staging schemas filtered out).

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: hide staging and test schemas from list_schemas and data_catalog"
```

---

## Task 4: Fix neighborhood_portrait OOM crash (P1)

**Priority:** High — tool crashes the entire container for some ZIPs.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Research — which sub-query causes the OOM**

The portrait runs 8+ sub-queries via `_safe_query()`. One of them is scanning a massive table without a LIMIT or proper index. The most likely culprits are:

1. 311 complaint counts by ZIP (30M+ rows in n311_service_requests)
2. Crime stats by ZIP (10M+ rows in public_safety tables)
3. Building stock counts (hpd_jurisdiction, large table)

Find all `PORTRAIT_*_SQL` constants:

```bash
grep 'PORTRAIT_.*_SQL' ~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py | head -20
```

- [ ] **Step 2: Add a timeout wrapper for portrait sub-queries**

Replace `_safe_query` calls in `neighborhood_portrait` with a version that sets a per-query timeout:

```python
def _safe_query_timeout(db, sql, params=None, timeout_sec=10):
    """Execute SQL with timeout, return (cols, rows) or ([], []) on failure/timeout."""
    try:
        with _db_lock:
            db.execute(f"SET statement_timeout='{timeout_sec}s'")
            try:
                return _execute(db, sql, params)
            finally:
                db.execute("SET statement_timeout='0'")  # reset
    except Exception:
        return [], []
```

Or simpler: add `LIMIT 1000` to each portrait sub-query to cap row scanning.

- [ ] **Step 3: Add memory-safe LIMITs to portrait SQL constants**

Find each `PORTRAIT_*_SQL` constant and ensure it has a LIMIT clause. The portrait only needs top-N results for each section (top 8 cuisines, top 5 crime types, etc.), so LIMIT is natural.

- [ ] **Step 4: Test with the problem ZIP**

```bash
# Test via MCP
mcp__duckdb__neighborhood_portrait(zipcode="10466")
```

Expected: Returns data without crashing. May have sparse sections but should not OOM.

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: add memory guards to neighborhood_portrait sub-queries

Prevents OOM crash on heavy ZIPs by adding LIMIT to portrait SQL."
```

---

## Task 5: Fix suggest_explorations SQL queries (P2)

**Priority:** Medium — 3 of 5 queries fail. Already partially fixed but ACRIS flip query still returns no data.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Fix the ACRIS property flips query**

The current query joins `acris_master` to `acris_pp_legals` to get BBL but returns no data. The issue is likely that `doc_type` values don't match or the amount filter is too strict.

Test the query parts:

```sql
-- Check what doc_types exist
SELECT doc_type, COUNT(*) FROM lake.housing.acris_master
WHERE TRY_CAST(document_amt AS DOUBLE) > 100000
GROUP BY doc_type ORDER BY COUNT(*) DESC LIMIT 10
```

If `DEED`/`DEEDO` don't exist, find the correct doc_type values.

- [ ] **Step 2: Replace with a simpler working query**

If the join is too complex, replace with a simpler approach using the graph tables or a known-working pattern from the existing `flipper_detector` tool:

```bash
grep -A 30 "def flipper_detector" ~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py
```

Copy the SQL pattern from `flipper_detector` and simplify it for the exploration highlight.

- [ ] **Step 3: Verify all 5 exploration queries work**

After rebuilding the graph cache (Task 1) and fixing the SQL, test:

```bash
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"suggest_explorations","arguments":{}}}' | grep "query failed"
```

Expected: No "query failed" messages.

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: correct exploration SQL queries for ACRIS flips and graph tables"
```

---

## Task 6: Deploy and full E2E validation

**Files:** Server-side deployment

- [ ] **Step 1: Deploy all code changes**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' --exclude '*.md' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Run full E2E test suite**

Test each tool that was broken:

| Tool | Test | Expected |
|------|------|----------|
| `list_schemas` | Call it | No staging/test schemas |
| `data_catalog("restaurant")` | Search | No test_curl/test_pure results |
| `worst_landlords(borough="Brooklyn", top_n=3)` | Call it | <1000 buildings per owner |
| `suggest_explorations` | Call it | 5 highlights, no "query failed" |
| `neighborhood_portrait(zipcode="10003")` | Call it | Returns data, no crash |
| `building_profile(bbl="1000670001")` | Call it | Full profile with violations |

- [ ] **Step 3: Check container stability**

```bash
ssh hetzner "docker ps --filter name=duckdb-server --format '{{.Names}} {{.Status}}'"
```

Expected: Container up for >5 minutes without restarting.

---

## Execution Order & Dependencies

```
Task 1 (invalidate cache)     ← Server-side, do FIRST — fixes data for Task 5 too
Task 2 (suggest_explorations) ← Investigate after restart, may be fixed by Task 1
Task 3 (filter schemas)       ← Independent, code change only
Task 4 (portrait OOM)         ← Independent, needs investigation of SQL constants
Task 5 (exploration SQL)      ← Depends on Task 1 (fresh graph cache)
Task 6 (deploy + validate)    ← After all code changes
```

Tasks 1 is the critical first step. Tasks 2-5 can be worked in parallel after Task 1. Task 6 deploys everything.

---

## Verification Checklist

- [ ] `worst_landlords` shows realistic building counts (<1000 per owner)
- [ ] `suggest_explorations` is callable from Claude Code
- [ ] `list_schemas` shows only 12 real schemas (no staging/test)
- [ ] `data_catalog` results exclude staging/test tables
- [ ] `neighborhood_portrait` returns data without crashing
- [ ] All 5 exploration queries succeed (no "query failed")
- [ ] Container stable for 5+ minutes after all tests
