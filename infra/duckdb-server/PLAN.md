# FastMCP Server Build Plan

> Reference: `FASTMCP-RESEARCH.md` in this directory.
> Phase 1 research completed 2026-03-10.

---

## Phase 1: Foundation — Core Server + Response Helpers

**Goal:** Replace current `server.py`-only setup with FastMCP server running alongside it. Get `sql_query`, `list_schemas`, `list_tables`, `describe_table` working with ToolResult.

### Current State

- `server.py` runs DuckDB httpserver on :9999 + DuckDB UI on :4213
- DuckLake attached as `lake` with PostgreSQL catalog
- 294 tables across 12 schemas, 61 spatial views, 3 table macros
- Extensions loaded: ducklake, postgres_scanner, spatial, fts, vss, httpfs, json, parquet
- Macros already exist: `building_profile(bbl)`, `complaints_by_zip(zip)`, `owner_violations(name)`
- Dockerfile installs duckdb==1.4.4 only (needs upgrade to 1.5.0 + fastmcp)

### Architecture Decision

FastMCP replaces the DuckDB UI on port 4213. The httpserver API on :9999 stays for backward compat.

- `server.py` → keeps httpserver on :9999 (unchanged except DuckDB version bump)
- `mcp_server.py` → new FastMCP server on :4213
- `start.sh` → runs both processes
- Each process gets its own DuckDB connection (both attach same DuckLake catalog)

### Verified Imports (FastMCP 3.1.0)

```python
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
```

### Verified Patterns

- **Sync tools work**: FastMCP wraps sync functions in thread executor automatically. DuckDB is blocking/CPU-bound, so sync tools are correct.
- **Lifespan context access**: `ctx.lifespan_context["db"]` (direct dict access, NOT `ctx.request_context.lifespan_context`)
- **Transport**: `mcp.run(transport="streamable-http", host="0.0.0.0", port=4213)`
- **Stateless mode**: Set env var `FASTMCP_STATELESS_HTTP=true` in Dockerfile/docker-compose
- **Tool descriptions**: Docstring is used automatically; `description=` param overrides it
- **Return types**: Plain string auto-converts to TextContent. Return ToolResult for full control.

### DuckDB Safety (from research)

- **Read-only**: `ATTACH ... (READ_ONLY TRUE)` on the DuckLake catalog
- **Lock config**: `SET lock_configuration = true` after setup to prevent LLM from changing settings
- **No native query timeout**: Use `conn.interrupt()` from a timer thread (30s default)
- **Thread safety**: Single DuckDB connection is NOT thread-safe for concurrent queries. Use asyncio.Lock since FastMCP dispatches sync tools to thread pool. Each tool call will acquire lock before executing.
- **Result limiting**: Use `cursor.fetchmany(N)` instead of wrapping in subquery (avoids CTE/UNION edge cases)
- **FTS + ATTACH bug**: FTS indexes on attached DB tables may fail with "table 'terms' does not exist". Workaround: create local copies or views in default database and index those.

---

### Task 1.1: Create `mcp_server.py` — Server Scaffold

Write the file with these sections:

```python
# === IMPORTS ===
import os
import time
import threading
import duckdb
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

# === CONSTANTS ===
MAX_LLM_ROWS = 20          # Max rows shown to LLM in text table
MAX_STRUCTURED_ROWS = 500   # Max rows in structured_content
MAX_QUERY_ROWS = 1000       # Hard cap on fetchmany
QUERY_TIMEOUT_S = 30        # Seconds before query is interrupted

READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)

# === RESPONSE HELPERS ===
# format_text_table(cols, rows, max_rows) → pipe-delimited text table
# make_result(summary, cols, rows, meta_extra) → ToolResult
# make_error(message) → ToolResult with is_error=True

# === DB HELPERS ===
# execute_safe(conn, sql, params, timeout) → (cols, rows) with timeout + lock
# validate_sql(sql) → raises ToolError if not SELECT/WITH/EXPLAIN

# === LIFESPAN ===
# Connect DuckDB, attach DuckLake (READ_ONLY), load extensions
# SET lock_configuration = true
# Cache table catalog (schema → tables → row counts)
# yield {"db": conn, "lock": asyncio.Lock(), "catalog": catalog_cache}

# === INSTRUCTIONS ===
INSTRUCTIONS = """..."""

# === TOOLS ===
# sql_query, list_schemas, list_tables, describe_table

# === ENTRYPOINT ===
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=4213)
```

**Implementation details:**

**Lifespan:**
```python
import asyncio

@lifespan
async def app_lifespan(server):
    conn = duckdb.connect()

    # Install and load extensions
    for ext in ["ducklake", "postgres", "spatial", "fts", "vss", "httpfs"]:
        conn.execute(f"INSTALL {ext}; LOAD {ext};")

    # Attach DuckLake catalog (READ_ONLY)
    pg_pass = os.environ["DAGSTER_PG_PASSWORD"]
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres'
        AS lake (DATA_PATH '/data/common-ground/data/', READ_ONLY TRUE)
    """)

    # Lock configuration to prevent LLM from changing settings via sql_query
    conn.execute("SET lock_configuration = true")

    # Build catalog cache at startup
    catalog = _build_catalog_cache(conn)

    lock = asyncio.Lock()

    try:
        yield {"db": conn, "lock": lock, "catalog": catalog}
    finally:
        conn.close()
```

**DB helper with timeout + lock:**
```python
def _execute_raw(conn, sql, params=None, timeout=QUERY_TIMEOUT_S):
    """Execute SQL with timeout. Runs in thread (called from sync tool via FastMCP)."""
    result_holder = [None]
    error_holder = [None]

    def run():
        try:
            if params:
                cur = conn.execute(sql, params)
            else:
                cur = conn.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(MAX_QUERY_ROWS)
            result_holder[0] = (cols, rows)
        except Exception as e:
            error_holder[0] = e

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        conn.interrupt()
        thread.join(timeout=5)
        raise ToolError(f"Query timed out after {timeout}s")

    if error_holder[0]:
        raise ToolError(str(error_holder[0]))

    return result_holder[0]
```

Wait — there's a problem with this approach. FastMCP already dispatches sync tools to a thread pool. If we ALSO spawn a thread inside the tool for timeout, we have nested threading. Simpler approach:

```python
def _execute_raw(conn, sql, params=None):
    """Execute SQL. Called from sync tool (FastMCP runs in thread pool)."""
    try:
        if params:
            cur = conn.execute(sql, params)
        else:
            cur = conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(MAX_QUERY_ROWS)
        return (cols, rows)
    except duckdb.Error as e:
        raise ToolError(f"SQL error: {e}")
```

Skip timeout for Phase 1. Add it in Phase 6 (hardening). READ_ONLY + lock_configuration already prevent abuse.

**Thread safety:** Since FastMCP runs sync tools in a thread pool, we need to serialize DuckDB access. Use a threading.Lock (not asyncio.Lock, since the tool runs in a thread):

```python
_db_lock = threading.Lock()

def _execute_safe(conn, sql, params=None):
    with _db_lock:
        return _execute_raw(conn, sql, params)
```

Put `_db_lock` in lifespan context or as module-level. Module-level is simpler since there's only one server process.

**SQL validation:**
```python
import re

_UNSAFE_PATTERN = re.compile(
    r'^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY)\b',
    re.IGNORECASE
)

def _validate_readonly_sql(sql: str):
    """Reject obviously dangerous SQL. READ_ONLY DuckLake is the real guard."""
    stripped = sql.strip().rstrip(';')
    if _UNSAFE_PATTERN.match(stripped):
        raise ToolError("Only SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, and PRAGMA queries are allowed.")
```

This is belt-and-suspenders. The real protection is `READ_ONLY TRUE` on the DuckLake ATTACH. Even if the regex is bypassed, DuckDB will reject writes.

---

### Task 1.2: Response Helpers

```python
def format_text_table(cols, rows, max_rows=MAX_LLM_ROWS):
    """Format query results as a compact pipe-delimited text table."""
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    # Calculate column widths
    str_rows = [[str(v) if v is not None else "" for v in row] for row in rows[:max_rows]]
    widths = [max(len(c), max((len(r[i]) for r in str_rows), default=0)) for i, c in enumerate(cols)]
    # Cap individual column width
    widths = [min(w, 40) for w in widths]

    def fmt_row(values):
        return " | ".join(str(v)[:40].ljust(w) for v, w in zip(values, widths))

    lines = [fmt_row(cols), "-+-".join("-" * w for w in widths)]
    for row in str_rows:
        lines.append(fmt_row(row))

    total = len(rows)
    if total > max_rows:
        lines.append(f"({max_rows} of {total} rows shown)")

    return "\n".join(lines)


def make_result(summary, cols, rows, meta_extra=None):
    """Build ToolResult: text table for LLM, structured dict list for clients."""
    text = summary
    if cols and rows:
        text += "\n\n" + format_text_table(cols, rows)

    structured = [dict(zip(cols, row)) for row in rows[:MAX_STRUCTURED_ROWS]] if cols and rows else []

    meta = {"total_rows": len(rows)}
    if meta_extra:
        meta.update(meta_extra)

    return ToolResult(content=text, structured_content=structured, meta=meta)


def make_error(message):
    """Return a ToolResult marked as error."""
    return ToolResult(content=message, is_error=True)
```

---

### Task 1.3: Power Tools

**Tool 1: `sql_query`**
```python
@mcp.tool(annotations=READONLY)
def sql_query(sql: str, ctx: Context) -> ToolResult:
    """Execute a read-only SQL query against the NYC data lake.
    The database is 'lake' with schemas: housing, public_safety, health,
    social_services, financial, environment, recreation, education,
    business, transportation, city_government, census.
    Example: SELECT * FROM lake.housing.hpd_violations LIMIT 10"""
    _validate_readonly_sql(sql)
    db = ctx.lifespan_context["db"]
    cols, rows = _execute_safe(db, sql.strip().rstrip(';'))
    return make_result(f"Query returned {len(rows)} rows.", cols, rows)
```

**Tool 2: `list_schemas`**
```python
@mcp.tool(annotations=READONLY)
def list_schemas(ctx: Context) -> str:
    """List all schemas in the NYC data lake."""
    catalog = ctx.lifespan_context["catalog"]
    lines = []
    for schema, tables in sorted(catalog.items()):
        total_rows = sum(t["row_count"] for t in tables.values())
        lines.append(f"{schema}: {len(tables)} tables, ~{total_rows:,} rows")
    return "\n".join(lines)
```

**Tool 3: `list_tables`**
```python
@mcp.tool(annotations=READONLY)
def list_tables(schema: str, ctx: Context) -> ToolResult:
    """List tables in a schema with row counts and column counts."""
    catalog = ctx.lifespan_context["catalog"]
    if schema not in catalog:
        raise ToolError(f"Schema '{schema}' not found. Available: {', '.join(sorted(catalog.keys()))}")
    tables = catalog[schema]
    cols = ["table_name", "row_count", "column_count"]
    rows = [(name, t["row_count"], t["column_count"]) for name, t in sorted(tables.items())]
    return make_result(f"Schema '{schema}': {len(tables)} tables.", cols, rows)
```

**Tool 4: `describe_table`**
```python
@mcp.tool(annotations=READONLY)
def describe_table(schema: str, table: str, ctx: Context) -> ToolResult:
    """Describe a table's columns with types, null counts, and sample values."""
    db = ctx.lifespan_context["db"]
    qualified = f"lake.{schema}.{table}"

    # Get column info
    try:
        cols_info, cols_rows = _execute_safe(db, f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """)
    except ToolError:
        raise ToolError(f"Table '{qualified}' not found.")

    if not cols_rows:
        raise ToolError(f"Table '{qualified}' not found.")

    # Get sample values and null counts for first 20 columns
    col_names = [r[0] for r in cols_rows[:20]]
    sample_parts = []
    for cn in col_names:
        sample_parts.append(
            f"(SELECT ARRAY_AGG(DISTINCT {cn}) FROM "
            f"(SELECT {cn} FROM {qualified} WHERE {cn} IS NOT NULL LIMIT 100) AS _s) AS sample_{cn}"
        )

    # Get row count
    _, count_rows = _execute_safe(db, f"SELECT COUNT(*) FROM {qualified}")
    row_count = count_rows[0][0] if count_rows else 0

    summary = f"{qualified}: {row_count:,} rows, {len(cols_rows)} columns"
    out_cols = ["column_name", "data_type", "nullable"]
    return make_result(summary, out_cols, cols_rows)
```

Note: Skip sample values query in Phase 1 (can be slow on large tables). Add in Phase 6 with caching.

---

### Task 1.4: Catalog Cache (built at startup)

```python
def _build_catalog_cache(conn):
    """Query all tables at startup, cache schema/table/row_count/column_count."""
    catalog = {}
    try:
        cur = conn.execute("""
            SELECT table_schema, table_name, estimated_size, column_count
            FROM duckdb_tables()
            WHERE database_name = 'lake'
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """)
        for schema, table, est_size, col_count in cur.fetchall():
            if schema not in catalog:
                catalog[schema] = {}
            catalog[schema][table] = {
                "row_count": est_size or 0,
                "column_count": col_count or 0,
            }
    except Exception as e:
        print(f"Warning: catalog cache failed: {e}", flush=True)
    return catalog
```

---

### Task 1.5: Dockerfile

```dockerfile
FROM python:3.12-slim

RUN pip install --no-cache-dir \
    duckdb==1.5.0 \
    "fastmcp[http]>=3.1.0"

WORKDIR /app
COPY server.py .
COPY mcp_server.py .
COPY start.sh .
RUN chmod +x start.sh

EXPOSE 9999 4213

ENV FASTMCP_STATELESS_HTTP=true

CMD ["./start.sh"]
```

### Task 1.6: start.sh

```bash
#!/bin/bash
set -e

echo "Starting FastMCP server on :4213..."
python mcp_server.py &
MCP_PID=$!

echo "Starting HTTP API on :9999..."
python server.py &
API_PID=$!

echo "Both servers started (MCP=$MCP_PID, API=$API_PID)"

# Wait for either to exit, then kill both
wait -n $API_PID $MCP_PID
EXIT_CODE=$?
echo "A server exited with code $EXIT_CODE, shutting down..."
kill $API_PID $MCP_PID 2>/dev/null
wait
exit $EXIT_CODE
```

### Task 1.7: Update server.py

- Bump DuckDB version compatibility (1.5.0 handles this at pip level, not code)
- Remove DuckDB UI extension loading (port 4213 now belongs to FastMCP):

```python
# REMOVE these lines from server.py:
# try:
#     conn.execute("INSTALL ui; LOAD ui;")
#     conn.execute("CALL start_ui()")
#     print("DuckDB UI running on :4213", flush=True)
# except Exception as e:
#     print(f"ui extension not available: {e}", flush=True)
```

### Task 1.8: Update ~/.mcp.json

Change the duckdb entry to point to FastMCP:
```json
"duckdb": {
    "type": "http",
    "url": "http://178.156.228.119:4213/mcp"
}
```

### Task 1.9: Deploy + Smoke Test

1. Run `deploy.sh`
2. Check container logs: `ssh root@178.156.228.119 "docker logs common-ground-duckdb-server-1 --tail 50"`
3. Check MCP endpoint: `curl -s http://178.156.228.119:4213/mcp | head`
4. Test tools via Claude Code MCP:
   - `mcp__duckdb__list_schemas` → should return 12 schemas with row counts
   - `mcp__duckdb__list_tables` with schema="housing" → should return ~39 tables
   - `mcp__duckdb__describe_table` with schema="housing", table="hpd_violations" → column info
   - `mcp__duckdb__sql_query` with sql="SELECT COUNT(*) FROM lake.housing.hpd_violations" → row count

**Exit criteria:** All 4 tools respond with text tables (not JSON), ToolResult meta includes total_rows, instructions visible to LLM.

---

## Known Risks & Mitigations (Phase 1)

| Risk | Impact | Mitigation |
|------|--------|------------|
| Thread pool contention | Slow queries block other tool calls | Module-level threading.Lock serializes access. OK for single-user MCP. |
| DuckLake READ_ONLY support | May not work with ATTACH syntax | Fall back to SQL validation only if READ_ONLY fails |
| FTS ATTACH bug | Phase 3 FTS may fail on attached tables | Create local views and index those instead |
| DuckDB 1.5.0 extension compat | ducklake/spatial may need specific versions | Pin duckdb==1.5.0, test extensions in container |
| Port conflict with DuckDB UI | UI was on :4213, now FastMCP is | Remove UI extension from server.py |

---

## Phase 2: Domain Tools — Named, Purpose-Built

> Phase 2 research completed 2026-03-10.

**Goal:** Add 4 domain-specific tools. Each returns a focused ToolResult summary so the LLM doesn't need SQL.

### Research Findings

**Macros don't exist.** The `building_profile`, `complaints_by_zip`, `owner_violations` macros from the previous deployment were lost. We'll embed the SQL directly in the Python tool functions instead of recreating DuckDB macros — simpler, more maintainable, and we can use ToolResult properly.

**Table name mapping** (Socrata suffixes):

| Friendly name | Actual table name | Schema |
|---|---|---|
| HPD Buildings | `buildings_subject_to_hpd_jurisdiction_kj4p_ruqc` | housing |
| HPD Complaints | `housing_maintenance_code_complaints_and_problems_ygpa_z7cr` | housing |
| HPD Violations | `housing_maintenance_code_violations_wvxf_dwi5` | housing |
| DOB/ECB Violations | `dob_ecb_violations_6bgk_3dad` | housing |
| DOB Complaints | `dob_complaints_received_eabe_havv` | housing |
| Evictions | `evictions_6z8x_wfk4` | housing |
| 311 Requests | `n_311_service_requests_from_2020_to_present_erm2_nwe9` | social_services |
| Restaurant Inspections | `dohmh_new_york_city_restaurant_inspection_results_43nn_pn8j` | health |

**BBL construction:**
- HPD Buildings has NO `bbl` column → construct: `boroid || LPAD(block, 5, '0') || LPAD(lot, 4, '0')`
- HPD Complaints/Violations have `bbl` column directly
- DOB tables have NO `bbl` → construct from `boro + block + lot`, or join via `bin`
- DOB Complaints has NO `block/lot/boro` → join via `bin` only

**Join strategy:**
- `bbl` is the universal key across HPD Complaints, Violations, Evictions, 311, Restaurants
- `bin` links DOB tables to HPD (via buildings table)
- `buildingid`/`building_id` links HPD Complaints ↔ HPD Violations ↔ HPD Buildings

**Catalog metadata:**
- 183 of 294 tables have `comment` in `duckdb_tables()` — use for data_catalog search
- 8 tables have `tags` with `join_keys` info
- `duckdb_columns()` has column names for all tables

---

### Task 2.1: `building_profile(bbl: str)`

**Validate:** 10-digit BBL string.

**SQL strategy:** Query multiple tables by BBL, aggregate counts.

```python
BUILDING_PROFILE_SQL = """
WITH building AS (
    SELECT boroid, block, lot, buildingid, bin, streetname, housenumber,
           legalstories, legalclassa, legalclassb, managementprogram, zip,
           (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl
    FROM lake.housing.buildings_subject_to_hpd_jurisdiction_kj4p_ruqc
    WHERE (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
    LIMIT 1
),
violation_counts AS (
    SELECT
        COUNT(*) AS total_violations,
        COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations,
        MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation_date
    FROM lake.housing.housing_maintenance_code_violations_wvxf_dwi5
    WHERE bbl = ?
),
complaint_counts AS (
    SELECT
        COUNT(DISTINCT complaint_id) AS total_complaints,
        COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_complaints,
        MAX(TRY_CAST(received_date AS DATE)) AS latest_complaint_date
    FROM lake.housing.housing_maintenance_code_complaints_and_problems_ygpa_z7cr
    WHERE bbl = ?
),
dob_counts AS (
    SELECT
        COUNT(*) AS total_dob_violations,
        MAX(TRY_CAST(issue_date AS DATE)) AS latest_dob_date
    FROM lake.housing.dob_ecb_violations_6bgk_3dad
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
)
SELECT
    b.bbl, b.housenumber || ' ' || b.streetname AS address, b.zip,
    b.legalstories AS stories,
    (COALESCE(b.legalclassa, 0) + COALESCE(b.legalclassb, 0)) AS total_units,
    b.managementprogram,
    v.total_violations, v.open_violations, v.latest_violation_date,
    c.total_complaints, c.open_complaints, c.latest_complaint_date,
    d.total_dob_violations, d.latest_dob_date
FROM building b
CROSS JOIN violation_counts v
CROSS JOIN complaint_counts c
CROSS JOIN dob_counts d
"""
```

**Tool implementation:**
```python
@mcp.tool(annotations=READONLY)
def building_profile(bbl: str, ctx: Context) -> ToolResult:
    """Get a comprehensive profile for a NYC building by BBL (Borough-Block-Lot).
    BBL is 10 digits: borough(1) + block(5) + lot(4). Example: 1000670001"""
    if not re.match(r'^\d{10}$', bbl):
        raise ToolError("BBL must be exactly 10 digits. Format: borough(1) + block(5) + lot(4)")

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(db, BUILDING_PROFILE_SQL, [bbl, bbl, bbl, bbl])

    if not rows:
        raise ToolError(f"No building found for BBL {bbl}")

    row = dict(zip(cols, rows[0]))
    summary = (
        f"BBL {row['bbl']}: {row['address']}, {row['zip']}\n"
        f"  {row['stories']} stories, {row['total_units']} units ({row['managementprogram'] or 'N/A'})\n"
        f"  HPD Violations: {row['total_violations']} total, {row['open_violations']} open"
        f" (latest: {row['latest_violation_date'] or 'N/A'})\n"
        f"  HPD Complaints: {row['total_complaints']} total, {row['open_complaints']} open"
        f" (latest: {row['latest_complaint_date'] or 'N/A'})\n"
        f"  DOB/ECB Violations: {row['total_dob_violations']}"
        f" (latest: {row['latest_dob_date'] or 'N/A'})"
    )
    return ToolResult(
        content=summary,
        structured_content={"building": row},
        meta={"bbl": bbl},
    )
```

---

### Task 2.2: `complaints_by_zip(zip_code: str, days: int = 365)`

**Validate:** 5-digit ZIP.

**SQL strategy:** Aggregate HPD + 311 + DOB complaints by type for a ZIP code.

```python
COMPLAINTS_BY_ZIP_SQL = """
WITH hpd AS (
    SELECT major_category AS category, 'HPD' AS source,
           COUNT(DISTINCT complaint_id) AS cnt
    FROM lake.housing.housing_maintenance_code_complaints_and_problems_ygpa_z7cr
    WHERE post_code = ?
      AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL ? DAY
    GROUP BY major_category
),
svc311 AS (
    SELECT problem_formerly_complaint_type AS category, '311' AS source,
           COUNT(*) AS cnt
    FROM lake.social_services.n_311_service_requests_from_2020_to_present_erm2_nwe9
    WHERE incident_zip = ?
      AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL ? DAY
    GROUP BY problem_formerly_complaint_type
),
combined AS (
    SELECT * FROM hpd
    UNION ALL
    SELECT * FROM svc311
)
SELECT source, category, cnt
FROM combined
ORDER BY cnt DESC
"""
```

**Tool implementation:**
```python
@mcp.tool(annotations=READONLY)
def complaints_by_zip(zip_code: str, days: int = 365, ctx: Context) -> ToolResult:
    """Get complaint statistics for a NYC ZIP code.
    Aggregates HPD housing complaints and 311 service requests.
    Args:
        zip_code: 5-digit NYC ZIP code
        days: Look-back period in days (default 365)"""
    if not re.match(r'^\d{5}$', zip_code):
        raise ToolError("ZIP code must be exactly 5 digits")

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(db, COMPLAINTS_BY_ZIP_SQL, [zip_code, days, zip_code, days])

    total = sum(r[2] for r in rows)
    sources = set(r[0] for r in rows)
    top5 = rows[:5]
    top5_text = ", ".join(f"{r[1]} ({r[2]})" for r in top5)

    summary = f"ZIP {zip_code} last {days} days: {total:,} complaints from {', '.join(sources)}\nTop: {top5_text}"

    return make_result(
        summary, cols, rows,
        {"total_complaints": total, "days": days, "sources": list(sources)},
    )
```

---

### Task 2.3: `owner_violations(bbl: str)`

**SQL strategy:** Get all violations (HPD + DOB/ECB) for a BBL, with status and dates.

```python
OWNER_VIOLATIONS_SQL = """
WITH hpd AS (
    SELECT 'HPD' AS source, violationid AS violation_id,
           class, novdescription AS description,
           violationstatus AS status, novissueddate AS issue_date,
           currentstatus AS current_status, currentstatusdate AS status_date
    FROM lake.housing.housing_maintenance_code_violations_wvxf_dwi5
    WHERE bbl = ?
    ORDER BY TRY_CAST(novissueddate AS DATE) DESC NULLS LAST
    LIMIT 200
),
dob AS (
    SELECT 'DOB/ECB' AS source, ecb_violation_number AS violation_id,
           severity AS class, violation_description AS description,
           ecb_violation_status AS status, issue_date,
           certification_status AS current_status, hearing_date AS status_date
    FROM lake.housing.dob_ecb_violations_6bgk_3dad
    WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
    ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
    LIMIT 200
)
SELECT * FROM hpd UNION ALL SELECT * FROM dob
ORDER BY TRY_CAST(issue_date AS DATE) DESC NULLS LAST
"""
```

**Tool implementation:**
```python
@mcp.tool(annotations=READONLY)
def owner_violations(bbl: str, ctx: Context) -> ToolResult:
    """Get violation history for a NYC property by BBL.
    Shows HPD housing violations and DOB/ECB building violations.
    BBL is 10 digits: borough(1) + block(5) + lot(4)."""
    if not re.match(r'^\d{10}$', bbl):
        raise ToolError("BBL must be exactly 10 digits")

    db = ctx.lifespan_context["db"]
    cols, rows = _execute(db, OWNER_VIOLATIONS_SQL, [bbl, bbl])

    total = len(rows)
    open_count = sum(1 for r in rows if r[4] and 'open' in str(r[4]).lower())
    sources = set(r[0] for r in rows)

    summary = f"BBL {bbl}: {total} violations ({open_count} open) from {', '.join(sources)}"

    return make_result(
        summary, cols, rows,
        {"total_violations": total, "open_count": open_count, "sources": list(sources)},
    )
```

---

### Task 2.4: `data_catalog(keyword: str = "")`

**Key insight:** 183 tables have comments. Search table names + comments + column names.

**No-keyword mode:** Return schema summary with descriptions.

**With keyword:** Search via ILIKE across table names, comments, and column names.

```python
DATA_CATALOG_SQL = """
WITH table_matches AS (
    SELECT DISTINCT t.schema_name, t.table_name, t.comment,
           t.column_count, t.estimated_size, 'table/comment' AS match_type
    FROM duckdb_tables() t
    WHERE t.database_name = 'lake'
      AND t.schema_name NOT IN ('information_schema', 'pg_catalog')
      AND (t.table_name ILIKE '%' || ? || '%' OR t.comment ILIKE '%' || ? || '%')
),
column_matches AS (
    SELECT DISTINCT c.schema_name, c.table_name, t.comment,
           t.column_count, t.estimated_size, 'column' AS match_type
    FROM duckdb_columns() c
    JOIN duckdb_tables() t ON c.schema_name = t.schema_name
         AND c.table_name = t.table_name AND t.database_name = 'lake'
    WHERE c.database_name = 'lake'
      AND c.column_name ILIKE '%' || ? || '%'
),
all_matches AS (
    SELECT * FROM table_matches
    UNION
    SELECT * FROM column_matches
)
SELECT schema_name, table_name, comment, estimated_size, column_count, match_type
FROM all_matches
ORDER BY estimated_size DESC
"""
```

**Tool implementation:**
```python
SCHEMA_DESCRIPTIONS = {
    "business": "BLS employment, ACS census, business licenses, M/WBE certs",
    "city_government": "PLUTO lots, payroll, OATH hearings, zoning, facilities",
    "economics": "FRED metro data, IRS income by ZIP",
    "education": "DOE surveys, test scores, enrollment, NYSED data",
    "environment": "Air quality, tree census, energy benchmarking, FEMA, EPA",
    "financial": "CFPB consumer complaints",
    "health": "Restaurant inspections, rat inspections, health indicators",
    "housing": "HPD complaints/violations, DOB, evictions, NYCHA, HMDA",
    "public_safety": "NYPD crimes/arrests, motor vehicle collisions, shootings",
    "recreation": "Parks, pools, permits, events",
    "social_services": "311 requests, food assistance, childcare",
    "transportation": "MTA ridership, parking tickets, traffic, streets",
}

@mcp.tool(annotations=READONLY)
def data_catalog(keyword: str = "", ctx: Context) -> ToolResult:
    """Search the NYC open data lake for tables matching a keyword.
    Searches table names, descriptions, and column names.
    Call with no keyword to see a schema overview.
    Examples: 'eviction', 'restaurant', 'arrest', 'school', 'rat'"""
    if not keyword.strip():
        # No keyword → schema overview
        catalog = ctx.lifespan_context["catalog"]
        lines = []
        for schema in sorted(catalog):
            tables = catalog[schema]
            total = sum(t["row_count"] for t in tables.values())
            desc = SCHEMA_DESCRIPTIONS.get(schema, "")
            lines.append(f"{schema} ({len(tables)} tables, ~{total:,} rows): {desc}")
        return ToolResult(content="\n".join(lines))

    db = ctx.lifespan_context["db"]
    kw = keyword.strip()
    cols, rows = _execute(db, DATA_CATALOG_SQL, [kw, kw, kw])

    if not rows:
        return ToolResult(content=f"No tables found matching '{kw}'. Try a broader term.")

    summary = f"Found {len(rows)} tables matching '{kw}':"
    return make_result(summary, cols, rows)
```

---

### Task 2.5: Update instructions

Update the `INSTRUCTIONS` string to reference the new tools (remove "Phase 2" placeholders):

```python
INSTRUCTIONS = """\
NYC open data lake — 294 tables across 12 schemas, 60M+ rows total.

PREFERRED TOOLS:
- building_profile(bbl) — full building dossier by BBL
- complaints_by_zip(zip_code, days?) — complaint analytics by ZIP
- owner_violations(bbl) — violation history for a property
- data_catalog(keyword?) — discover tables by topic (or no keyword for overview)

POWER USER:
- sql_query(sql) — raw read-only SQL
- list_schemas() — schema overview with row counts
- list_tables(schema) — tables in a schema
- describe_table(schema, table) — column names and types

SCHEMAS: housing, public_safety, health, social_services, financial, \
environment, recreation, education, business, transportation, \
city_government, census

SPATIAL: 61 views in lake.spatial.* with GEOMETRY columns.
BBL FORMAT: 10 digits — borough(1) + block(5) + lot(4). Example: 1000670001
ZIP FORMAT: 5 digits. Manhattan 100xx, Brooklyn 112xx, Bronx 104xx.

All tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations LIMIT 5
"""
```

---

### Task 2.6: Deploy + integration test

- [ ] Deploy updated server
- [ ] Test `building_profile('3012340001')` — should return address, units, violation counts
- [ ] Test `building_profile('0000000000')` — should return error "No building found"
- [ ] Test `complaints_by_zip('10001')` — should return HPD + 311 complaint summary
- [ ] Test `complaints_by_zip('99999')` — should return 0 complaints
- [ ] Test `owner_violations('3012340001')` — should return violation list
- [ ] Test `data_catalog('eviction')` — should find evictions table
- [ ] Test `data_catalog('restaurant')` — should find restaurant inspection tables
- [ ] Test `data_catalog()` — should return schema overview
- [ ] Verify all ToolResult responses have content (text) + structured_content (dict)

**Exit criteria:** 8 tools total. LLM can answer "tell me about building X", "what complaints in ZIP Y", "find tables about Z" — all without writing SQL.

---

## Phase 3: Search — FTS + Text Search Tool

**Goal:** Add BM25 full-text search as a tool.

### FTS ATTACH Bug Workaround

DuckDB FTS has a known bug (#13523) where `match_bm25` fails on attached database tables ("table 'terms' does not exist"). Workaround:

```python
# In lifespan, after attaching lake:
# Create local views for FTS-indexable tables
conn.execute("CREATE OR REPLACE VIEW cfpb_complaints AS SELECT rowid, * FROM lake.health.cfpb_complaints")
conn.execute("PRAGMA create_fts_index('cfpb_complaints', 'rowid', 'narrative', stemmer='porter', overwrite=1)")
```

This creates the FTS index in the default (memory) database, referencing a view of the DuckLake table.

### Task 3.1: FTS index creation at startup (in lifespan)

- [ ] Create local views for text-heavy tables:
  - `cfpb_complaints` → `lake.health.cfpb_complaints` (narrative)
  - `service_requests_311` → `lake.social_services.n_311_service_requests_from_2020` (descriptor, resolution_description)
  - `restaurant_inspections` → `lake.health.restaurant_inspections` (violation_description)
- [ ] Create FTS indexes on each view with stemmer='porter'
- [ ] Handle gracefully if underlying table doesn't exist
- [ ] Store index metadata in lifespan context: `{"fts_tables": {"cfpb_complaints": ["narrative"], ...}}`

### Task 3.2: `text_search(table: str, query: str, limit: int = 10)`

- [ ] Validate table has FTS index (check fts_tables in context)
- [ ] Run BM25 match query
- [ ] LLM content: top results with scores + key text columns
- [ ] structured_content: full result rows
- [ ] meta: total_matches, query_time_ms

### Task 3.3: Deploy + test

- [ ] Test: "mold" in cfpb, "rat" in 311, "mice" in restaurants

**Exit criteria:** `text_search` returns BM25-ranked results.

---

## Phase 4: Neighborhood Snapshot — Cross-Dataset Intelligence

**Goal:** Given a ZIP code, pull data from multiple schemas and return a unified profile.

### Task 4.1: `neighborhood_snapshot(zip_code: str)`

- [ ] Run multiple queries against different schemas, aggregate per-section
- [ ] LLM content: multi-section text summary (HOUSING / SAFETY / HEALTH / SOCIAL)
- [ ] structured_content: full data per section
- [ ] meta: data_freshness, coverage
- [ ] Handle missing data gracefully

### Task 4.2: Deploy + test

- [ ] Test: 10001, 11201, 10451

**Exit criteria:** Single tool call → complete neighborhood picture.

---

## Phase 5: Embeddings — Semantic Search on High-Value Tables

**Goal:** Add Model2Vec embeddings to CFPB narratives as proof-of-concept.

### Task 5.1: Embedding column + batch indexer

- [ ] Add FLOAT[512] column to cfpb_complaints
- [ ] Write batch indexer script using Model2Vec (potion-base-32M)
- [ ] Run indexer (782K rows)

### Task 5.2: `semantic_search(query, table, filters, limit)`

- [ ] Load Model2Vec model in lifespan
- [ ] Generate query embedding, use array_cosine_similarity
- [ ] Return ranked results

### Task 5.3: Deploy + test

**Exit criteria:** Meaning-based results distinct from keyword search.

---

## Phase 6: Hardening — Production Polish

### Task 6.1: Query timeout

- [ ] Add threading-based timeout wrapper (30s)
- [ ] Use conn.interrupt() to cancel long queries

### Task 6.2: Error handling audit

- [ ] Every tool: try/except → ToolError with clean message
- [ ] Log errors server-side
- [ ] No stack traces to LLM

### Task 6.3: describe_table improvements

- [ ] Add sample values (cached at startup for top tables)
- [ ] Add null percentage per column

### Task 6.4: Instructions tuning

- [ ] Dynamic instructions from catalog cache (actual table names + row counts)

### Task 6.5: Kill old motherduck MCP

- [ ] Stop mcp-server-motherduck on 8642
- [ ] Clean up configs

**Exit criteria:** Production-safe, no SQL injection, clean errors, fast catalog.

---

## Execution Order

```
Phase 1 (Foundation)     → MUST do first, everything depends on this
Phase 2 (Domain Tools)   → Immediately after Phase 1
Phase 3 (FTS/Search)     → Can start during Phase 2
Phase 4 (Neighborhood)   → Needs Phase 2 macros working
Phase 5 (Embeddings)     → Independent, can start anytime after Phase 1
Phase 6 (Hardening)      → After all tools are working
```

## Files Changed

| File | Action | Phase |
|------|--------|-------|
| `duckdb-server/mcp_server.py` | Create | 1 |
| `duckdb-server/start.sh` | Create | 1 |
| `duckdb-server/Dockerfile` | Update | 1 |
| `duckdb-server/server.py` | Edit (remove UI extension) | 1 |
| `docker-compose.yml` | Add FASTMCP_STATELESS_HTTP env | 1 |
| `~/.mcp.json` | Update duckdb entry | 1 |

## Dependencies

- `fastmcp[http]>=3.1.0` (new)
- `duckdb==1.5.0` (upgrade from 1.4.4)
- `model2vec` + `numpy` (Phase 5 only)
