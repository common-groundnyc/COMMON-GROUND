# Super Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the 13,576-line monolithic MCP server into 14 modular super tools with full 294-table coverage.

**Architecture:** Each super tool lives in its own module (`tools/*.py`). Shared infrastructure (db, types, lance, graph, validation, formatting) lives in `shared/`. The entry point `mcp_server.py` shrinks to ~250 lines: lifespan + tool registration + middleware. All 63 old tool functions are deleted and replaced by 14 new ones.

**Tech Stack:** FastMCP, DuckDB 1.5.1, DuckLake, Lance, Pydantic v2, Python 3.12

**Spec:** `docs/superpowers/specs/2026-03-31-super-tools-design.md`

**Base directory:** `infra/duckdb-server/`

---

## Task 1: Scaffold shared/ directory

**Files:**
- Create: `shared/__init__.py`
- Create: `shared/types.py`
- Create: `shared/db.py`
- Create: `shared/formatting.py`
- Create: `shared/validation.py`
- Create: `shared/lance.py`
- Create: `shared/graph.py`

This task extracts reusable code from `mcp_server.py` into `shared/` modules. Zero behavior change. The monolith still runs — we're just creating importable copies of the shared utilities.

- [ ] **Step 1: Create `shared/__init__.py`**

```python
# infra/duckdb-server/shared/__init__.py
```

Empty init file. Run: `mkdir -p shared && touch shared/__init__.py`

- [ ] **Step 2: Create `shared/types.py`**

Extract annotated types and constants from `mcp_server.py` lines 38-51, 64-65, 70-82.

```python
# infra/duckdb-server/shared/types.py
"""Reusable annotated types, constants, and tool annotations for all super tools."""

import re
from typing import Annotated, Literal
from pydantic import Field
from mcp.types import ToolAnnotations

# --- Annotated input types ---

BBL = Annotated[str, Field(
    description="10-digit BBL: borough(1) + block(5) + lot(4), e.g. '1000670001'",
    examples=["1000670001", "2039720033", "3012340001"],
)]

ZIP = Annotated[str, Field(
    description="5-digit NYC ZIP code, e.g. '10003', '11201'",
    examples=["10003", "11201", "10456"],
)]

NAME = Annotated[str, Field(
    description="Person or company name, fuzzy matched. Partial names and misspellings OK, e.g. 'Steven Croman', 'BLACKSTONE', 'J Smith'",
    examples=["Steven Croman", "Barton Perlbinder", "BLACKSTONE GROUP", "Jane Smith"],
)]

# --- Tool annotations ---

READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
ADMIN = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)

# --- Row limits ---

MAX_LLM_ROWS = 20
MAX_STRUCTURED_ROWS = 500
MAX_QUERY_ROWS = 1000

# --- Lance distance thresholds ---

LANCE_DIR = "/data/common-ground/lance"
LANCE_NAME_DISTANCE = 0.4
LANCE_TIGHT_DISTANCE = 0.3
LANCE_CATALOG_DISTANCE = 0.7
LANCE_CATEGORY_SIM = 0.5
```

- [ ] **Step 3: Create `shared/validation.py`**

Extract SQL validation from `mcp_server.py` lines 53-75, 617-636.

```python
# infra/duckdb-server/shared/validation.py
"""SQL validation and safety checks."""

import re
from fastmcp.exceptions import ToolError

_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY"
    r"|CALL|LOAD|INSTALL|ATTACH|DETACH|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)

_SAFE_DDL = re.compile(
    r"^\s*CREATE\s+OR\s+REPLACE\s+VIEW\b",
    re.IGNORECASE,
)

_UNSAFE_FUNCTIONS = re.compile(
    r"\b(read_parquet|read_csv|read_csv_auto|read_json|read_json_auto"
    r"|read_text|glob|read_blob|write_parquet|write_csv"
    r"|httpfs_|http_get|http_post)\s*\(",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> None:
    """Raise ToolError if SQL contains unsafe operations or functions."""
    if _UNSAFE_SQL.match(sql) and not _SAFE_DDL.match(sql):
        raise ToolError(
            f"Only SELECT, WITH, EXPLAIN, and DESCRIBE queries are allowed. "
            f"CREATE OR REPLACE VIEW is allowed via mode='admin'. "
            f"Your query starts with a disallowed keyword."
        )
    if _UNSAFE_FUNCTIONS.search(sql):
        raise ToolError(
            "Filesystem and network functions are not allowed in queries. "
            "Use lake.schema.table to access data."
        )


def validate_admin_sql(sql: str) -> None:
    """Raise ToolError if DDL is not CREATE OR REPLACE VIEW."""
    if not _SAFE_DDL.match(sql):
        raise ToolError("Only CREATE OR REPLACE VIEW statements are allowed in admin mode.")
```

- [ ] **Step 4: Create `shared/db.py`**

Copy `CursorPool` from `cursor_pool.py` and extract `_execute`, `_safe_query`, `_fill_placeholders` from `mcp_server.py` lines 638-704, 5329-5342.

```python
# infra/duckdb-server/shared/db.py
"""Database access layer — CursorPool and query helpers."""

import threading
from cursor_pool import CursorPool
from sql_utils import sanitize_error
from shared.types import MAX_QUERY_ROWS

# Re-export CursorPool for tools that import from shared.db
__all__ = ["CursorPool", "execute", "safe_query", "fill_placeholders"]

_RECONNECT_ERRORS = ("HTTP 403", "HTTP 400", "HTTP 301", "Bad Request", "s3://ducklake")
_reconnect_lock = threading.Lock()
_last_reconnect = 0


def execute(pool: CursorPool, sql: str, params=None, max_rows: int = MAX_QUERY_ROWS):
    """Execute SQL through cursor pool. Returns (cols, rows). Handles reconnection on S3 errors."""
    try:
        return pool.execute(sql, params, max_rows)
    except Exception as e:
        msg = str(e)
        if any(err in msg for err in _RECONNECT_ERRORS):
            _handle_reconnect(pool, msg)
            return pool.execute(sql, params, max_rows)
        raise


def safe_query(pool: CursorPool, sql: str, params=None):
    """Execute SQL, return (cols, rows) on success or ([], []) on error. Never raises."""
    try:
        return execute(pool, sql, params)
    except Exception:
        return [], []


def fill_placeholders(sql_template: str, bbls: list[str]) -> str:
    """Replace $BBL_LIST placeholder with quoted BBL values."""
    quoted = ", ".join(f"'{b}'" for b in bbls)
    return sql_template.replace("$BBL_LIST", quoted)


def _handle_reconnect(pool, error_msg):
    """Thread-safe reconnection on S3/HTTP errors."""
    import time
    global _last_reconnect
    with _reconnect_lock:
        now = time.time()
        if now - _last_reconnect < 30:
            return
        _last_reconnect = now
        # Log the reconnection attempt
        print(f"Reconnecting due to: {error_msg[:100]}", flush=True)
```

- [ ] **Step 5: Create `shared/formatting.py`**

Extract `format_text_table` and `make_result` from `mcp_server.py` lines 465-506.

```python
# infra/duckdb-server/shared/formatting.py
"""Result formatting helpers for tool responses."""

from fastmcp.tools.tool import ToolResult
from shared.types import MAX_LLM_ROWS


def format_text_table(cols: list, rows: list, max_rows: int = MAX_LLM_ROWS) -> str:
    """Format columns and rows as a pipe-separated text table."""
    if not cols or not rows:
        return ""
    display_rows = rows[:max_rows]
    widths = [max(len(str(c)), max((len(str(r[i])) for r in display_rows), default=0)) for i, c in enumerate(cols)]
    header = " | ".join(str(c).ljust(w) for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines = [header, sep]
    for row in display_rows:
        lines.append(" | ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows)")
    return "\n".join(lines)


def make_result(summary: str, cols: list, rows: list, meta_extra: dict | None = None) -> ToolResult:
    """Build a ToolResult with text content and structured_content."""
    text = summary
    if cols and rows:
        table = format_text_table(cols, rows)
        text = f"{summary}\n\n{table}" if summary else table

    structured = None
    if cols and rows:
        structured = {"rows": [dict(zip(cols, row)) for row in rows[:500]]}

    meta = {"total_rows": len(rows)}
    if meta_extra:
        meta.update(meta_extra)

    return ToolResult(content=text, structured_content=structured, meta=meta)
```

- [ ] **Step 6: Create `shared/lance.py`**

Extract Lance helpers from `mcp_server.py` lines 86-170.

```python
# infra/duckdb-server/shared/lance.py
"""Lance vector search helpers for entity resolution and semantic search."""

from shared.types import LANCE_DIR, LANCE_NAME_DISTANCE


def vector_expand_names(ctx, search_term: str, threshold: float = LANCE_NAME_DISTANCE, k: int = 5) -> set:
    """Find similar entity names via Lance vector search. Returns empty set on failure."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if not embed_fn:
        return set()
    try:
        from embedder import vec_to_sql
        query_vec = embed_fn(search_term)
        vec_literal = vec_to_sql(query_vec)
        sql = f"""
            SELECT name, _distance AS dist
            FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={k})
            ORDER BY _distance ASC
        """
        pool = ctx.lifespan_context["pool"]
        with pool.cursor() as cur:
            rows = cur.execute(sql).fetchall()
        result = set()
        for name, dist in rows:
            if dist < threshold:
                result.add(name.upper())
        result.discard(search_term.upper())
        return result
    except Exception:
        return set()


def lance_route_entity(ctx, search_term: str, k: int = 30) -> dict:
    """Search Lance entity index to find which source tables contain matching names.
    Returns dict with 'sources' (set of table names) and 'matched_names' (list of names).
    Returns empty dict on failure (caller falls back to full scan)."""
    embed_fn = ctx.lifespan_context.get("embed_fn")
    if not embed_fn:
        return {}
    try:
        from embedder import vec_to_sql
        query_vec = embed_fn(search_term)
        vec_literal = vec_to_sql(query_vec)
        sql = f"""
            SELECT name, source, _distance AS dist
            FROM lance_vector_search('{LANCE_DIR}/entity_name_embeddings.lance', 'embedding', {vec_literal}, k={k})
            WHERE _distance < {LANCE_NAME_DISTANCE}
            ORDER BY _distance ASC
        """
        pool = ctx.lifespan_context["pool"]
        with pool.cursor() as cur:
            rows = cur.execute(sql).fetchall()
        if not rows:
            return {}
        sources = set()
        matched_names = []
        for name, source, dist in rows:
            sources.add(source)
            matched_names.append(name)
        return {"sources": sources, "matched_names": matched_names}
    except Exception:
        return {}
```

- [ ] **Step 7: Create `shared/graph.py`**

Extract graph cache helpers from `mcp_server.py` lines 1093-1154, 9399-9403.

```python
# infra/duckdb-server/shared/graph.py
"""Graph cache management for DuckPGQ property graph."""

import pathlib
import time
from fastmcp.exceptions import ToolError

GRAPH_CACHE_DIR = "/data/common-ground/graph-cache"

GRAPH_TABLES = [
    "graph_owners", "graph_buildings", "graph_owns",
    "graph_violations", "graph_has_violation", "graph_shared_owner",
    "graph_building_flags", "graph_acris_sales", "graph_rent_stabilization",
    "graph_corp_contacts", "graph_business_at_building", "graph_acris_chain",
    "graph_dob_owners", "graph_eviction_petitioners",
    "graph_doing_business", "graph_campaign_donors",
    "graph_epa_facilities",
    "graph_tx_entities", "graph_tx_edges", "graph_tx_shared",
    "graph_corps", "graph_corp_people", "graph_corp_officer_edges", "graph_corp_shared_officer",
    "graph_pol_entities", "graph_pol_donations", "graph_pol_contracts", "graph_pol_lobbying",
    "graph_contractors", "graph_permit_edges", "graph_contractor_shared",
    "graph_contractor_building_shared",
    "graph_fec_contributions", "graph_litigation_respondents",
    "graph_officers", "graph_officer_complaints", "graph_officer_shared_command",
    "graph_coib_donors", "graph_coib_policymakers", "graph_coib_donor_edges",
    "graph_bic_companies", "graph_bic_violation_edges", "graph_bic_shared_bbl",
    "graph_dob_respondents", "graph_dob_respondent_bbl",
]


def require_graph(ctx) -> None:
    """Raise ToolError if graph tables are not loaded."""
    if not ctx.lifespan_context.get("graph_ready"):
        raise ToolError("Graph tables are still loading. Try again in a few minutes.")


def graph_cache_fresh() -> bool:
    """Check if Parquet cache exists and is < 24 hours old."""
    cache = pathlib.Path(GRAPH_CACHE_DIR)
    if not cache.exists():
        return False
    marker = cache / "_built_at.txt"
    if not marker.exists():
        return False
    try:
        built_ts = float(marker.read_text().strip())
        age_hours = (time.time() - built_ts) / 3600
        return age_hours < 24 and all((cache / f"{t}.parquet").exists() for t in GRAPH_TABLES)
    except (ValueError, OSError):
        return False


def load_graph_from_cache(conn) -> None:
    """Load all graph tables from Parquet cache."""
    for t in GRAPH_TABLES:
        path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
        conn.execute(f"CREATE OR REPLACE TABLE main.{t} AS SELECT * FROM read_parquet('{path}')")


def save_graph_to_cache(conn) -> None:
    """Export all graph tables to Parquet for fast restart."""
    cache = pathlib.Path(GRAPH_CACHE_DIR)
    cache.mkdir(parents=True, exist_ok=True)
    for t in GRAPH_TABLES:
        path = f"{GRAPH_CACHE_DIR}/{t}.parquet"
        conn.execute(f"COPY main.{t} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    (cache / "_built_at.txt").write_text(str(time.time()))
```

- [ ] **Step 8: Verify imports work**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -c "from shared.types import BBL, ZIP, NAME, READONLY, MAX_LLM_ROWS; print('types OK')"`

Run: `python -c "from shared.validation import validate_sql; print('validation OK')"`

Run: `python -c "from shared.formatting import make_result, format_text_table; print('formatting OK')"`

Expected: All print OK without import errors.

- [ ] **Step 9: Commit**

```bash
git add shared/
git commit -m "feat: scaffold shared/ directory with types, db, formatting, validation, lance, graph"
```

---

## Task 2: Scaffold tools/ and middleware/ directories

**Files:**
- Create: `tools/__init__.py`
- Create: `middleware/__init__.py`
- Move: `response_middleware.py` → `middleware/response_middleware.py`
- Move: `citation_middleware.py` → `middleware/citation_middleware.py`
- Move: `freshness_middleware.py` → `middleware/freshness_middleware.py`
- Move: `percentile_middleware.py` → `middleware/percentile_middleware.py`

- [ ] **Step 1: Create directories and init files**

```bash
mkdir -p tools middleware
touch tools/__init__.py middleware/__init__.py
```

- [ ] **Step 2: Copy middleware files to middleware/ directory**

Note: We COPY first (not move) so the monolith keeps working during migration. Once all tools are extracted, we delete the originals.

```bash
cp response_middleware.py middleware/response_middleware.py
cp citation_middleware.py middleware/citation_middleware.py
cp freshness_middleware.py middleware/freshness_middleware.py
cp percentile_middleware.py middleware/percentile_middleware.py
```

- [ ] **Step 3: Verify the monolith still starts**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -c "import mcp_server; print('monolith OK')"`

Note: This will fail if run outside Docker (needs DuckLake). That's fine — the import test verifies syntax, not runtime.

- [ ] **Step 4: Commit**

```bash
git add tools/ middleware/
git commit -m "feat: scaffold tools/ and middleware/ directories"
```

---

## Task 3: Extract school tool (simplest, no Lance/graph deps)

**Files:**
- Create: `tools/school.py`
- Test: Manual test via MCP client

This is the first tool extraction. It absorbs 4 old tools: `school_search`, `school_report`, `school_compare`, `district_report`. It has zero Lance or graph dependencies — pure SQL queries.

- [ ] **Step 1: Identify source functions in mcp_server.py**

The 4 functions to extract:
- `school_report` — line ~6361
- `school_search` — line ~6639
- `school_compare` — line ~6761
- `district_report` — line ~6910

Read each function from `mcp_server.py` to understand the SQL queries, helper functions, and return patterns they use.

- [ ] **Step 2: Create `tools/school.py`**

```python
# infra/duckdb-server/tools/school.py
"""school() super tool — NYC public school lookup by name, DBN, ZIP, or district."""

import re
from typing import Annotated
from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError

from shared.types import READONLY, MAX_LLM_ROWS
from shared.db import execute, safe_query
from shared.formatting import make_result, format_text_table


# --- Input detection patterns ---

_DBN_PATTERN = re.compile(r"^\d{2}[MXKQR]\d{3}$", re.IGNORECASE)
_DISTRICT_PATTERN = re.compile(r"^(?:district\s*)?(\d{1,2})$", re.IGNORECASE)
_ZIP_PATTERN = re.compile(r"^\d{5}$")
_MULTI_DBN = re.compile(r"^[\dMXKQR,\s]+$", re.IGNORECASE)


def school(
    query: Annotated[str, Field(
        description="School name, DBN, ZIP, or district number. Auto-detected, e.g. 'Stuyvesant', '02M475', '10003', '2', '02M475,02M001'",
        examples=["Stuyvesant", "02M475", "10003", "district 2", "02M475,02M001"],
    )],
    ctx: Context = None,
) -> ToolResult:
    """Look up NYC public schools by name, DBN, ZIP code, or district number. Returns test scores, chronic absenteeism, class size, and survey results. Auto-detects input type: DBN gives a full report, ZIP searches nearby schools, district number gives an aggregate, multiple DBNs compare side-by-side. Do NOT use for neighborhood-level analysis (use neighborhood) or person lookups (use entity)."""
    pool = ctx.lifespan_context["pool"]
    q = query.strip()

    # Multiple DBNs → compare
    if "," in q:
        dbns = [d.strip().upper() for d in q.split(",") if d.strip()]
        if all(_DBN_PATTERN.match(d) for d in dbns):
            return _compare(pool, dbns)
        raise ToolError(f"Multiple values must be valid DBNs like '02M475,02M001'. Got: {q}")

    # Single DBN → report
    if _DBN_PATTERN.match(q):
        return _report(pool, q.upper())

    # District number → district report
    m = _DISTRICT_PATTERN.match(q)
    if m:
        return _district(pool, int(m.group(1)))

    # ZIP → search in area
    if _ZIP_PATTERN.match(q):
        return _search_by_zip(pool, q)

    # Name → search by name
    return _search_by_name(pool, q)


# --- Private query functions ---
# These contain the actual SQL queries extracted from the old school_report,
# school_search, school_compare, and district_report tool functions.
# The implementing agent should read the original functions from mcp_server.py
# lines ~6361-7053 and copy the SQL queries into these private functions,
# replacing pool access with shared.db.execute().

def _report(pool, dbn: str) -> ToolResult:
    """Full school report for a single DBN. Extracted from school_report()."""
    # Copy SQL queries from mcp_server.py school_report() (line ~6361)
    # Replace: ctx.lifespan_context["pool"] → pool (already passed)
    # Replace: _execute(pool, sql) → execute(pool, sql)
    # Keep all SQL queries exactly as-is
    raise NotImplementedError("Extract from mcp_server.py school_report()")


def _search_by_name(pool, name: str) -> ToolResult:
    """Search schools by name. Extracted from school_search()."""
    raise NotImplementedError("Extract from mcp_server.py school_search()")


def _search_by_zip(pool, zipcode: str) -> ToolResult:
    """Search schools by ZIP code. Extracted from school_search()."""
    raise NotImplementedError("Extract from mcp_server.py school_search()")


def _compare(pool, dbns: list[str]) -> ToolResult:
    """Side-by-side comparison. Extracted from school_compare()."""
    raise NotImplementedError("Extract from mcp_server.py school_compare()")


def _district(pool, district_num: int) -> ToolResult:
    """District aggregate report. Extracted from district_report()."""
    raise NotImplementedError("Extract from mcp_server.py district_report()")
```

- [ ] **Step 3: Extract actual SQL from monolith**

Read `mcp_server.py` lines 6361-7053. For each of the 4 old tool functions:
1. Copy the function body (SQL queries + result formatting)
2. Replace `_execute(pool, sql)` calls with `execute(pool, sql)` from `shared.db`
3. Replace `make_result(...)` calls with `make_result(...)` from `shared.formatting`
4. Replace `ToolError` imports with `from fastmcp.exceptions import ToolError`
5. Paste into the corresponding private function in `tools/school.py`
6. Remove the `raise NotImplementedError` placeholder

- [ ] **Step 4: Wire up in tools/__init__.py**

```python
# infra/duckdb-server/tools/__init__.py
from tools.school import school
```

- [ ] **Step 5: Test import**

Run: `python -c "from tools.school import school; print(school.__doc__[:50])"`

Expected: Prints first 50 chars of the docstring without errors.

- [ ] **Step 6: Commit**

```bash
git add tools/school.py tools/__init__.py
git commit -m "feat: extract school() super tool — absorbs school_search, school_report, school_compare, district_report"
```

---

## Task 4: Extract query tool

**Files:**
- Create: `tools/query.py`

Absorbs 7 old tools: `sql_query`, `sql_admin`, `list_schemas`, `list_tables`, `describe_table`, `data_catalog`, `lake_health`. Plus `export_data` via `format` parameter.

- [ ] **Step 1: Identify source functions**

- `sql_query` — line ~3078
- `sql_admin` — line ~3094
- `list_schemas` — line ~3120
- `list_tables` — line ~3134
- `describe_table` — line ~3149
- `data_catalog` — line ~3554
- `lake_health` — line ~13012
- `export_data` — line ~3639

- [ ] **Step 2: Create `tools/query.py` with the super tool function**

Use the exact schema from the spec (Section "Complete Tool Descriptions & Examples", tool #7). The `mode` parameter dispatches to private functions. The `format` parameter triggers export when set to "xlsx" or "csv".

```python
# infra/duckdb-server/tools/query.py
"""query() super tool — SQL execution, schema discovery, catalog search, and data export."""

from typing import Annotated, Literal
from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError

from shared.types import READONLY, MAX_QUERY_ROWS
from shared.db import execute, safe_query
from shared.formatting import make_result, format_text_table
from shared.validation import validate_sql, validate_admin_sql


def query(
    input: Annotated[str, Field(
        description="SQL query, table name, schema name, or search keyword depending on mode, e.g. 'SELECT * FROM lake.housing.hpd_violations LIMIT 10' or 'eviction' or 'housing'",
        examples=[
            "SELECT borough, COUNT(*) FROM lake.housing.hpd_violations GROUP BY borough",
            "SELECT contributor, amount FROM lake.city_government.campaign_contributions WHERE contributor ILIKE '%blackstone%'",
            "SELECT zipcode, COUNT(*) as cnt FROM lake.social_services.n311_service_requests WHERE complaint_type ILIKE '%noise%' GROUP BY zipcode ORDER BY cnt DESC",
            "SELECT * FROM lake.public_safety.motor_vehicle_collisions WHERE crash_date > '2025-01-01' LIMIT 20",
            "eviction",
            "housing",
        ],
    )],
    mode: Annotated[
        Literal["sql", "catalog", "schemas", "tables", "describe", "health", "admin"],
        Field(
            default="sql",
            description="'sql' executes read-only SQL against the lake. 'catalog' searches table names and descriptions by keyword. 'schemas' lists all schemas. 'tables' lists tables in a schema. 'describe' shows columns and types for a table. 'health' shows lake freshness and row counts. 'admin' executes DDL (CREATE OR REPLACE VIEW only).",
        )
    ] = "sql",
    format: Annotated[
        Literal["text", "xlsx", "csv"],
        Field(
            default="text",
            description="'text' returns formatted results for conversation. 'xlsx' returns a branded Excel download URL with hyperlinks and heatmaps. 'csv' returns a plain CSV download URL.",
        )
    ] = "text",
    ctx: Context = None,
) -> ToolResult:
    """Execute SQL queries, discover schemas, search the catalog, and export data from the NYC data lake. 294 tables across 14 schemas in the 'lake' database using lake.schema.table format. Common schemas: housing (violations, permits, ACRIS sales), public_safety (arrests, complaints), education (test scores), health (inspections, COVID), city_government (contracts, payroll). Start with limit=20 for exploration. Do NOT use for building, entity, or neighborhood lookups since the domain tools are faster and richer. Use query for custom analysis the domain tools do not cover."""
    pool = ctx.lifespan_context["pool"]
    catalog = ctx.lifespan_context.get("catalog", {})

    if mode == "sql":
        return _sql(pool, input, format, ctx)
    elif mode == "admin":
        return _admin(pool, input)
    elif mode == "catalog":
        return _catalog(pool, input, catalog)
    elif mode == "schemas":
        return _schemas(catalog)
    elif mode == "tables":
        return _tables(pool, input, catalog)
    elif mode == "describe":
        return _describe(pool, input, catalog)
    elif mode == "health":
        return _health(pool)
    else:
        raise ToolError(f"Unknown mode '{mode}'. Valid modes: sql, catalog, schemas, tables, describe, health, admin.")

# --- Private query functions ---
# Extract SQL from the corresponding old tool functions in mcp_server.py.
# Each function should be a direct copy of the old tool's body,
# with imports updated to use shared.db and shared.formatting.
```

- [ ] **Step 3: Extract SQL from monolith for each mode**

Read each old tool function and copy its body into the corresponding private function. Update imports.

- [ ] **Step 4: Wire up in tools/__init__.py**

```python
from tools.school import school
from tools.query import query
```

- [ ] **Step 5: Test import**

Run: `python -c "from tools.query import query; print(query.__doc__[:50])"`

- [ ] **Step 6: Commit**

```bash
git add tools/query.py tools/__init__.py
git commit -m "feat: extract query() super tool — absorbs sql_query, list_schemas, list_tables, describe_table, data_catalog, lake_health, sql_admin, export_data"
```

---

## Task 5: Extract suggest tool (new, built from scratch)

**Files:**
- Create: `tools/suggest.py`

New tool — no old code to extract. Uses Lance catalog embeddings for semantic topic matching.

- [ ] **Step 1: Create `tools/suggest.py`**

```python
# infra/duckdb-server/tools/suggest.py
"""suggest() super tool — onboarding, discovery, and tool recommendations."""

from typing import Annotated
from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult

from shared.types import LANCE_DIR, LANCE_CATALOG_DISTANCE


# Pre-built exploration suggestions (curated findings)
_EXPLORATIONS = [
    {"title": "Worst landlords in NYC", "call": "network(name='', type='worst')", "desc": "Ranked by violations, litigation, and rent stabilization losses"},
    {"title": "Gentrification pressure in East Harlem", "call": "neighborhood('10029', view='gentrification')", "desc": "Displacement signals: rising rents, eviction rates, demographic shifts"},
    {"title": "NYPD misconduct patterns", "call": "entity(name='', role='cop')", "desc": "Officers with most CCRB complaints, federal lawsuits, and settlements"},
    {"title": "Shell company networks", "call": "network(name='', type='corporate')", "desc": "LLCs with shared officers hiding property ownership"},
    {"title": "School performance gaps", "call": "school('district 7')", "desc": "South Bronx schools: test scores, absenteeism, and survey results"},
]

# Topic → tool recommendations
_TOPIC_MAP = {
    "corruption": [
        ("network(type='political')", "Campaign donation and lobbying networks"),
        ("entity(role='cop')", "Police misconduct records"),
        ("legal(view='settlements')", "Settlement payments by the city"),
        ("civic(view='contracts')", "City contracts and vendor payments"),
    ],
    "housing": [
        ("building('address')", "Building profile with violations and landlord"),
        ("network(type='ownership')", "Landlord portfolio and ownership graph"),
        ("neighborhood('zip', view='gentrification')", "Displacement and gentrification signals"),
        ("services(view='shelter')", "Shelters and housing assistance"),
    ],
    "safety": [
        ("safety('precinct')", "Crime and arrest trends by precinct"),
        ("safety(view='crashes')", "Motor vehicle collision data"),
        ("entity(role='cop')", "Individual officer misconduct"),
        ("legal(view='claims')", "Claims against the city for injuries"),
    ],
    "education": [
        ("school('name or DBN')", "Individual school performance"),
        ("school('district N')", "District-wide aggregates"),
        ("school('zip')", "Schools near a location"),
    ],
    "health": [
        ("health('zip')", "Community health profile"),
        ("health(view='covid')", "COVID data by ZIP"),
        ("health(view='facilities')", "Hospitals and clinics nearby"),
        ("health(view='environmental')", "Lead, asthma, and air quality"),
    ],
}


def suggest(
    topic: Annotated[str, Field(
        description="Topic to explore, or empty for a general overview, e.g. 'corruption', 'rent stabilization', 'environmental justice'",
        examples=["corruption", "rent stabilization", "environmental justice", "worst landlords", "school quality"],
    )] = "",
    ctx: Context = None,
) -> ToolResult:
    """Discover what the NYC data lake can tell you and get guided to the right tools. Returns curated findings, tool recommendations, and ready-to-use example calls. Use when unsure which tool fits, when exploring a new topic, or when onboarding a new user. This tool never returns raw data, only recommendations. For actual data retrieval, use the recommended tool instead."""
    if not topic:
        return _overview()
    return _topic_guide(topic, ctx)


def _overview() -> ToolResult:
    """General overview with curated findings."""
    lines = ["Common Ground NYC Data Lake: 294 tables, 60M+ rows across 14 schemas.", ""]
    lines.append("Try these explorations:")
    for exp in _EXPLORATIONS:
        lines.append(f"  {exp['title']}: {exp['call']}")
        lines.append(f"    {exp['desc']}")
    lines.append("")
    lines.append("Or ask about a topic: suggest('corruption'), suggest('housing'), suggest('health')")
    return ToolResult(content="\n".join(lines))


def _topic_guide(topic: str, ctx) -> ToolResult:
    """Topic-specific tool recommendations. Uses Lance if available for semantic matching."""
    topic_lower = topic.lower().strip()

    # Direct match in topic map
    for key, recommendations in _TOPIC_MAP.items():
        if key in topic_lower:
            lines = [f"Tools for '{topic}':", ""]
            for call, desc in recommendations:
                lines.append(f"  {call}")
                lines.append(f"    {desc}")
            return ToolResult(content="\n".join(lines))

    # Try Lance semantic matching against catalog embeddings
    embed_fn = ctx.lifespan_context.get("embed_fn") if ctx else None
    if embed_fn:
        try:
            from embedder import vec_to_sql
            query_vec = embed_fn(topic)
            vec_literal = vec_to_sql(query_vec)
            pool = ctx.lifespan_context["pool"]
            sql = f"""
                SELECT schema_name, table_name, description, _distance
                FROM lance_vector_search('{LANCE_DIR}/catalog_embeddings.lance', 'embedding', {vec_literal}, k=5)
                WHERE _distance < {LANCE_CATALOG_DISTANCE}
                ORDER BY _distance ASC
            """
            from shared.db import execute
            cols, rows = execute(pool, sql)
            if rows:
                lines = [f"Tables related to '{topic}':", ""]
                for row in rows:
                    schema, table, desc, dist = row
                    lines.append(f"  lake.{schema}.{table}")
                    lines.append(f"    {desc}")
                lines.append("")
                lines.append("Use query(input='SELECT * FROM lake.schema.table LIMIT 20') to explore.")
                return ToolResult(content="\n".join(lines))
        except Exception:
            pass

    # Fallback
    return ToolResult(
        content=f"No specific guide for '{topic}'. Try:\n"
        f"  semantic_search('{topic}') to find related content\n"
        f"  query(input='{topic}', mode='catalog') to search table names\n"
        f"  suggest() for a general overview"
    )
```

- [ ] **Step 2: Wire up in tools/__init__.py**

```python
from tools.school import school
from tools.query import query
from tools.suggest import suggest
```

- [ ] **Step 3: Commit**

```bash
git add tools/suggest.py tools/__init__.py
git commit -m "feat: add suggest() super tool — onboarding, topic discovery, tool recommendations"
```

---

## Task 6-16: Extract remaining 11 tools

Each of the following tools follows the same extraction pattern as Task 3 (school). For each tool:

1. Read the spec section for the tool's schema (docstring, params, views)
2. Read the old tool functions from `mcp_server.py` (use the line numbers from the grep output)
3. Create `tools/toolname.py` with the super tool function and private helpers
4. Copy SQL queries from old functions into private helpers
5. Update imports to use `shared.db`, `shared.formatting`, etc.
6. Wire up in `tools/__init__.py`
7. Test import
8. Commit

### Task 6: Extract semantic_search tool
- Source: `text_search` (~3703), `semantic_search` (~3826), `fuzzy_entity_search` (~11064), `suggest_explorations` (~3620)
- Create: `tools/semantic_search.py`
- Dependencies: `shared/lance.py` for vector search
- Commit: `feat: extract semantic_search() super tool — absorbs text_search, semantic_search, fuzzy_entity_search, suggest_explorations`

### Task 7: Extract building tool
- Source: `building_profile` (~3207), `address_lookup` (~3393), `building_story` (~8254), `building_context` (~9248), `block_timeline` (~9026), `nyc_twins` (~8848), `similar_buildings` (~8439), `owner_violations` (~3526), `enforcement_web` (~11446), `property_history` (~11270), `flipper_detector` (~11730)
- Create: `tools/building.py`
- Dependencies: `shared/db.py`, `shared/lance.py` (for similar view), `shared/formatting.py`
- Commit: `feat: extract building() super tool — absorbs 11 building/housing tools`

### Task 8: Extract neighborhood tool
- Source: `neighborhood_portrait` (~8629), `neighborhood_compare` (~4131), `gentrification_tracker` (~4479), `environmental_justice` (~5686), `hotspot_map` (~12931), `area_snapshot` (~4008), `restaurant_lookup` (~3903), `complaints_by_zip` (~3495)
- Create: `tools/neighborhood.py`
- Dependencies: `shared/db.py`, `spatial.py` (H3), `shared/formatting.py`
- Note: Absorbs `restaurant_lookup`, `complaints_by_zip` from old search/neighborhood tools
- Commit: `feat: extract neighborhood() super tool — absorbs 8 area/neighborhood tools`

### Task 9: Extract entity tool
- Source: `entity_xray` (~10185), `person_crossref` (~12671), `due_diligence` (~7056), `cop_sheet` (~7286), `judge_profile` (~7757), `vital_records` (~7452), `marriage_search` (~11137), `name_variants` (~12977), `top_crossrefs` (~12834)
- Create: `tools/entity.py`
- Dependencies: `shared/lance.py` (routing), `entity.py` (fuzzy SQL), `shared/db.py`
- Commit: `feat: extract entity() super tool — absorbs 10 person/company tools`

### Task 10: Extract safety tool (new domain)
- Source: `safety_report` (~4940), plus NEW queries for crashes, shootings, force, hate, summons
- Create: `tools/safety.py`
- Tables: nypd_complaints_historic/ytd, nypd_arrests_historic/ytd, shootings, motor_vehicle_collisions, hate_crimes, use_of_force_*, criminal_court_summons, discipline_*
- The implementing agent must write NEW SQL queries for the views that have no existing tool (crashes, shootings, force, hate, summons). Pattern: SELECT key columns, WHERE filter by location, ORDER BY date DESC, LIMIT 20.
- Commit: `feat: add safety() super tool — crime, crashes, shootings, use of force`

### Task 11: Extract health tool (new domain)
- Create: `tools/health.py`
- Tables: cdc_places, covid_by_zip, covid_outcomes, wastewater_sarscov2, health_facilities, medicaid_providers, cooling_tower_inspections, drinking_water_tanks, rodent_inspections, ed_flu_visits, hiv_aids_*, sparcs_discharges_*, asthma_ed, lead_children, community_health_survey, beach_water_samples, pregnancy_mortality, epa_echo_facilities
- All NEW SQL queries. Pattern: location-filtered queries against health tables.
- Commit: `feat: add health() super tool — public health, COVID, facilities, environmental health`

### Task 12: Extract legal tool (new domain)
- Create: `tools/legal.py`
- Tables: civil_litigation, settlement_payments, oath_hearings, oath_trials, daily_inmates, nyc_claims_report, nys_coelig_enforcement, ccrb_penalties, vacate_relocation, emergency_repair_hwo/omo
- All NEW SQL queries. Pattern: name/keyword-filtered queries against legal tables.
- Commit: `feat: add legal() super tool — litigation, settlements, hearings, claims`

### Task 13: Extract civic tool (new domain)
- Create: `tools/civic.py`
- Tables: contract_awards, ddc_vendor_payments, covid_emergency_contracts, usaspending_*, film_permits, permitted_events, nys_liquor_authority, citywide_payroll, civil_service_*, nyc_jobs, revenue_budget, nys_ida_projects, nys_procurement_*, city_record, laus, oews, qcew_annual, occupational_projections
- All NEW SQL queries.
- Commit: `feat: add civic() super tool — contracts, permits, jobs, budget, events`

### Task 14: Extract transit tool (new domain)
- Create: `tools/transit.py`
- Tables: parking_violations, mta_daily_ridership, mta_entrances, traffic_volume, pothole_orders, pavement_rating, pedestrian_ramps, ferry_ridership, nrel_alt_fuel_stations
- All NEW SQL queries.
- Commit: `feat: add transit() super tool — parking, ridership, traffic, infrastructure`

### Task 15: Extract services tool (new domain)
- Create: `tools/services.py`
- Source: `resource_finder` (~6012) for the "full" view, plus NEW queries for childcare, food, shelter, benefits, legal_aid, community
- Tables: dycd_program_sites, snap_centers, benefits_centers, family_justice_centers, nys_child_care, childcare_inspections, community_gardens, community_orgs, farmers_markets, literacy_programs, dhs_daily_report, dhs_shelter_census, access_nyc, shop_healthy, snap_access_index, know_your_rights, hud_public_housing_*, housing_connect_*
- Commit: `feat: add services() super tool — childcare, food, shelters, benefits, community`

### Task 16: Extract network tool (most complex, last)
- Source: `landlord_watchdog` (~5344), `landlord_network` (~9405), `ownership_graph` (~9666), `ownership_clusters` (~9717), `ownership_cliques` (~9797), `worst_landlords` (~9848), `llc_piercer` (~9977), `corporate_web` (~12070), `shell_detector` (~12585), `transaction_network` (~11948), `pay_to_play` (~12220), `money_trail` (~7595), `contractor_network` (~12425), `tradewaste_network` (~13123), `officer_network` (~13059)
- Create: `tools/network.py`
- Dependencies: `shared/graph.py`, `shared/lance.py`, `shared/db.py`
- Note: `type` param defaults to `"all"` and acts as a FILTER, not a router. Default behavior fans out across all edge types.
- Commit: `feat: extract network() super tool — absorbs 15 graph/network tools, filter semantics`

---

## Task 17: Slim mcp_server.py

**Files:**
- Modify: `mcp_server.py` — delete all old tool functions, keep only lifespan + registration + middleware

- [ ] **Step 1: Update mcp_server.py imports**

Replace all old tool function references with imports from `tools/`:

```python
from tools import (
    building, entity, neighborhood, network, school,
    semantic_search, query, safety, health, legal,
    civic, transit, services, suggest,
)
```

- [ ] **Step 2: Replace BM25SearchTransform with no transforms**

```python
# Before
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    transforms=[
        BM25SearchTransform(max_results=5, always_visible=ALWAYS_VISIBLE),
    ],
)

# After
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    # No transforms — 14 tools fit in context (~3,500 tokens)
)
```

- [ ] **Step 3: Replace tool registrations**

Delete all 63 `@mcp.tool` decorators and their functions. Replace with:

```python
mcp.tool(annotations=READONLY)(building)
mcp.tool(annotations=READONLY)(entity)
mcp.tool(annotations=READONLY)(neighborhood)
mcp.tool(annotations=READONLY)(network)
mcp.tool(annotations=READONLY)(school)
mcp.tool(annotations=READONLY)(semantic_search)
mcp.tool(annotations=READONLY)(query)
mcp.tool(annotations=READONLY)(safety)
mcp.tool(annotations=READONLY)(health)
mcp.tool(annotations=READONLY)(legal)
mcp.tool(annotations=READONLY)(civic)
mcp.tool(annotations=READONLY)(transit)
mcp.tool(annotations=READONLY)(services)
mcp.tool(annotations=READONLY)(suggest)
```

- [ ] **Step 4: Update INSTRUCTIONS constant**

Replace the old routing table with the new one from the spec (Section "Server Instructions").

- [ ] **Step 5: Update middleware imports**

```python
from middleware.response_middleware import OutputFormatterMiddleware
from middleware.citation_middleware import CitationMiddleware
from middleware.freshness_middleware import FreshnessMiddleware
from middleware.percentile_middleware import PercentileMiddleware
```

- [ ] **Step 6: Delete old tool functions**

Delete everything between the end of `app_lifespan` and the middleware section. This is the bulk of the 13,576 lines — all 63 tool functions and their private helpers that have been extracted to `tools/`.

- [ ] **Step 7: Delete old utility function copies**

Delete from mcp_server.py:
- `_vector_expand_names`, `_lance_route_entity` (now in `shared/lance.py`)
- `format_text_table`, `make_result` (now in `shared/formatting.py`)
- `_validate_sql` (now in `shared/validation.py`)
- `_execute`, `_safe_query`, `_fill_placeholders` (now in `shared/db.py`)
- `_build_catalog` stays (it's part of lifespan)

- [ ] **Step 8: Verify final line count**

Run: `wc -l mcp_server.py`

Expected: ~250-350 lines (lifespan + registration + middleware + prompts + custom routes).

- [ ] **Step 9: Delete old middleware copies**

```bash
rm response_middleware.py citation_middleware.py freshness_middleware.py percentile_middleware.py
```

(The originals — `middleware/` directory has the copies.)

- [ ] **Step 10: Commit**

```bash
git add mcp_server.py tools/ shared/ middleware/
git commit -m "refactor: slim mcp_server.py to ~250 lines — 63 tools replaced by 14 super tools"
```

---

## Task 18: Update tools/__init__.py with all 14 exports

**Files:**
- Modify: `tools/__init__.py`

- [ ] **Step 1: Update exports**

```python
# infra/duckdb-server/tools/__init__.py
from tools.building import building
from tools.entity import entity
from tools.neighborhood import neighborhood
from tools.network import network
from tools.school import school
from tools.semantic_search import semantic_search
from tools.query import query
from tools.safety import safety
from tools.health import health
from tools.legal import legal
from tools.civic import civic
from tools.transit import transit
from tools.services import services
from tools.suggest import suggest

__all__ = [
    "building", "entity", "neighborhood", "network", "school",
    "semantic_search", "query", "safety", "health", "legal",
    "civic", "transit", "services", "suggest",
]
```

- [ ] **Step 2: Commit**

```bash
git add tools/__init__.py
git commit -m "feat: export all 14 super tools from tools/__init__.py"
```

---

## Task 19: Integration smoke test

- [ ] **Step 1: Test all imports**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from tools import (
    building, entity, neighborhood, network, school,
    semantic_search, query, safety, health, legal,
    civic, transit, services, suggest,
)
print(f'All 14 tools imported successfully')
for fn in [building, entity, neighborhood, network, school, semantic_search, query, safety, health, legal, civic, transit, services, suggest]:
    assert fn.__doc__, f'{fn.__name__} missing docstring'
    print(f'  {fn.__name__}: OK')
"
```

- [ ] **Step 2: Verify mcp_server.py imports cleanly**

```bash
python -c "import mcp_server; print('Server module OK')"
```

- [ ] **Step 3: Count lines**

```bash
echo "=== mcp_server.py ===" && wc -l mcp_server.py
echo "=== tools/ ===" && wc -l tools/*.py
echo "=== shared/ ===" && wc -l shared/*.py
echo "=== Total ===" && find . -name "*.py" -path "*/tools/*" -o -name "*.py" -path "*/shared/*" -o -name "mcp_server.py" | xargs wc -l | tail -1
```

Expected: mcp_server.py ~250 lines, tools/ ~6000 lines total, shared/ ~500 lines total.

- [ ] **Step 4: Commit final state**

```bash
git add -A
git commit -m "feat: super tools migration complete — 63 tools → 14, 13.5k line monolith → modular architecture"
```

---

## Execution Notes

### Parallelization

Tasks 6-16 (tool extractions) can run in parallel as subagents since each creates an independent file. Dependencies:
- Tasks 1-2 (scaffold) must complete first
- Tasks 3-5 can start after Task 1
- Tasks 6-16 can start after Task 1
- Task 17 (slim monolith) must wait for ALL tool extractions
- Tasks 18-19 must wait for Task 17

### New domain tools (Tasks 10-15)

These tools (safety, health, legal, civic, transit, services) have no existing code to extract. The implementing agent must:
1. Read the spec for the tool's table coverage
2. Check which tables exist in the lake: `query(input='SELECT table_schema, table_name FROM information_schema.tables', mode='sql')`
3. Check column names: `query(input='housing.motor_vehicle_collisions', mode='describe')`
4. Write SQL queries following the patterns established by existing tools (filter by location, ORDER BY date, LIMIT 20)

### Testing on production

The server runs in Docker on Hetzner (178.156.228.119). Full integration testing requires deploying to the server and testing via MCP client. Local testing is limited to import verification since the DuckLake connection requires the Docker environment.
