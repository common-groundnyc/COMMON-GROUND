# Data Explorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a live data explorer page on common-ground.nyc where users land from LLM chat links, view query results from the DuckDB lake with universal filters, select rows, and download CSV.

**Architecture:** Two-part system: (1) new API endpoints on the MCP server (`/api/table-meta`, `/api/query`) that accept structured filter objects and return live query results from DuckLake, (2) a new `/explore` page on the CG website (Next.js 16 + React 19) with TanStack Table, nuqs URL state, and 6 auto-detected universal filters. The LLM constructs a URL with filter params; the explore page parses them and queries live.

**Tech Stack:**
- MCP server: FastMCP 3.1.1, Python, DuckDB/DuckLake, Starlette custom routes
- Website: Next.js 16, React 19, TanStack Table v8, TanStack Virtual v3, nuqs v2, Cloudflare Workers
- No DuckDB-WASM (can't attach DuckLake), no Prefab UI (no download capability), no AG Grid (too heavy)

---

## File Structure

### MCP Server (infra/duckdb-server/)

| File | Responsibility |
|------|----------------|
| `api/explore.py` (create) | `/api/table-meta` and `/api/query` endpoint handlers — column classification, filter→SQL builder, parameterized query execution |
| `api/__init__.py` (create) | Package init |
| `mcp_server.py` (modify ~2098-2110) | Register the two new custom routes, add CORS for them |
| `tests/test_explore_api.py` (create) | Unit tests for filter→SQL builder, column classifier, query validation |

### Website (common-ground-website/)

| File | Responsibility |
|------|----------------|
| `src/app/explore/page.tsx` (create) | Route entry — reads URL params via nuqs, fetches table-meta + query, renders ExploreView |
| `src/components/explore/explore-view.tsx` (create) | Main layout — filter bar + table + download button + row count |
| `src/components/explore/filter-bar.tsx` (create) | Horizontal filter pills with [+ Add filter] dropdown — renders geography, date, text, category, numeric filters |
| `src/components/explore/data-table.tsx` (create) | TanStack Table + Virtual wrapper — sortable columns, row checkboxes, pagination |
| `src/components/explore/download-button.tsx` (create) | "Download selected" → client-side CSV generation from selected rows |
| `src/lib/explore-api.ts` (create) | Typed fetch helpers: `fetchTableMeta()`, `fetchQueryResults()` |
| `package.json` (modify) | Add nuqs, @tanstack/react-table, @tanstack/react-virtual |

---

## Task 1: MCP Server — Column Classifier

**Files:**
- Create: `infra/duckdb-server/api/__init__.py`
- Create: `infra/duckdb-server/api/explore.py`
- Create: `infra/duckdb-server/tests/test_explore_api.py`

- [ ] **Step 1: Write failing tests for column classification**

```python
# tests/test_explore_api.py
"""Tests for the explore API — column classifier and filter builder."""

import pytest
from api.explore import classify_column, FilterType


class TestClassifyColumn:
    """Column classifier detects filter types from name + DuckDB type."""

    def test_borough_varchar_is_geography(self):
        assert classify_column("borough", "VARCHAR") == FilterType.GEOGRAPHY

    def test_boro_varchar_is_geography(self):
        assert classify_column("boro", "VARCHAR") == FilterType.GEOGRAPHY

    def test_zipcode_varchar_is_geography(self):
        assert classify_column("zipcode", "VARCHAR") == FilterType.GEOGRAPHY

    def test_zip_code_varchar_is_geography(self):
        assert classify_column("zip_code", "VARCHAR") == FilterType.GEOGRAPHY

    def test_incident_zip_is_geography(self):
        assert classify_column("incident_zip", "VARCHAR") == FilterType.GEOGRAPHY

    def test_borocode_integer_is_geography(self):
        assert classify_column("borocode", "INTEGER") == FilterType.GEOGRAPHY

    def test_inspection_date_is_date_range(self):
        assert classify_column("inspection_date", "DATE") == FilterType.DATE_RANGE

    def test_created_at_timestamp_is_date_range(self):
        assert classify_column("created_at", "TIMESTAMP") == FilterType.DATE_RANGE

    def test_closed_date_is_date_range(self):
        assert classify_column("closed_date", "DATE") == FilterType.DATE_RANGE

    def test_penalty_amount_double_is_numeric(self):
        assert classify_column("penalty_amount", "DOUBLE") == FilterType.NUMERIC_RANGE

    def test_total_count_integer_is_numeric(self):
        assert classify_column("total_count", "INTEGER") == FilterType.NUMERIC_RANGE

    def test_score_double_is_numeric(self):
        assert classify_column("score", "DOUBLE") == FilterType.NUMERIC_RANGE

    def test_violation_id_bigint_is_none(self):
        assert classify_column("violation_id", "BIGINT") is None

    def test_random_varchar_is_none(self):
        assert classify_column("some_field", "VARCHAR") is None

    def test_address_varchar_is_text_search(self):
        assert classify_column("address", "VARCHAR") == FilterType.TEXT_SEARCH

    def test_owner_name_is_text_search(self):
        assert classify_column("owner_name", "VARCHAR") == FilterType.TEXT_SEARCH

    def test_complaint_type_varchar_is_category(self):
        assert classify_column("complaint_type", "VARCHAR") == FilterType.CATEGORY

    def test_status_varchar_is_category(self):
        assert classify_column("status", "VARCHAR") == FilterType.CATEGORY

    def test_grade_varchar_is_category(self):
        assert classify_column("grade", "VARCHAR") == FilterType.CATEGORY
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_explore_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'api.explore'`

- [ ] **Step 3: Implement column classifier**

```python
# api/__init__.py
"""Explore API — HTTP endpoints for the data explorer page."""
```

```python
# api/explore.py
"""Explore API — table metadata and live query endpoints for common-ground.nyc/explore."""

from __future__ import annotations

import re
from enum import Enum


class FilterType(str, Enum):
    GEOGRAPHY = "geography"
    DATE_RANGE = "date_range"
    TEXT_SEARCH = "text_search"
    CATEGORY = "category"
    NUMERIC_RANGE = "numeric_range"


# --- Column name patterns for each filter type ---

_GEOGRAPHY_NAMES = frozenset({
    "borough", "boro", "borocode", "boro_code", "borough_id",
    "zipcode", "zip_code", "zip", "incident_zip", "facility_zip",
    "postcode", "postal_code",
})

_DATE_TYPES = frozenset({"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"})

_TEXT_SEARCH_NAMES = frozenset({
    "address", "street_name", "owner_name", "respondent_name",
    "description", "violation_description", "complaint_description",
    "facility_name", "dba", "business_name", "name",
})

_CATEGORY_NAMES = frozenset({
    "status", "complaint_type", "violation_type", "type", "grade",
    "disposition", "outcome", "offense_description", "law_cat_cd",
    "building_class", "zoning_district", "community_board",
})

_NUMERIC_SUFFIXES = ("_amount", "_count", "_score", "_total", "_penalty", "_fine")
_NUMERIC_NAMES = frozenset({
    "amount", "count", "score", "penalty", "total", "units",
    "stories", "num_floors", "year_built",
})


def classify_column(name: str, dtype: str) -> FilterType | None:
    """Classify a column into a filter type based on name and DuckDB type."""
    lower = name.lower()
    upper = dtype.upper()

    # Geography — match by name (works across VARCHAR and INTEGER borocodes)
    if lower in _GEOGRAPHY_NAMES or lower.endswith("_zip"):
        return FilterType.GEOGRAPHY

    # Date range — match by type
    if upper in _DATE_TYPES or (upper == "VARCHAR" and lower.endswith("_date")):
        return FilterType.DATE_RANGE

    # Category — exact name match (must be VARCHAR)
    if "VARCHAR" in upper and lower in _CATEGORY_NAMES:
        return FilterType.CATEGORY

    # Text search — exact name match (must be VARCHAR)
    if "VARCHAR" in upper and lower in _TEXT_SEARCH_NAMES:
        return FilterType.TEXT_SEARCH

    # Numeric range — name pattern match (must be numeric type)
    if upper in ("INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT"):
        if lower in _NUMERIC_NAMES or any(lower.endswith(s) for s in _NUMERIC_SUFFIXES):
            return FilterType.NUMERIC_RANGE

    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_explore_api.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/api/__init__.py infra/duckdb-server/api/explore.py infra/duckdb-server/tests/test_explore_api.py
git commit -m "feat(explore): add column classifier for universal filter detection"
```

---

## Task 2: MCP Server — Filter-to-SQL Builder

**Files:**
- Modify: `infra/duckdb-server/api/explore.py`
- Modify: `infra/duckdb-server/tests/test_explore_api.py`

- [ ] **Step 1: Write failing tests for SQL builder**

```python
# Append to tests/test_explore_api.py
from api.explore import build_filtered_query


class TestBuildFilteredQuery:
    """Filter objects are translated to parameterized SQL."""

    def test_no_filters_returns_select_with_limit(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert "SELECT *" in sql
        assert "lake.housing.hpd_violations" in sql
        assert "LIMIT" in sql
        assert "OFFSET" in sql
        assert params == [20, 0]

    def test_borough_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"borough": "MANHATTAN"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert 'WHERE "borough" = ?' in sql
        assert params[0] == "MANHATTAN"

    def test_date_after_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"after:inspection_date": "2024-01-01"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert '"inspection_date" >= ?' in sql
        assert params[0] == "2024-01-01"

    def test_date_before_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"before:inspection_date": "2025-01-01"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert '"inspection_date" <= ?' in sql
        assert params[0] == "2025-01-01"

    def test_text_search_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"q:address": "350 5TH"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert 'ILIKE' in sql
        assert params[0] == "%350 5TH%"

    def test_numeric_min_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"min:penalty_amount": "1000"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert '"penalty_amount" >= ?' in sql

    def test_numeric_max_filter(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={"max:penalty_amount": "5000"},
            sort=None,
            order="desc",
            page=1,
            limit=20,
        )
        assert '"penalty_amount" <= ?' in sql

    def test_sort_asc(self):
        sql, _ = build_filtered_query(
            table="housing.hpd_violations",
            filters={},
            sort="inspection_date",
            order="asc",
            page=1,
            limit=20,
        )
        assert 'ORDER BY "inspection_date" ASC' in sql

    def test_sort_desc(self):
        sql, _ = build_filtered_query(
            table="housing.hpd_violations",
            filters={},
            sort="borough",
            order="desc",
            page=1,
            limit=20,
        )
        assert 'ORDER BY "borough" DESC' in sql

    def test_pagination_page_2(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={},
            sort=None,
            order="desc",
            page=2,
            limit=20,
        )
        # OFFSET should be 20 for page 2
        assert params[-1] == 20

    def test_multiple_filters_combined(self):
        sql, params = build_filtered_query(
            table="housing.hpd_violations",
            filters={
                "borough": "MANHATTAN",
                "status": "Open",
                "after:inspection_date": "2024-01-01",
            },
            sort="inspection_date",
            order="desc",
            page=1,
            limit=20,
        )
        assert sql.count("?") == len(params)
        assert "MANHATTAN" in params
        assert "Open" in params
        assert "2024-01-01" in params

    def test_rejects_invalid_table_name(self):
        with pytest.raises(ValueError, match="Invalid table"):
            build_filtered_query(
                table="housing; DROP TABLE --",
                filters={},
                sort=None,
                order="desc",
                page=1,
                limit=20,
            )

    def test_rejects_invalid_column_in_filter(self):
        with pytest.raises(ValueError, match="Invalid column"):
            build_filtered_query(
                table="housing.hpd_violations",
                filters={"Robert'; DROP TABLE students--": "x"},
                sort=None,
                order="desc",
                page=1,
                limit=20,
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_explore_api.py::TestBuildFilteredQuery -v`
Expected: FAIL — `ImportError: cannot import name 'build_filtered_query'`

- [ ] **Step 3: Implement filter-to-SQL builder**

Append to `api/explore.py`:

```python
# --- Validation ---

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VALID_TABLE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, label: str = "column") -> str:
    """Raise ValueError if name is not a safe SQL identifier."""
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {label} name: {name!r}")
    return name


# --- Filter-to-SQL builder ---

def build_filtered_query(
    table: str,
    filters: dict[str, str],
    sort: str | None,
    order: str,
    page: int,
    limit: int,
) -> tuple[str, list]:
    """Build a parameterized SELECT from structured filters.

    Filter key formats:
    - "column_name"            → exact match (WHERE "col" = ?)
    - "after:column_name"      → date/numeric >= ?
    - "before:column_name"     → date/numeric <= ?
    - "min:column_name"        → numeric >= ?
    - "max:column_name"        → numeric <= ?
    - "q:column_name"          → text ILIKE %?%

    Returns (sql, params) with positional ? placeholders.
    """
    if not _VALID_TABLE.match(table):
        raise ValueError(f"Invalid table name: {table!r}")

    clauses: list[str] = []
    params: list = []

    for key, value in filters.items():
        if ":" in key and key.split(":", 1)[0] in ("after", "before", "min", "max", "q"):
            prefix, col = key.split(":", 1)
            col = _validate_identifier(col)
        else:
            col = _validate_identifier(key)
            prefix = "eq"

        if prefix == "eq":
            clauses.append(f'"{col}" = ?')
            params.append(value)
        elif prefix in ("after", "min"):
            clauses.append(f'"{col}" >= ?')
            params.append(value)
        elif prefix in ("before", "max"):
            clauses.append(f'"{col}" <= ?')
            params.append(value)
        elif prefix == "q":
            clauses.append(f'"{col}" ILIKE ?')
            params.append(f"%{value}%")

    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

    order_dir = "ASC" if order.lower() == "asc" else "DESC"
    order_clause = f' ORDER BY "{_validate_identifier(sort)}" {order_dir}' if sort else ""

    offset = (max(1, page) - 1) * limit
    params.extend([limit, offset])

    sql = f'SELECT * FROM lake.{table}{where}{order_clause} LIMIT ? OFFSET ?'
    return sql, params
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && python -m pytest tests/test_explore_api.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/api/explore.py infra/duckdb-server/tests/test_explore_api.py
git commit -m "feat(explore): add filter-to-SQL builder with parameterized queries"
```

---

## Task 3: MCP Server — Register HTTP Endpoints

**Files:**
- Modify: `infra/duckdb-server/api/explore.py` (add endpoint handler functions)
- Modify: `infra/duckdb-server/mcp_server.py` (~line 2081, after catalog endpoint)

- [ ] **Step 1: Add table-meta and query handler functions to explore.py**

Append to `api/explore.py`:

```python
from starlette.requests import Request
from starlette.responses import JSONResponse


async def handle_table_meta(
    request: Request,
    pool,
    catalog: dict,
    cors_origin: str,
) -> JSONResponse:
    """GET /api/table-meta?table=schema.table_name — return column metadata with filter classifications."""
    headers = {"Access-Control-Allow-Origin": cors_origin, "Vary": "Origin"}

    table_param = request.query_params.get("table", "")
    if not _VALID_TABLE.match(table_param):
        return JSONResponse({"error": "Invalid table parameter"}, status_code=400, headers=headers)

    schema, table_name = table_param.split(".", 1)

    # Verify table exists in catalog
    if schema not in catalog or table_name not in catalog[schema]:
        return JSONResponse({"error": f"Table {table_param} not found"}, status_code=404, headers=headers)

    info = catalog[schema][table_name]

    # Get column info from DuckDB
    from shared.db import execute
    try:
        cols, rows = execute(pool, """
            SELECT column_name, data_type
            FROM duckdb_columns()
            WHERE schema_name = ? AND table_name = ?
            ORDER BY column_index
        """, [schema, table_name])
    except Exception as e:
        # Fallback: try lake-prefixed
        try:
            cols, rows = execute(pool, """
                SELECT column_name, data_type
                FROM duckdb_columns()
                WHERE schema_name = ? AND table_name = ?
                ORDER BY column_index
            """, [schema, table_name])
        except Exception:
            return JSONResponse({"error": str(e)}, status_code=500, headers=headers)

    columns = []
    for row in rows:
        col_name, dtype = row[0], row[1]
        filter_type = classify_column(col_name, dtype)
        col_info = {
            "name": col_name,
            "type": dtype,
            "filterable": filter_type.value if filter_type else None,
        }
        columns.append(col_info)

    result = {
        "table": table_param,
        "row_count": info.get("row_count", 0),
        "comment": info.get("comment", ""),
        "columns": columns,
    }

    return JSONResponse(result, headers=headers)


async def handle_query(
    request: Request,
    pool,
    catalog: dict,
    cors_origin: str,
) -> JSONResponse:
    """POST /api/query — execute a filtered query and return rows."""
    headers = {"Access-Control-Allow-Origin": cors_origin, "Vary": "Origin"}

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400, headers=headers)

    table = body.get("table", "")
    filters = body.get("filters", {})
    sort = body.get("sort")
    order = body.get("order", "desc")
    page = int(body.get("page", 1))
    limit = min(int(body.get("limit", 20)), 200)  # cap at 200 rows per page

    # Validate table exists
    if not _VALID_TABLE.match(table):
        return JSONResponse({"error": "Invalid table"}, status_code=400, headers=headers)
    schema, table_name = table.split(".", 1)
    if schema not in catalog or table_name not in catalog[schema]:
        return JSONResponse({"error": f"Table {table} not found"}, status_code=404, headers=headers)

    try:
        sql, params = build_filtered_query(table, filters, sort, order, page, limit)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400, headers=headers)

    # Execute with count
    from shared.db import execute
    import time
    t0 = time.monotonic()

    try:
        cols, rows = execute(pool, sql, params)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500, headers=headers)

    # Get total count for pagination (separate query, no LIMIT/OFFSET)
    count_sql, count_params = sql, params
    # Strip LIMIT/OFFSET from the query to get total
    where_part = ""
    if " WHERE " in sql:
        where_part = sql.split("FROM lake.")[1].split(" ORDER BY")[0].split(" LIMIT")[0]
        where_part = where_part.split(table, 1)[1] if table in where_part else where_part
    count_query = f"SELECT COUNT(*) FROM lake.{table}"
    if " WHERE " in sql:
        where_clause = sql.split(" WHERE ", 1)[1].split(" ORDER BY")[0].split(" LIMIT")[0]
        count_query += f" WHERE {where_clause}"
        count_params_only = params[:-2]  # strip limit and offset
    else:
        count_params_only = []

    try:
        _, count_rows = execute(pool, count_query, count_params_only)
        total_count = count_rows[0][0] if count_rows else 0
    except Exception:
        total_count = None

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    # Convert rows to list of dicts
    row_dicts = [dict(zip(cols, row)) for row in rows]

    result = {
        "columns": cols,
        "rows": row_dicts,
        "total_count": total_count,
        "page": page,
        "limit": limit,
        "query_time_ms": elapsed_ms,
    }

    return JSONResponse(result, headers=headers)
```

- [ ] **Step 2: Register routes in mcp_server.py**

Add after the existing `/api/catalog` route (~line 2208):

```python
# ---------------------------------------------------------------------------
# Public HTTP API — explore endpoints for data explorer page
# ---------------------------------------------------------------------------

from api.explore import handle_table_meta, handle_query


@mcp.custom_route("/api/table-meta", methods=["GET", "OPTIONS"])
async def table_meta_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse({}, status_code=204, headers={
            "Access-Control-Allow-Origin": _cors_origin(request),
            "Vary": "Origin",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Max-Age": "86400",
        })
    return await handle_table_meta(request, _shared_pool, _shared_catalog, _cors_origin(request))


@mcp.custom_route("/api/query", methods=["POST", "OPTIONS"])
async def query_route(request: Request) -> JSONResponse:
    if request.method == "OPTIONS":
        return JSONResponse({}, status_code=204, headers={
            "Access-Control-Allow-Origin": _cors_origin(request),
            "Vary": "Origin",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "86400",
        })
    return await handle_query(request, _shared_pool, _shared_catalog, _cors_origin(request))
```

- [ ] **Step 3: Test endpoints manually**

Run locally or on the server:
```bash
# Table metadata
curl -s "https://mcp.common-ground.nyc/api/table-meta?table=housing.hpd_violations" | python -m json.tool | head -30

# Filtered query
curl -s -X POST "https://mcp.common-ground.nyc/api/query" \
  -H "Content-Type: application/json" \
  -d '{"table":"housing.hpd_violations","filters":{"borough":"MANHATTAN","status":"Open"},"sort":"inspectiondate","order":"desc","page":1,"limit":5}' | python -m json.tool
```

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/api/explore.py infra/duckdb-server/mcp_server.py
git commit -m "feat(explore): add /api/table-meta and /api/query HTTP endpoints"
```

---

## Task 4: Website — Install Dependencies and Create API Client

**Files:**
- Modify: `common-ground-website/package.json`
- Create: `common-ground-website/src/lib/explore-api.ts`

- [ ] **Step 1: Install dependencies**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
npm install nuqs @tanstack/react-table @tanstack/react-virtual
```

- [ ] **Step 2: Create typed API client**

```typescript
// src/lib/explore-api.ts
const API_BASE = "https://mcp.common-ground.nyc";

export interface ColumnMeta {
  name: string;
  type: string;
  filterable: "geography" | "date_range" | "text_search" | "category" | "numeric_range" | null;
}

export interface TableMeta {
  table: string;
  row_count: number;
  comment: string;
  columns: ColumnMeta[];
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  total_count: number | null;
  page: number;
  limit: number;
  query_time_ms: number;
}

export async function fetchTableMeta(table: string): Promise<TableMeta> {
  const res = await fetch(`${API_BASE}/api/table-meta?table=${encodeURIComponent(table)}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function fetchQueryResults(params: {
  table: string;
  filters: Record<string, string>;
  sort?: string;
  order?: string;
  page?: number;
  limit?: number;
}): Promise<QueryResult> {
  const res = await fetch(`${API_BASE}/api/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      table: params.table,
      filters: params.filters,
      sort: params.sort || null,
      order: params.order || "desc",
      page: params.page || 1,
      limit: params.limit || 20,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}
```

- [ ] **Step 3: Commit**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
git add package.json package-lock.json src/lib/explore-api.ts
git commit -m "feat(explore): add API client and install TanStack + nuqs"
```

---

## Task 5: Website — Data Table Component

**Files:**
- Create: `common-ground-website/src/components/explore/data-table.tsx`
- Create: `common-ground-website/src/components/explore/download-button.tsx`

- [ ] **Step 1: Create data table with row selection**

```typescript
// src/components/explore/data-table.tsx
"use client";

import { useMemo, useRef } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
  type RowSelectionState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

interface DataTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
  rowSelection: RowSelectionState;
  onRowSelectionChange: (selection: RowSelectionState) => void;
  onSort: (column: string) => void;
  currentSort: string | null;
  currentOrder: string;
}

export function DataTable({
  columns: colNames,
  rows,
  rowSelection,
  onRowSelectionChange,
  onSort,
  currentSort,
  currentOrder,
}: DataTableProps) {
  const parentRef = useRef<HTMLDivElement>(null);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    const selectCol: ColumnDef<Record<string, unknown>> = {
      id: "select",
      header: ({ table }) => (
        <input
          type="checkbox"
          checked={table.getIsAllRowsSelected()}
          onChange={table.getToggleAllRowsSelectedHandler()}
          style={{ accentColor: "var(--truth)" }}
        />
      ),
      cell: ({ row }) => (
        <input
          type="checkbox"
          checked={row.getIsSelected()}
          onChange={row.getToggleSelectedHandler()}
          style={{ accentColor: "var(--truth)" }}
        />
      ),
      size: 40,
    };

    const dataCols: ColumnDef<Record<string, unknown>>[] = colNames.map((name) => ({
      accessorKey: name,
      header: () => {
        const isActive = currentSort === name;
        const arrow = isActive ? (currentOrder === "asc" ? " ↑" : " ↓") : "";
        return (
          <span
            onClick={() => onSort(name)}
            style={{ cursor: "pointer", userSelect: "none" }}
          >
            {name}{arrow}
          </span>
        );
      },
      cell: ({ getValue }) => {
        const v = getValue();
        if (v === null || v === undefined) return <span style={{ color: "var(--muted-dim)" }}>—</span>;
        return String(v);
      },
    }));

    return [selectCol, ...dataCols];
  }, [colNames, currentSort, currentOrder, onSort]);

  const table = useReactTable({
    data: rows,
    columns,
    state: { rowSelection },
    onRowSelectionChange: (updater) => {
      const next = typeof updater === "function" ? updater(rowSelection) : updater;
      onRowSelectionChange(next);
    },
    getCoreRowModel: getCoreRowModel(),
    getRowId: (_, index) => String(index),
    enableRowSelection: true,
  });

  const { rows: tableRows } = table.getRowModel();

  const virtualizer = useVirtualizer({
    count: tableRows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 36,
    overscan: 20,
  });

  return (
    <div
      ref={parentRef}
      style={{
        overflow: "auto",
        maxHeight: "calc(100vh - 300px)",
        border: "1px solid var(--muted-dim)",
        borderRadius: 4,
      }}
    >
      <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-jetbrains), monospace", fontSize: 13 }}>
        <thead style={{ position: "sticky", top: 0, background: "var(--void)", zIndex: 1 }}>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id}>
              {hg.headers.map((h) => (
                <th
                  key={h.id}
                  style={{
                    padding: "8px 12px",
                    textAlign: "left",
                    borderBottom: "2px solid var(--muted-dim)",
                    color: "var(--lilac)",
                    fontWeight: 600,
                    fontSize: 11,
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    whiteSpace: "nowrap",
                  }}
                >
                  {flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {virtualizer.getVirtualItems().map((vi) => {
            const row = tableRows[vi.index];
            return (
              <tr
                key={row.id}
                style={{
                  background: row.getIsSelected() ? "rgba(45, 212, 191, 0.08)" : "transparent",
                  borderBottom: "1px solid rgba(117, 132, 148, 0.2)",
                }}
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    style={{
                      padding: "6px 12px",
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      maxWidth: 300,
                      color: "var(--warm-white)",
                    }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 2: Create download button**

```typescript
// src/components/explore/download-button.tsx
"use client";

interface DownloadButtonProps {
  rows: Record<string, unknown>[];
  columns: string[];
  selectedIndices: string[];
  tableName: string;
}

export function DownloadButton({ rows, columns, selectedIndices, tableName }: DownloadButtonProps) {
  const count = selectedIndices.length;

  function handleDownload() {
    const selected = count > 0
      ? selectedIndices.map((i) => rows[Number(i)])
      : rows;

    const header = columns.join(",");
    const csvRows = selected.map((row) =>
      columns.map((col) => {
        const v = row[col];
        if (v === null || v === undefined) return "";
        const s = String(v);
        return s.includes(",") || s.includes('"') || s.includes("\n")
          ? `"${s.replace(/"/g, '""')}"`
          : s;
      }).join(",")
    );

    const csv = [header, ...csvRows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${tableName.replace(".", "_")}_export.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <button
      onClick={handleDownload}
      disabled={rows.length === 0}
      style={{
        background: count > 0 ? "var(--cta)" : "transparent",
        color: count > 0 ? "var(--void)" : "var(--muted)",
        border: count > 0 ? "none" : "1px solid var(--muted-dim)",
        padding: "8px 16px",
        borderRadius: 4,
        cursor: rows.length === 0 ? "not-allowed" : "pointer",
        fontFamily: "var(--font-jetbrains), monospace",
        fontSize: 12,
        fontWeight: 600,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
      }}
    >
      {count > 0 ? `Download ${count} selected` : "Download all"}
    </button>
  );
}
```

- [ ] **Step 3: Commit**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
git add src/components/explore/data-table.tsx src/components/explore/download-button.tsx
git commit -m "feat(explore): add data table with row selection and CSV download"
```

---

## Task 6: Website — Filter Bar Component

**Files:**
- Create: `common-ground-website/src/components/explore/filter-bar.tsx`

- [ ] **Step 1: Create filter bar with horizontal pills**

```typescript
// src/components/explore/filter-bar.tsx
"use client";

import { useState } from "react";
import type { ColumnMeta } from "@/lib/explore-api";

interface FilterBarProps {
  columns: ColumnMeta[];
  filters: Record<string, string>;
  onFilterChange: (filters: Record<string, string>) => void;
}

const BOROUGH_OPTIONS = ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"];
const DATE_PRESETS = [
  { label: "Last 30 days", days: 30 },
  { label: "Last year", days: 365 },
  { label: "2024", value: "2024-01-01" },
];

export function FilterBar({ columns, filters, onFilterChange }: FilterBarProps) {
  const [showAdd, setShowAdd] = useState(false);

  const filterableColumns = columns.filter((c) => c.filterable);
  const activeKeys = Object.keys(filters);

  function setFilter(key: string, value: string) {
    onFilterChange({ ...filters, [key]: value });
  }

  function removeFilter(key: string) {
    const next = { ...filters };
    delete next[key];
    onFilterChange(next);
  }

  function clearAll() {
    onFilterChange({});
  }

  const pillStyle = {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    padding: "4px 12px",
    background: "rgba(196, 166, 232, 0.12)",
    border: "1px solid var(--lilac)",
    borderRadius: 20,
    color: "var(--lilac)",
    fontFamily: "var(--font-jetbrains), monospace",
    fontSize: 11,
    textTransform: "uppercase" as const,
    letterSpacing: "0.03em",
  };

  const removeStyle = {
    cursor: "pointer",
    color: "var(--muted)",
    fontWeight: 700,
    fontSize: 14,
    lineHeight: 1,
  };

  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
      <span style={{ color: "var(--muted-dim)", fontFamily: "var(--font-jetbrains), monospace", fontSize: 11, textTransform: "uppercase", letterSpacing: "0.05em" }}>
        Filters:
      </span>

      {activeKeys.map((key) => {
        const displayKey = key.includes(":") ? key.split(":").slice(1).join(":") : key;
        const prefix = key.includes(":") ? key.split(":")[0] : "";
        const label = prefix ? `${displayKey} ${prefix === "after" ? "≥" : prefix === "before" ? "≤" : prefix === "min" ? "≥" : prefix === "max" ? "≤" : prefix === "q" ? "~" : "="} ${filters[key]}` : `${displayKey}: ${filters[key]}`;

        return (
          <span key={key} style={pillStyle}>
            {label}
            <span style={removeStyle} onClick={() => removeFilter(key)}>×</span>
          </span>
        );
      })}

      <button
        onClick={() => setShowAdd(!showAdd)}
        style={{
          background: "transparent",
          border: "1px dashed var(--muted-dim)",
          borderRadius: 20,
          padding: "4px 12px",
          color: "var(--muted)",
          cursor: "pointer",
          fontFamily: "var(--font-jetbrains), monospace",
          fontSize: 11,
          textTransform: "uppercase",
          letterSpacing: "0.03em",
        }}
      >
        + Add filter
      </button>

      {activeKeys.length > 0 && (
        <button
          onClick={clearAll}
          style={{
            background: "transparent",
            border: "none",
            color: "var(--muted-dim)",
            cursor: "pointer",
            fontFamily: "var(--font-jetbrains), monospace",
            fontSize: 11,
            textDecoration: "underline",
          }}
        >
          Clear all
        </button>
      )}

      {showAdd && (
        <div style={{
          position: "absolute",
          top: "100%",
          left: 0,
          background: "var(--void)",
          border: "1px solid var(--muted-dim)",
          borderRadius: 8,
          padding: 12,
          zIndex: 10,
          minWidth: 260,
          marginTop: 4,
        }}>
          {filterableColumns.map((col) => {
            if (col.filterable === "geography" && col.name.toLowerCase().includes("borough")) {
              return (
                <div key={col.name} style={{ marginBottom: 8 }}>
                  <label style={{ color: "var(--lilac)", fontSize: 11, textTransform: "uppercase", fontFamily: "var(--font-jetbrains), monospace" }}>{col.name}</label>
                  <select
                    onChange={(e) => { setFilter(col.name, e.target.value); setShowAdd(false); }}
                    defaultValue=""
                    style={{ display: "block", width: "100%", marginTop: 4, padding: 6, background: "var(--void)", color: "var(--warm-white)", border: "1px solid var(--muted-dim)", borderRadius: 4 }}
                  >
                    <option value="" disabled>Select borough...</option>
                    {BOROUGH_OPTIONS.map((b) => <option key={b} value={b}>{b}</option>)}
                  </select>
                </div>
              );
            }
            if (col.filterable === "geography" && (col.name.toLowerCase().includes("zip"))) {
              return (
                <div key={col.name} style={{ marginBottom: 8 }}>
                  <label style={{ color: "var(--lilac)", fontSize: 11, textTransform: "uppercase", fontFamily: "var(--font-jetbrains), monospace" }}>{col.name}</label>
                  <input
                    placeholder="ZIP code..."
                    onKeyDown={(e) => { if (e.key === "Enter") { setFilter(col.name, (e.target as HTMLInputElement).value); setShowAdd(false); } }}
                    style={{ display: "block", width: "100%", marginTop: 4, padding: 6, background: "var(--void)", color: "var(--warm-white)", border: "1px solid var(--muted-dim)", borderRadius: 4 }}
                  />
                </div>
              );
            }
            if (col.filterable === "date_range") {
              return (
                <div key={col.name} style={{ marginBottom: 8 }}>
                  <label style={{ color: "var(--lilac)", fontSize: 11, textTransform: "uppercase", fontFamily: "var(--font-jetbrains), monospace" }}>{col.name}</label>
                  <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
                    <input
                      type="date"
                      placeholder="After"
                      onChange={(e) => { setFilter(`after:${col.name}`, e.target.value); }}
                      style={{ flex: 1, padding: 6, background: "var(--void)", color: "var(--warm-white)", border: "1px solid var(--muted-dim)", borderRadius: 4 }}
                    />
                    <input
                      type="date"
                      placeholder="Before"
                      onChange={(e) => { setFilter(`before:${col.name}`, e.target.value); setShowAdd(false); }}
                      style={{ flex: 1, padding: 6, background: "var(--void)", color: "var(--warm-white)", border: "1px solid var(--muted-dim)", borderRadius: 4 }}
                    />
                  </div>
                </div>
              );
            }
            if (col.filterable === "text_search") {
              return (
                <div key={col.name} style={{ marginBottom: 8 }}>
                  <label style={{ color: "var(--lilac)", fontSize: 11, textTransform: "uppercase", fontFamily: "var(--font-jetbrains), monospace" }}>{col.name}</label>
                  <input
                    placeholder={`Search ${col.name}...`}
                    onKeyDown={(e) => { if (e.key === "Enter") { setFilter(`q:${col.name}`, (e.target as HTMLInputElement).value); setShowAdd(false); } }}
                    style={{ display: "block", width: "100%", marginTop: 4, padding: 6, background: "var(--void)", color: "var(--warm-white)", border: "1px solid var(--muted-dim)", borderRadius: 4 }}
                  />
                </div>
              );
            }
            return null;
          })}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
git add src/components/explore/filter-bar.tsx
git commit -m "feat(explore): add filter bar with geography, date, text, category filters"
```

---

## Task 7: Website — Explore Page (Main Assembly)

**Files:**
- Create: `common-ground-website/src/components/explore/explore-view.tsx`
- Create: `common-ground-website/src/app/explore/page.tsx`

- [ ] **Step 1: Create the ExploreView component**

```typescript
// src/components/explore/explore-view.tsx
"use client";

import { useState, useEffect, useCallback } from "react";
import { useQueryStates, parseAsString, parseAsInteger } from "nuqs";
import type { RowSelectionState } from "@tanstack/react-table";
import { fetchTableMeta, fetchQueryResults, type TableMeta, type QueryResult } from "@/lib/explore-api";
import { DataTable } from "./data-table";
import { FilterBar } from "./filter-bar";
import { DownloadButton } from "./download-button";

export function ExploreView() {
  const [params, setParams] = useQueryStates({
    table: parseAsString.withDefault(""),
    sort: parseAsString,
    order: parseAsString.withDefault("desc"),
    page: parseAsInteger.withDefault(1),
  });

  // Filters are all other URL params (not table/sort/order/page)
  const [filterParams, setFilterParams] = useQueryStates({});

  const [meta, setMeta] = useState<TableMeta | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});

  // Extract filters from URL (everything except reserved keys)
  const RESERVED = new Set(["table", "sort", "order", "page", "limit"]);
  const urlParams = new URLSearchParams(typeof window !== "undefined" ? window.location.search : "");
  const filters: Record<string, string> = {};
  urlParams.forEach((value, key) => {
    if (!RESERVED.has(key)) filters[key] = value;
  });

  // Load table metadata
  useEffect(() => {
    if (!params.table) return;
    fetchTableMeta(params.table)
      .then(setMeta)
      .catch((e) => setError(e.message));
  }, [params.table]);

  // Load query results
  useEffect(() => {
    if (!params.table) return;
    setLoading(true);
    setError(null);
    setRowSelection({});
    fetchQueryResults({
      table: params.table,
      filters,
      sort: params.sort || undefined,
      order: params.order,
      page: params.page,
      limit: 50,
    })
      .then((r) => { setResult(r); setLoading(false); })
      .catch((e) => { setError(e.message); setLoading(false); });
  }, [params.table, params.sort, params.order, params.page, JSON.stringify(filters)]);

  const handleSort = useCallback((column: string) => {
    const newOrder = params.sort === column && params.order === "asc" ? "desc" : "asc";
    setParams({ sort: column, order: newOrder, page: 1 });
  }, [params.sort, params.order, setParams]);

  const handleFilterChange = useCallback((newFilters: Record<string, string>) => {
    // Update URL with new filters
    const url = new URL(window.location.href);
    // Remove old filter params
    Array.from(url.searchParams.keys()).forEach((k) => {
      if (!RESERVED.has(k)) url.searchParams.delete(k);
    });
    // Add new filter params
    Object.entries(newFilters).forEach(([k, v]) => url.searchParams.set(k, v));
    url.searchParams.set("page", "1");
    window.history.pushState({}, "", url.toString());
    window.dispatchEvent(new PopStateEvent("popstate"));
  }, []);

  const handlePageChange = useCallback((newPage: number) => {
    setParams({ page: newPage });
  }, [setParams]);

  if (!params.table) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>
        <p style={{ fontFamily: "var(--font-jetbrains), monospace", fontSize: 14 }}>
          No table selected. This page is accessed via links from the Common Ground AI.
        </p>
      </div>
    );
  }

  const totalPages = result?.total_count ? Math.ceil(result.total_count / (result.limit || 50)) : 0;
  const selectedIndices = Object.keys(rowSelection).filter((k) => rowSelection[k]);

  return (
    <div style={{ padding: "24px 32px", maxWidth: 1400, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <h1 style={{
          fontFamily: "var(--font-space-grotesk), sans-serif",
          fontSize: 24,
          fontWeight: 700,
          color: "var(--warm-white)",
          margin: 0,
        }}>
          {params.table}
        </h1>
        {meta?.comment && (
          <p style={{ color: "var(--muted)", fontSize: 13, marginTop: 4 }}>{meta.comment}</p>
        )}
        {meta && (
          <p style={{
            fontFamily: "var(--font-jetbrains), monospace",
            fontSize: 11,
            color: "var(--muted-dim)",
            marginTop: 4,
            textTransform: "uppercase",
            letterSpacing: "0.05em",
          }}>
            {meta.row_count.toLocaleString()} total rows
          </p>
        )}
      </div>

      {/* Filter bar */}
      {meta && (
        <div style={{ position: "relative", marginBottom: 16 }}>
          <FilterBar
            columns={meta.columns}
            filters={filters}
            onFilterChange={handleFilterChange}
          />
        </div>
      )}

      {/* Status bar */}
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: 12,
      }}>
        <span style={{
          fontFamily: "var(--font-jetbrains), monospace",
          fontSize: 11,
          color: "var(--muted-dim)",
          textTransform: "uppercase",
          letterSpacing: "0.05em",
        }}>
          {loading ? "Loading..." : result ? `Showing ${result.rows.length} of ${result.total_count?.toLocaleString() ?? "?"} rows · ${result.query_time_ms}ms` : ""}
        </span>
        {result && (
          <DownloadButton
            rows={result.rows}
            columns={result.columns}
            selectedIndices={selectedIndices}
            tableName={params.table}
          />
        )}
      </div>

      {/* Error */}
      {error && (
        <div style={{ padding: 16, background: "rgba(255,0,0,0.1)", border: "1px solid red", borderRadius: 4, marginBottom: 12, color: "#ff6b6b", fontSize: 13 }}>
          {error}
        </div>
      )}

      {/* Table */}
      {result && (
        <DataTable
          columns={result.columns}
          rows={result.rows}
          rowSelection={rowSelection}
          onRowSelectionChange={setRowSelection}
          onSort={handleSort}
          currentSort={params.sort}
          currentOrder={params.order}
        />
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginTop: 12,
          fontFamily: "var(--font-jetbrains), monospace",
          fontSize: 12,
          color: "var(--muted)",
        }}>
          <button
            onClick={() => handlePageChange(params.page - 1)}
            disabled={params.page <= 1}
            style={{ background: "transparent", border: "1px solid var(--muted-dim)", borderRadius: 4, padding: "4px 12px", color: "var(--muted)", cursor: params.page <= 1 ? "not-allowed" : "pointer" }}
          >
            ← Prev
          </button>
          <span>Page {params.page} of {totalPages}</span>
          <button
            onClick={() => handlePageChange(params.page + 1)}
            disabled={params.page >= totalPages}
            style={{ background: "transparent", border: "1px solid var(--muted-dim)", borderRadius: 4, padding: "4px 12px", color: "var(--muted)", cursor: params.page >= totalPages ? "not-allowed" : "pointer" }}
          >
            Next →
          </button>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Create the route page**

```typescript
// src/app/explore/page.tsx
import { Suspense } from "react";
import { ExploreView } from "@/components/explore/explore-view";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Data Explorer — Common Ground",
  description: "Explore NYC open data — filter, sort, and download from 294 tables and 60M+ records.",
  openGraph: {
    title: "Data Explorer — Common Ground",
    description: "Explore NYC open data — filter, sort, and download.",
    siteName: "Common Ground",
  },
};

export default function ExplorePage() {
  return (
    <div style={{
      background: "var(--void)",
      color: "var(--warm-white)",
      minHeight: "100vh",
    }}>
      <Suspense fallback={
        <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>Loading explorer...</div>
      }>
        <ExploreView />
      </Suspense>
    </div>
  );
}
```

- [ ] **Step 3: Verify it builds**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
npm run build
```

Expected: Build succeeds (may show type warnings but should not fail)

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/common-ground-website
git add src/components/explore/explore-view.tsx src/app/explore/page.tsx
git commit -m "feat(explore): add /explore page with live filters, table, and CSV download"
```

---

## Task 8: Integration — Update MCP Tool Responses with Explore Links

**Files:**
- Modify: `infra/duckdb-server/tools/query.py` (add explore link to export responses)

- [ ] **Step 1: Add explore_link helper to query.py**

At the top of `tools/query.py`, after imports:

```python
from urllib.parse import urlencode


def explore_link(table: str, filters: dict | None = None) -> str:
    """Generate a common-ground.nyc/explore URL with pre-applied filters."""
    params = {"table": table}
    if filters:
        params.update(filters)
    qs = urlencode({k: v for k, v in params.items() if v})
    return f"https://common-ground.nyc/explore?{qs}"
```

- [ ] **Step 2: Update query tool response to include explore link**

In the `_sql()` function, where it returns the export message (after generating CSV/XLSX), append the explore link:

Find the section that returns the export confirmation string and add the link. The exact line depends on current code structure, but the pattern is:

```python
# After export file is written, before returning:
link = explore_link(f"{schema}.{table_name}")
return f"... existing export message ...\n\nExplore live: {link}"
```

- [ ] **Step 3: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/query.py
git commit -m "feat(explore): add explore links to query tool export responses"
```

---

Plan complete and saved to `docs/superpowers/plans/2026-04-02-data-explorer.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?