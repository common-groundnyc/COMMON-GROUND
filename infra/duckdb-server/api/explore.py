"""Column classifier, query builder, and HTTP handlers for the explore UI."""
from __future__ import annotations

import re
import time
from enum import Enum

from starlette.requests import Request
from starlette.responses import JSONResponse


class FilterType(str, Enum):
    GEOGRAPHY = "geography"
    DATE_RANGE = "date_range"
    TEXT_SEARCH = "text_search"
    CATEGORY = "category"
    NUMERIC_RANGE = "numeric_range"


_GEOGRAPHY_NAMES = frozenset({
    "borough", "boro", "borocode", "boro_code", "borough_id",
    "zipcode", "zip_code", "zip", "incident_zip", "facility_zip",
    "postcode", "postal_code",
})

_DATE_TYPES = frozenset({
    "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
})

_TEXT_SEARCH_NAMES = frozenset({
    "address", "street_name", "owner_name", "respondent_name",
    "description", "violation_description", "complaint_description",
    "facility_name", "dba", "business_name", "name",
})

_CATEGORY_NAMES = frozenset({
    "status", "complaint_type", "violation_type", "type", "grade",
    "disposition", "outcome", "offense_description", "law_cat_cd",
    "building_class", "zoning_district", "community_board",
    # Socrata-style compound names (no underscores)
    "violationstatus", "currentstatus", "novtype", "class",
    "rentimpairing", "inspectiontype",
})

# Suffixes that indicate a category column (VARCHAR only)
_CATEGORY_SUFFIXES = ("status", "type", "grade", "class")

_NUMERIC_SUFFIXES = ("_amount", "_count", "_score", "_total", "_penalty", "_fine")

_NUMERIC_NAMES = frozenset({
    "amount", "count", "score", "penalty", "total",
    "units", "stories", "num_floors", "year_built",
})

_NUMERIC_TYPES = frozenset({"INTEGER", "BIGINT", "SMALLINT", "DOUBLE", "FLOAT", "REAL", "NUMERIC", "DECIMAL"})


def classify_column(name: str, dtype: str) -> FilterType | None:
    """Return the FilterType for a column, or None if no match."""
    lower = name.lower()

    # Geography: match by name (works across any type)
    if lower in _GEOGRAPHY_NAMES or lower.endswith("_zip"):
        return FilterType.GEOGRAPHY

    # Date: match by type, or columns with "date" in the name
    if dtype in _DATE_TYPES:
        return FilterType.DATE_RANGE
    if lower.endswith("_date") or lower.endswith("date"):
        return FilterType.DATE_RANGE

    # Category: exact name match OR ends with a category suffix (VARCHAR only)
    if dtype == "VARCHAR":
        if lower in _CATEGORY_NAMES:
            return FilterType.CATEGORY
        if any(lower.endswith(s) for s in _CATEGORY_SUFFIXES) and lower not in _TEXT_SEARCH_NAMES:
            return FilterType.CATEGORY

    # Text search: name match, or contains "description"/"name" (VARCHAR only)
    if dtype == "VARCHAR":
        if lower in _TEXT_SEARCH_NAMES:
            return FilterType.TEXT_SEARCH
        if "description" in lower or (lower.endswith("name") and lower != "streetname"):
            return FilterType.TEXT_SEARCH

    # Numeric: name or suffix match, numeric types only
    if dtype in _NUMERIC_TYPES:
        if lower in _NUMERIC_NAMES:
            return FilterType.NUMERIC_RANGE
        if any(lower.endswith(s) for s in _NUMERIC_SUFFIXES):
            return FilterType.NUMERIC_RANGE

    return None


# ---------------------------------------------------------------------------
# SQL injection guards
# ---------------------------------------------------------------------------

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VALID_TABLE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")

_FILTER_PREFIXES = frozenset({"after", "before", "min", "max", "q"})


def _validate_identifier(name: str, label: str = "column") -> str:
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {label} name: {name!r}")
    return name


def _validate_table(table: str) -> str:
    if not _VALID_TABLE.match(table):
        raise ValueError(f"Invalid table name: {table!r}")
    return table


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

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
    - "column_name"          -> exact match: WHERE "col" = ?
    - "after:column_name"    -> date/numeric >= ?
    - "before:column_name"   -> date/numeric <= ?
    - "min:column_name"      -> numeric >= ?
    - "max:column_name"      -> numeric <= ?
    - "q:column_name"        -> text ILIKE %?%

    Returns (sql, params) with positional ? placeholders.
    All column names are quoted with double-quotes.
    Table is prefixed with lake. (e.g., lake.housing.hpd_violations)
    """
    _validate_table(table)
    full_table = f"lake.{table}"

    clauses: list[str] = []
    params: list = []

    for key, value in filters.items():
        if ":" in key:
            prefix, col = key.split(":", 1)
            if prefix not in _FILTER_PREFIXES:
                raise ValueError(f"Unknown filter prefix: {prefix!r}")
            _validate_identifier(col)
            quoted = f'"{col}"'
            if prefix == "after":
                clauses.append(f"{quoted} >= ?")
                params.append(value)
            elif prefix == "before":
                clauses.append(f"{quoted} <= ?")
                params.append(value)
            elif prefix == "min":
                clauses.append(f"{quoted} >= ?")
                params.append(value)
            elif prefix == "max":
                clauses.append(f"{quoted} <= ?")
                params.append(value)
            elif prefix == "q":
                clauses.append(f"{quoted} ILIKE ?")
                params.append(f"%{value}%")
        else:
            _validate_identifier(key)
            clauses.append(f'"{key}" = ?')
            params.append(value)

    where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""

    order_sql = ""
    if sort is not None:
        _validate_identifier(sort)
        direction = "ASC" if order.lower() == "asc" else "DESC"
        order_sql = f' ORDER BY "{sort}" {direction}'

    offset = (page - 1) * limit
    params.extend([limit, offset])

    sql = f"SELECT * FROM {full_table}{where_sql}{order_sql} LIMIT ? OFFSET ?"
    return sql, params


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

def _cors_headers(cors_origin: str) -> dict:
    return {
        "Access-Control-Allow-Origin": cors_origin,
        "Vary": "Origin",
    }


def _build_provenance(
    schema_name: str,
    table_name: str,
    pool,
) -> dict:
    """Build provenance metadata from source_registry + pipeline state."""
    from source_registry import SOURCE_REGISTRY
    from source_links import DATASET_URLS

    full_key = f"{schema_name}.{table_name}"
    provenance: dict = {"table": full_key}

    # Look up in the comprehensive registry (334 tables, all providers)
    reg = SOURCE_REGISTRY.get(full_key)
    if reg:
        provenance["provider"] = reg["provider"]
        if "source_url" in reg:
            provenance["source_url"] = reg["source_url"]
        if "api_url" in reg:
            provenance["api_url"] = reg["api_url"]
        if "dataset_id" in reg:
            provenance["dataset_id"] = reg["dataset_id"]
    else:
        provenance["provider"] = "unknown"

    # Add key_column from source_links if available (for row-level linking)
    sl_entry = DATASET_URLS.get(table_name)
    if sl_entry and "key_col" in sl_entry:
        provenance["key_column"] = sl_entry["key_col"]

    # Pipeline state for ingestion metadata
    from shared.db import execute
    try:
        _, ps_rows = execute(pool, """
            SELECT last_updated_at, row_count, last_run_at,
                   source_rows, sync_status, source_checked_at
            FROM lake._pipeline_state
            WHERE dataset_name = ?
        """, [full_key])
        if ps_rows:
            row = ps_rows[0]
            provenance["last_ingested"] = str(row[2]) if row[2] else None
            provenance["rows_ingested"] = row[1]
            provenance["source_rows"] = row[3]
            provenance["sync_status"] = row[4]
    except Exception:
        pass

    return provenance


async def handle_table_meta(
    request: Request,
    pool,
    catalog: dict,
    cors_origin: str,
) -> JSONResponse:
    """GET /api/table-meta?table=schema.table_name"""
    table_param = request.query_params.get("table", "").strip()
    if not table_param:
        return JSONResponse(
            {"error": "Missing required query param: table"},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    try:
        _validate_table(table_param)
    except ValueError as exc:
        return JSONResponse(
            {"error": str(exc)},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    schema_name, table_name = table_param.split(".", 1)
    if schema_name not in catalog or table_name not in catalog[schema_name]:
        return JSONResponse(
            {"error": f"Table not found: {table_param}"},
            status_code=404,
            headers=_cors_headers(cors_origin),
        )

    cat_entry = catalog[schema_name][table_name]
    row_count = cat_entry.get("row_count", 0)
    comment = cat_entry.get("comment", None)

    from shared.db import execute  # local import to avoid circular deps at module level

    try:
        col_cols, col_rows = execute(
            pool,
            f"DESCRIBE lake.{schema_name}.{table_name}",
        )
    except Exception as exc:
        return JSONResponse(
            {"error": f"Failed to fetch columns: {exc}"},
            status_code=500,
            headers=_cors_headers(cors_origin),
        )

    # DESCRIBE returns: column_name, column_type, null, key, default, extra
    columns = []
    for row in col_rows:
        col_name, data_type = row[0], row[1]
        filter_type = classify_column(col_name, data_type)
        columns.append({
            "name": col_name,
            "type": data_type,
            "filterable": filter_type.value if filter_type is not None else None,
        })

    provenance = _build_provenance(schema_name, table_name, pool)

    return JSONResponse(
        {
            "table": table_param,
            "row_count": row_count,
            "comment": comment,
            "provenance": provenance,
            "columns": columns,
        },
        headers=_cors_headers(cors_origin),
    )


async def handle_query(
    request: Request,
    pool,
    catalog: dict,
    cors_origin: str,
) -> JSONResponse:
    """POST /api/query  body: {table, filters, sort, order, page, limit}"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            {"error": "Invalid JSON body"},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    table_param = body.get("table", "").strip()
    if not table_param:
        return JSONResponse(
            {"error": "Missing required field: table"},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    try:
        _validate_table(table_param)
    except ValueError as exc:
        return JSONResponse(
            {"error": str(exc)},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    schema_name, table_name = table_param.split(".", 1)
    if schema_name not in catalog or table_name not in catalog[schema_name]:
        return JSONResponse(
            {"error": f"Table not found: {table_param}"},
            status_code=404,
            headers=_cors_headers(cors_origin),
        )

    filters = body.get("filters", {})
    if not isinstance(filters, dict):
        return JSONResponse(
            {"error": "filters must be an object"},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    sort = body.get("sort", None)
    order = body.get("order", "desc")
    page = int(body.get("page", 1))
    limit = min(int(body.get("limit", 20)), 200)

    if page < 1:
        page = 1

    try:
        sql, params = build_filtered_query(table_param, filters, sort, order, page, limit)
    except ValueError as exc:
        return JSONResponse(
            {"error": str(exc)},
            status_code=400,
            headers=_cors_headers(cors_origin),
        )

    from shared.db import execute  # local import to avoid circular deps at module level

    t0 = time.monotonic()
    try:
        cols, rows = execute(pool, sql, params)
    except Exception as exc:
        return JSONResponse(
            {"error": f"Query failed: {exc}"},
            status_code=500,
            headers=_cors_headers(cors_origin),
        )
    query_time_ms = int((time.monotonic() - t0) * 1000)

    # COUNT(*) with same WHERE, no LIMIT/OFFSET
    # Re-derive the WHERE params (everything except the trailing LIMIT/OFFSET pair)
    where_params = params[:-2]
    # Build COUNT query: replace SELECT * FROM ... LIMIT ? OFFSET ? with COUNT
    count_sql = sql.split(" LIMIT ?")[0]
    count_sql = re.sub(r"^SELECT \* FROM", "SELECT COUNT(*) FROM", count_sql)

    total_count = 0
    try:
        _, count_rows = execute(pool, count_sql, where_params)
        if count_rows:
            total_count = count_rows[0][0]
    except Exception:
        pass

    row_dicts = [dict(zip(cols, row)) for row in rows]

    return JSONResponse(
        {
            "columns": list(cols),
            "rows": row_dicts,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "query_time_ms": query_time_ms,
        },
        headers=_cors_headers(cors_origin),
    )
