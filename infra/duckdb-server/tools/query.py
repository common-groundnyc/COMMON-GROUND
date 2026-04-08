"""query() super tool — absorbs sql_query, sql_admin, list_schemas, list_tables,
describe_table, data_catalog, lake_health, and export_data into one dispatch."""

import os
import re
import time
from typing import Annotated, Literal
from urllib.parse import urlencode

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, CursorPool
from shared.formatting import make_result
from shared.types import (
    MAX_QUERY_ROWS,
    VS_CATALOG_DISTANCE,
)
from shared.validation import validate_sql, validate_admin_sql

# ---------------------------------------------------------------------------
# Explore link helper
# ---------------------------------------------------------------------------


def explore_link(table: str, filters: dict | None = None) -> str:
    """Generate a common-ground.nyc/explore URL with pre-applied filters."""
    params = {"table": table}
    if filters:
        params.update(filters)
    qs = urlencode({k: v for k, v in params.items() if v})
    return f"https://common-ground.nyc/explore?{qs}"


# ---------------------------------------------------------------------------
# Schema metadata (duplicated from mcp_server.py — shared source of truth)
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTIONS = {
    "business": "BLS employment stats, ACS census demographics, business licenses, M/WBE certs. 15 tables. Ask: which industries are growing? where are minority-owned businesses concentrated?",
    "city_government": "PLUTO lot data (every parcel in NYC), city payroll, OATH hearings, zoning, facilities. 25 tables, 3M+ rows. Ask: who owns this lot? what's the zoning? how much do city employees earn?",
    "education": "DOE school surveys, test scores, enrollment, NYSED data, College Scorecard. 20 tables. Ask: how do schools compare? what's the graduation rate? which schools are overcrowded?",
    "environment": "Air quality, tree census (680k trees), energy benchmarking, FEMA flood zones, EPA enforcement. 18 tables. Ask: what's the air quality here? which buildings waste the most energy?",
    "financial": "CFPB consumer complaints with full narratives (searchable via text_search). 1 table, 1.2M rows. Ask: what financial companies get the most complaints? what are people complaining about?",
    "health": "Restaurant inspections (27k restaurants, letter grades), rat inspections, community health indicators. 10 tables. Ask: is this restaurant safe? where are the worst rat problems?",
    "housing": "HPD complaints/violations, DOB permits, evictions, NYCHA, HMDA mortgages, ACRIS transactions (85M records since 1966). 40 tables, 30M+ rows. THE richest schema. Ask: who owns this building? how many violations? when was it last sold? who's the worst landlord?",
    "public_safety": "NYPD crimes/arrests/shootings, motor vehicle collisions, hate crimes. 12 tables, 10M+ rows. Ask: is this neighborhood safe? what crimes are most common? how do precincts compare?",
    "recreation": "Parks, pools, permits, events. 8 tables. Ask: what parks are nearby? what events are happening?",
    "social_services": "311 service requests (30M+ rows), food assistance, childcare. 8 tables. Ask: what do people complain about? where are food deserts?",
    "transportation": "MTA ridership, parking tickets (40M+), traffic speeds, street conditions. 15 tables. Ask: which subway stations are busiest? where do people get the most parking tickets?",
}

_HIDDEN_SCHEMAS = frozenset({
    "pg_catalog", "information_schema", "ducklake", "public",
})

DATA_CATALOG_SQL = """
WITH table_matches AS (
    SELECT DISTINCT t.schema_name, t.table_name, t.comment,
           t.column_count, t.estimated_size, 'table/comment' AS match_type,
           GREATEST(
               rapidfuzz_partial_ratio(LOWER(t.table_name), LOWER(?)),
               rapidfuzz_partial_ratio(LOWER(COALESCE(t.comment, '')), LOWER(?))
           ) AS score
    FROM duckdb_tables() t
    WHERE t.database_name = 'lake'
      AND t.schema_name NOT IN ('information_schema', 'pg_catalog')
),
column_matches AS (
    SELECT DISTINCT c.schema_name, c.table_name, t.comment,
           t.column_count, t.estimated_size, 'column' AS match_type,
           rapidfuzz_partial_ratio(LOWER(c.column_name), LOWER(?)) AS score
    FROM duckdb_columns() c
    JOIN duckdb_tables() t ON c.schema_name = t.schema_name
         AND c.table_name = t.table_name AND t.database_name = 'lake'
    WHERE c.database_name = 'lake'
),
h3_tables AS (
    SELECT DISTINCT source_table, COUNT(*) AS geo_rows
    FROM lake.foundation.h3_index
    GROUP BY source_table
),
all_matches AS (
    SELECT schema_name, table_name, comment, column_count, estimated_size,
           match_type, MAX(score) AS score
    FROM (
        SELECT * FROM table_matches
        UNION ALL
        SELECT * FROM column_matches
    )
    GROUP BY schema_name, table_name, comment, column_count, estimated_size, match_type
)
SELECT a.schema_name, a.table_name, a.comment, a.estimated_size, a.column_count,
       a.match_type, a.score,
       h.geo_rows IS NOT NULL AS has_geo,
       COALESCE(h.geo_rows, 0) AS geo_row_count
FROM all_matches a
LEFT JOIN h3_tables h ON (a.schema_name || '.' || a.table_name) = h.source_table
WHERE a.score >= 60
ORDER BY a.score DESC, a.estimated_size DESC
LIMIT 30
"""

# ---------------------------------------------------------------------------
# Fuzzy matching helpers
# ---------------------------------------------------------------------------


def _fuzzy_match_schema(pool: CursorPool, input_schema: str, catalog: dict) -> str:
    """Return the best-matching schema name, or raise ToolError with suggestion."""
    if input_schema in catalog:
        return input_schema
    try:
        with pool.cursor() as cur:
            result = cur.execute("""
                SELECT schema_name, rapidfuzz_token_set_ratio(LOWER(?), LOWER(schema_name)) AS score
                FROM (SELECT UNNEST(?::VARCHAR[]) AS schema_name)
                WHERE score >= 60
                ORDER BY score DESC
                LIMIT 1
            """, [input_schema, list(catalog.keys())])
            row = result.fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    raise ToolError(
        f"Schema '{input_schema}' not found. Available: {', '.join(sorted(catalog))}"
    )


def _fuzzy_match_table(pool: CursorPool, input_table: str, schema: str, catalog: dict) -> str:
    """Return the best-matching table name in a schema, or raise ToolError."""
    tables = catalog.get(schema, {})
    if input_table in tables:
        return input_table
    try:
        with pool.cursor() as cur:
            result = cur.execute("""
                SELECT table_name, rapidfuzz_token_set_ratio(LOWER(?), LOWER(table_name)) AS score
                FROM (SELECT UNNEST(?::VARCHAR[]) AS table_name)
                WHERE score >= 50
                ORDER BY score DESC
                LIMIT 1
            """, [input_table, list(tables.keys())])
            row = result.fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    raise ToolError(
        f"Table '{input_table}' not found in '{schema}'. Use list_tables('{schema}') to see available tables."
    )


# ---------------------------------------------------------------------------
# Private mode handlers
# ---------------------------------------------------------------------------


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

    # Export path — xlsx or csv
    if not cols:
        raise ToolError("Query returned no columns. Check your SQL.")

    safe_name = "export"
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    export_dir = "/data/common-ground/exports"
    os.makedirs(export_dir, exist_ok=True)

    if fmt == "xlsx":
        from xlsx_export import generate_branded_xlsx

        xlsx_bytes = generate_branded_xlsx(cols, rows, sql=sql)
        path = os.path.join(export_dir, f"{safe_name}_{timestamp}.xlsx")
        with open(path, "wb") as f:
            f.write(xlsx_bytes)
        return (
            f"Exported {len(rows):,} rows to XLSX ({elapsed}ms).\n\n"
            f"File: {path}\n"
            f"Size: {len(xlsx_bytes):,} bytes\n\n"
            f"Features: branded header, frozen columns, "
            + ("BBL hyperlinks to WhoOwnsWhat, " if any(c.lower() == "bbl" for c in cols) else "")
            + "percentile color scales\n\n"
            f"Preview (first {min(5, len(rows))} rows):\n"
            + _preview_table(cols, rows[:5])
            + (f"\n\n... and {len(rows) - 5:,} more rows" if len(rows) > 5 else "")
        )

    # csv
    from csv_export import generate_branded_csv, write_export

    csv_text = generate_branded_csv(cols, rows, sql=sql)
    path = write_export(csv_text, safe_name, export_dir)
    return (
        f"Exported {len(rows):,} rows to CSV ({elapsed}ms).\n\n"
        f"File: {path}\n"
        f"Size: {len(csv_text):,} bytes"
    )


def _preview_table(cols: list, rows: list) -> str:
    """Quick text preview of a few rows."""
    lines = [" | ".join(str(c) for c in cols)]
    lines.append("-" * len(lines[0]))
    for row in rows:
        lines.append(" | ".join(str(v) if v is not None else "" for v in row))
    return "\n".join(lines)


def _admin(pool: CursorPool, sql: str, ctx: Context) -> str:
    """Execute a DDL statement (CREATE OR REPLACE VIEW only)."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    stripped = re.sub(r"--[^\n]*", " ", stripped)
    stripped = stripped.strip().rstrip(";")

    if ";" in stripped:
        raise ToolError("Only single SQL statements are allowed.")

    validate_admin_sql(stripped)

    with pool.cursor() as cur:
        try:
            cur.execute(stripped)
            # Refresh catalog from the live database
            catalog = ctx.lifespan_context.get("catalog", {})
            db = ctx.lifespan_context.get("db")
            if db is not None:
                from shared.db import build_catalog
                ctx.lifespan_context["catalog"] = build_catalog(db)
            return "OK — view created successfully."
        except Exception as e:
            raise ToolError(f"DDL error: {e}. Only CREATE OR REPLACE VIEW is allowed.")


def _catalog(pool: CursorPool, keyword: str, catalog: dict, embed_fn=None, ctx=None) -> ToolResult:
    """Search the catalog by keyword with fuzzy and semantic matching."""
    if not keyword.strip():
        lines = []
        for schema in sorted(catalog):
            if schema in _HIDDEN_SCHEMAS:
                continue
            tables = catalog[schema]
            total = sum(t["row_count"] for t in tables.values())
            desc = SCHEMA_DESCRIPTIONS.get(schema, "")
            lines.append(
                f"{schema} ({len(tables)} tables, ~{total:,} rows): {desc}"
            )
        return ToolResult(content="\n".join(lines))

    kw = keyword.strip()
    cols, rows = execute(pool, DATA_CATALOG_SQL, [kw, kw, kw])

    if rows:
        schema_idx = cols.index("schema_name")
        rows = [r for r in rows if r[schema_idx] not in _HIDDEN_SCHEMAS]

    # Semantic fallback: when rapidfuzz finds < 3 tables, try vector similarity
    semantic_note = _semantic_fallback(pool, kw, len(rows), embed_fn, ctx=ctx)

    if not rows and semantic_note:
        return ToolResult(content=f"No exact matches for '{kw}', but found related tables:{semantic_note}")

    if not rows:
        return ToolResult(
            content=f"No tables found matching '{kw}'. Try a broader term."
        )

    summary = f"Found {len(rows)} tables matching '{kw}':"
    if semantic_note:
        result = make_result(summary, cols, rows)
        return ToolResult(content=result.content + semantic_note)
    return make_result(summary, cols, rows)


def _semantic_fallback(pool: CursorPool, kw: str, match_count: int, embed_fn=None, ctx=None) -> str:
    """Try vector search on catalog descriptions when fuzzy matches are sparse."""
    if match_count >= 3 or embed_fn is None:
        return ""
    try:
        emb_conn = ctx.lifespan_context.get("emb_conn") if hasattr(ctx, 'lifespan_context') else None
        if not emb_conn:
            return ""
        query_vec = embed_fn(kw)
        sem_rows = emb_conn.execute(
            "SELECT schema_name, table_name, description, "
            "array_cosine_distance(embedding, ?::FLOAT[]) AS distance "
            "FROM catalog_embeddings WHERE table_name != '__schema__' "
            "ORDER BY distance LIMIT 5",
            [query_vec.tolist()],
        ).fetchall()
        good_matches = [(s, t, d) for s, t, d, dist in sem_rows if dist < VS_CATALOG_DISTANCE]
        if good_matches:
            note = "\n\nSemantic matches (by description similarity):\n"
            for s, t, d in good_matches:
                note += f"  {s}.{t}: {d}\n"
            return note
    except Exception:
        pass
    return ""


def _schemas(catalog: dict) -> str:
    """List all schemas with table counts and total rows."""
    lines = []
    for schema in sorted(catalog):
        if schema in _HIDDEN_SCHEMAS:
            continue
        tables = catalog[schema]
        total_rows = sum(t["row_count"] for t in tables.values())
        lines.append(f"{schema}: {len(tables)} tables, ~{total_rows:,} rows")
    return "\n".join(lines)


def _tables(pool: CursorPool, schema_input: str, catalog: dict) -> ToolResult:
    """List tables in a schema with row and column counts."""
    schema = _fuzzy_match_schema(pool, schema_input, catalog)
    tables = catalog[schema]
    cols = ["table_name", "row_count", "column_count"]
    rows = sorted(
        [(name, t["row_count"], t["column_count"]) for name, t in tables.items()],
        key=lambda r: -r[1],
    )
    return make_result(f"Schema '{schema}': {len(tables)} tables.", cols, rows)


def _describe(pool: CursorPool, table_input: str, catalog: dict) -> ToolResult:
    """Show columns, types, and nullability for a table."""
    # Parse "schema.table" or just "table" (search all schemas)
    parts = table_input.strip().split(".")
    if len(parts) >= 2:
        schema = _fuzzy_match_schema(pool, parts[0], catalog)
        table = _fuzzy_match_table(pool, parts[1], schema, catalog)
    else:
        # Try to find the table in any schema
        found = False
        for s in catalog:
            if s in _HIDDEN_SCHEMAS:
                continue
            if table_input in catalog[s]:
                schema, table, found = s, table_input, True
                break
        if not found:
            # Fuzzy match across all schemas
            for s in catalog:
                if s in _HIDDEN_SCHEMAS:
                    continue
                try:
                    table = _fuzzy_match_table(pool, table_input, s, catalog)
                    schema = s
                    found = True
                    break
                except ToolError:
                    continue
            if not found:
                raise ToolError(
                    f"Table '{table_input}' not found. Provide schema.table or use mode='catalog' to search."
                )

    qualified = f"lake.{schema}.{table}"
    cols, rows = execute(
        pool,
        """
        SELECT c.column_name, c.data_type, c.is_nullable, dc.comment
        FROM information_schema.columns c
        LEFT JOIN duckdb_columns() dc
          ON dc.database_name = 'lake'
          AND dc.schema_name = c.table_schema
          AND dc.table_name = c.table_name
          AND dc.column_name = c.column_name
        WHERE c.table_catalog = 'lake'
          AND c.table_schema = ?
          AND c.table_name = ?
        ORDER BY c.ordinal_position
        """,
        [schema, table],
    )
    if not rows:
        raise ToolError(
            f"Table '{qualified}' not found. Use mode='tables' with input='{schema}' to see available tables."
        )

    row_count = catalog.get(schema, {}).get(table, {}).get("row_count")
    row_count_str = f"{row_count:,}" if isinstance(row_count, int) else "unknown"

    # Table-level comment
    table_comment = ""
    try:
        _, tc_rows = execute(pool, """
            SELECT comment FROM duckdb_tables()
            WHERE database_name = 'lake' AND schema_name = ? AND table_name = ?
        """, [schema, table])
        if tc_rows and tc_rows[0][0]:
            table_comment = tc_rows[0][0]
    except Exception:
        pass

    summary = f"{qualified}: {row_count_str} rows, {len(rows)} columns"
    if table_comment:
        summary += f"\n{table_comment}"
    return make_result(summary, cols, rows)


def _health(pool: CursorPool, schema_filter: str | None = None) -> str:
    """Data lake health dashboard — row counts, null rates, freshness."""
    if schema_filter and schema_filter.strip():
        where = "WHERE schema_name = ?"
        params = [schema_filter.strip()]
    else:
        where = ""
        params = []
    cols, raw_rows = execute(pool, f"""
        SELECT schema_name, table_name, row_count, column_count, high_null_columns, profiled_at
        FROM lake.foundation.data_health
        {where}
        ORDER BY row_count DESC
    """, params)
    if not raw_rows:
        return "No health data. Run the foundation_rebuild job first."

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total_rows = sum(r["row_count"] for r in rows)
    unhealthy = [r for r in rows if r["high_null_columns"] > 0]

    lines = [
        f"## Data Lake Health ({len(rows)} tables, {total_rows:,} total rows)\n",
        f"**Tables with high-null columns:** {len(unhealthy)}\n",
        "| Schema | Table | Rows | Cols | High-Null Cols |",
        "|--------|-------|------|------|----------------|",
    ]
    for r in rows[:30]:
        flag = " !" if r["high_null_columns"] > 0 else ""
        lines.append(
            f"| {r['schema_name']} | {r['table_name']} | {r['row_count']:,} | "
            f"{r['column_count']} | {r['high_null_columns']}{flag} |"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public super tool
# ---------------------------------------------------------------------------


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
        Literal["sql", "nl", "catalog", "schemas", "tables", "describe", "health", "admin"],
        Field(
            default="sql",
            description="'sql' executes read-only SQL against the lake. 'nl' translates a natural language question into SQL and executes it (e.g., 'how many violations in Brooklyn?'). 'catalog' searches table names and descriptions by keyword. 'schemas' lists all schemas. 'tables' lists tables in a schema. 'describe' shows columns and types for a table. 'health' shows lake freshness and row counts. 'admin' executes DDL (CREATE OR REPLACE VIEW only).",
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
    """Custom SQL queries, table exploration, and data export from the NYC data lake. 294 tables across 14 schemas.

    GUIDELINES: Show complete query results as interactive table. All rows, all columns.
    Present the FULL response to the user. Do not summarize SQL results.

    LIMITATIONS: Read-only. No INSERT/UPDATE/DELETE. Max rows enforced server-side.

    RETURNS: Query results as table, or schema/catalog metadata depending on mode."""
    pool = ctx.lifespan_context["pool"]
    catalog = ctx.lifespan_context.get("catalog", {})
    directive = "PRESENTATION: Show the complete query results as an interactive table. All rows, all columns. Do not summarize SQL results.\n\n"

    if mode == "sql":
        result = _sql(pool, input, format, ctx)
    elif mode == "nl":
        from tools.nl_query import nl_query
        emb_conn = ctx.lifespan_context.get("emb_conn")
        embed_fn = ctx.lifespan_context.get("embed_fn")
        api_key = ""
        try:
            import embedder
            api_key = embedder._GEMINI_KEYS[0] if embedder._GEMINI_KEYS else ""
        except Exception:
            pass
        if not embed_fn or not emb_conn:
            raise ToolError("Embeddings not available. NL query requires the embedding pipeline.")
        if not api_key:
            raise ToolError("No Gemini API key available for NL query generation.")
        result = nl_query(input, pool, emb_conn, embed_fn, api_key, format, ctx=ctx)
    elif mode == "admin":
        result = _admin(pool, input, ctx)
    elif mode == "catalog":
        embed_fn = ctx.lifespan_context.get("embed_fn")
        result = _catalog(pool, input, catalog, embed_fn, ctx=ctx)
    elif mode == "schemas":
        result = _schemas(catalog)
    elif mode == "tables":
        result = _tables(pool, input, catalog)
    elif mode == "describe":
        result = _describe(pool, input, catalog)
    elif mode == "health":
        result = _health(pool, input if input.strip() else None)
    else:
        raise ToolError(f"Unknown mode '{mode}'. Use: sql, nl, catalog, schemas, tables, describe, health, admin.")

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
