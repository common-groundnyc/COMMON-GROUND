"""Discrete catalog tools split out of the legacy query() super-tool.

Each tool is thin — it delegates to private helpers already defined in
tools/query.py. The motivation is agent ergonomics: LLMs consistently guess
tool names like `list_tables`, `describe_table`, `list_schemas`, so we expose
them directly instead of hiding them behind `query(mode='...')`.
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

    ALWAYS call this before writing custom SQL against an unfamiliar table --
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
    """Data lake health -- row counts, null rates, freshness per table.

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
