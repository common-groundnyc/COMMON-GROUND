"""
CitationMiddleware — appends data source citations to every tool response.

Detects which lake tables were queried by scanning the tool's SQL constants
(via a pre-built mapping of tool_name → table_names), then appends a
"Sources:" footer with table names and row counts from the cached catalog.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult
from constants import TOOL_SOURCES

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
