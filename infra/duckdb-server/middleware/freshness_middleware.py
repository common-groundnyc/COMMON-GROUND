"""
FreshnessMiddleware — warns when queried data is stale.

Checks _pipeline_state.last_run_at for the tables a tool queries.
If any table hasn't been refreshed in > 7 days, appends a warning.
"""

import time
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from constants import TOOL_SOURCES

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
