"""
OutputFormatterMiddleware — rewrites ToolResult content for LLM readability.

Intercepts all tool calls, detects tabular structured_content, and replaces
the raw text content with a format chosen by detect_format():
  - kv        : **field:** value pairs (1–3 rows)
  - markdown  : pipe-separated markdown table (4–50 rows or narrow)
  - toon      : TOON token-efficient format (51+ rows, 4+ cols)

Skips tools that return navigational/schema data (list_schemas, etc.).
Always returns the original result if formatting fails — never breaks a call.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from formatters import (
    detect_format,
    filter_columns,
    format_markdown_kv,
    format_markdown_table,
    format_toon,
)

from constants import MIDDLEWARE_SKIP_TOOLS

_SKIP_TOOLS = MIDDLEWARE_SKIP_TOOLS


class OutputFormatterMiddleware(Middleware):
    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP_TOOLS:
            return result

        try:
            return _reformat(result)
        except Exception:
            return result


def _reformat(result: ToolResult) -> ToolResult:
    sc = result.structured_content
    if not isinstance(sc, dict):
        return result

    rows_data = sc.get("rows")
    if not rows_data:
        return result

    cols = list(rows_data[0].keys())
    rows = [list(r.values()) for r in rows_data]

    total_rows = result.meta.get("total_rows", len(rows)) if result.meta else len(rows)

    if len(cols) > 12:
        cols, rows = filter_columns(cols, rows)

    fmt = detect_format(total_rows, len(cols))

    # Extract summary: everything before the first double-newline
    original_text = ""
    if isinstance(result.content, list) and result.content:
        original_text = result.content[0].text
    elif isinstance(result.content, str):
        original_text = result.content

    summary = original_text.split("\n\n")[0] if "\n\n" in original_text else ""

    if fmt == "kv":
        body = format_markdown_kv(cols, rows) or format_markdown_table(cols, rows, total_count=total_rows)
    elif fmt == "toon":
        body = format_toon(cols, rows, total_count=total_rows)
    else:
        body = format_markdown_table(cols, rows, total_count=total_rows)

    new_content = f"{summary}\n\n{body}" if summary else body

    return ToolResult(
        content=new_content,
        structured_content=result.structured_content,
        meta=result.meta,
    )
