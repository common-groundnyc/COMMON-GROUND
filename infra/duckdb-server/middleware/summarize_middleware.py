"""
SummarizeMiddleware — replaces ResponseLimitingMiddleware's dumb truncation
with an intelligent summary when tool responses exceed the size limit.

Instead of cutting text mid-sentence, this middleware:
  1. Counts rows/items per section in structured_content
  2. Builds a compact summary table showing what was found
  3. Appends a link to the explore page on common-ground.nyc
  4. Preserves meta (confidence, freshness, budget) through summarization

Registered FIRST in the middleware chain (runs LAST on response path)
so all other middleware have already written their signals before this
decides whether to summarize.
"""
from __future__ import annotations

import json
from typing import Any

import pydantic_core
from mcp.types import TextContent

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

# 50KB default — matches the old ResponseLimitingMiddleware threshold
MAX_RESPONSE_BYTES = 50_000

EXPLORE_BASE = "https://common-ground.nyc/explore"

# Tools whose large responses should never be summarized (they handle their own sizing)
_SKIP = frozenset({"export_data", "sql_query", "query"})


def _count_items(value: Any) -> int:
    """Count rows/items in a structured_content value."""
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        # Nested dict with a "rows" key
        rows = value.get("rows")
        if isinstance(rows, list):
            return len(rows)
        # Count non-meta keys as items
        return sum(1 for k in value if not k.startswith("_"))
    return 0


def _build_explore_link(sc: dict, tool_name: str) -> str:
    """Build a deep link to the explore page based on context clues."""
    # Try to find a zip/location for the explore link
    for key in ("zipcode", "zip", "zip_code", "location"):
        val = sc.get(key)
        if val and isinstance(val, str) and val.strip():
            return f"{EXPLORE_BASE}?zip={val.strip()}"

    # BBL → no zip-based explore link, but the explore page works without params
    bbl = sc.get("bbl")
    if bbl:
        return EXPLORE_BASE

    # Name → explore page
    name = sc.get("name") or sc.get("query")
    if name:
        return EXPLORE_BASE

    return EXPLORE_BASE


def _summarize(result: ToolResult, tool_name: str) -> ToolResult:
    """Build a compact summary of an oversized response."""
    sc = result.structured_content
    if not isinstance(sc, dict):
        # No structured content — fall back to text truncation
        return _truncate_text(result)

    # Count items per section
    sections: list[tuple[str, int]] = []
    total_items = 0
    for key, val in sc.items():
        if key.startswith("_") or key in ("name", "query", "bbl", "zipcode", "zip"):
            continue
        count = _count_items(val)
        if count > 0:
            label = key.replace("_", " ").title()
            sections.append((label, count))
            total_items += count

    if not sections:
        return _truncate_text(result)

    # Build the summary
    lines = [f"Found {total_items} total records across {len(sections)} categories:\n"]
    for label, count in sorted(sections, key=lambda x: -x[1]):
        lines.append(f"  {label}: {count} records")

    explore_url = _build_explore_link(sc, tool_name)
    lines.append(f"\nThis is a summary — the full dataset is too large to display here.")
    lines.append(f"View and explore the complete data at: {explore_url}")
    lines.append(f"\nTo narrow results, try a more specific query or use max_tokens to control response size.")

    # Keep a reduced structured_content with just counts (for agent routing)
    summary_sc = {
        "summary": True,
        "total_items": total_items,
        "sections": {label: count for label, count in sections},
    }

    # Copy any scalar metadata fields (bbl, name, zipcode, etc.)
    for key in ("name", "query", "bbl", "zipcode", "zip"):
        if key in sc:
            summary_sc[key] = sc[key]

    return ToolResult(
        content="\n".join(lines),
        structured_content=summary_sc,
        meta=result.meta,
    )


def _truncate_text(result: ToolResult) -> ToolResult:
    """Fallback: truncate text content with explore link."""
    texts = []
    if isinstance(result.content, list):
        texts = [b.text for b in result.content if isinstance(b, TextContent)]
    elif isinstance(result.content, str):
        texts = [result.content]

    text = "\n\n".join(texts) if texts else ""

    # Truncate to ~40KB to leave room for the suffix
    if len(text.encode("utf-8")) > 40_000:
        text = text.encode("utf-8")[:40_000].decode("utf-8", errors="ignore")

    text += f"\n\n[Response summarized — view full data at {EXPLORE_BASE}]"

    return ToolResult(
        content=text,
        structured_content=result.structured_content,
        meta=result.meta,
    )


class SummarizeMiddleware(Middleware):
    def __init__(self, max_size: int = MAX_RESPONSE_BYTES):
        self.max_size = max_size

    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)

        tool_name = context.message.name
        if tool_name in _SKIP:
            return result

        try:
            serialized = pydantic_core.to_json(result, fallback=str)
            if len(serialized) <= self.max_size:
                return result

            return _summarize(result, tool_name)
        except Exception:
            return result
