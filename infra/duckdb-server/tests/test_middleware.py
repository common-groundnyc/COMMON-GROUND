"""Tests for response_middleware.py — written first (TDD), RED before GREEN."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

from fastmcp.tools.tool import ToolResult

# Import the module under test (will fail until implemented)
from response_middleware import OutputFormatterMiddleware


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_result(content: str, rows: list | None = None, total_rows: int | None = None) -> ToolResult:
    structured = {"rows": rows} if rows is not None else {}
    meta = {}
    if total_rows is not None:
        meta["total_rows"] = total_rows
    return ToolResult(content=content, structured_content=structured, meta=meta)


def make_context(tool_name: str) -> MagicMock:
    ctx = MagicMock()
    ctx.message = MagicMock()
    ctx.message.name = tool_name
    return ctx


def result_text(result: ToolResult) -> str:
    """Extract plain text from ToolResult content."""
    if isinstance(result.content, list) and result.content:
        return result.content[0].text
    return str(result.content)


# ---------------------------------------------------------------------------
# TestOutputFormatterMiddleware
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tabular_result_gets_reformatted():
    """Tabular structured_content (rows) triggers reformatting."""
    rows = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Carol", "age": 35, "city": "Chicago"},
        {"name": "Dave", "age": 28, "city": "Boston"},
    ]
    original_content = "Found 4 results.\n\nSome old JSON blob here."
    original_result = make_result(original_content, rows=rows, total_rows=4)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    text = result_text(result)
    # Should be reformatted — summary line preserved, old JSON blob gone
    assert "Found 4 results." in text
    # Should contain markdown table formatting (4 rows => markdown format)
    assert "|" in text or "**name:**" in text  # markdown table or kv


@pytest.mark.asyncio
async def test_no_structured_content_passes_through():
    """Results without structured_content rows are returned unchanged."""
    original_result = make_result("Plain text response with no rows.", rows=None)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    assert result is original_result


@pytest.mark.asyncio
async def test_empty_rows_passes_through():
    """Results with empty rows list are returned unchanged."""
    original_result = make_result("No results found.", rows=[], total_rows=0)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    assert result is original_result


@pytest.mark.asyncio
async def test_single_row_uses_kv_format():
    """Single-row result uses KV format with **field:** pattern."""
    rows = [{"name": "Alice", "age": 30, "city": "NYC"}]
    original_result = make_result("Found 1 result.\n\nold stuff", rows=rows, total_rows=1)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    text = result_text(result)
    # KV format uses **field:** pattern
    assert "**name:**" in text
    assert "**age:**" in text
    assert "**city:**" in text


@pytest.mark.asyncio
async def test_skipped_tools_pass_through_unchanged():
    """Tools in _SKIP_TOOLS are returned without reformatting."""
    rows = [{"col": "val"}]
    original_result = make_result("Schema list here.", rows=rows, total_rows=1)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()

    for tool_name in ["list_schemas", "search_tools", "data_catalog", "sql_admin"]:
        context = make_context(tool_name)
        result = await middleware.on_call_tool(context, call_next)
        assert result is original_result, f"Tool {tool_name!r} should pass through unchanged"


@pytest.mark.asyncio
async def test_tool_exceptions_propagate():
    """Exceptions raised by the tool itself are NOT caught by the middleware."""
    async def call_next(ctx):
        raise RuntimeError("DuckDB query failed")

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")

    with pytest.raises(RuntimeError, match="DuckDB query failed"):
        await middleware.on_call_tool(context, call_next)


@pytest.mark.asyncio
async def test_formatter_error_returns_original_result():
    """If formatting logic raises an exception, return the original result unchanged."""
    rows = [{"x": "val"}]
    original_result = make_result("Summary.\n\ndata", rows=rows, total_rows=1)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")

    # Monkeypatch detect_format to raise inside the middleware
    import response_middleware as rm
    original_detect = rm.detect_format

    def broken_detect(*args, **kwargs):
        raise ValueError("simulated formatter crash")

    rm.detect_format = broken_detect
    try:
        result = await middleware.on_call_tool(context, call_next)
        assert result is original_result, "Should return original result on formatter error"
    finally:
        rm.detect_format = original_detect


@pytest.mark.asyncio
async def test_structured_content_preserved_in_new_result():
    """The new ToolResult keeps original structured_content and meta unchanged."""
    rows = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
        {"a": 5, "b": 6},
        {"a": 7, "b": 8},
    ]
    original_result = make_result("Summary.\n\ndata", rows=rows, total_rows=4)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    # structured_content and meta must be passed through unchanged
    assert result.structured_content == {"rows": rows}
    assert result.meta == {"total_rows": 4}


@pytest.mark.asyncio
async def test_summary_line_preserved_in_output():
    """The summary line (text before first double-newline) appears in the reformatted output."""
    rows = [{"col1": "a", "col2": "b"}]
    summary = "Showing results for sql_query."
    original_result = make_result(f"{summary}\n\nold content", rows=rows, total_rows=1)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    text = result_text(result)
    assert summary in text


@pytest.mark.asyncio
async def test_wide_table_triggers_column_filtering():
    """Tables with >12 columns have filter_columns applied."""
    # 15 columns
    cols = [f"col{i}" for i in range(15)]
    rows = [{c: f"val{i}" for i, c in enumerate(cols)}]
    original_result = make_result("Summary.\n\ndata", rows=rows, total_rows=1)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    text = result_text(result)
    # Result should not contain all 15 column headers
    col_count_in_output = sum(1 for c in cols if c in text)
    assert col_count_in_output <= 12


@pytest.mark.asyncio
async def test_toon_format_for_large_result():
    """51+ rows with 4+ columns use TOON format."""
    cols = ["a", "b", "c", "d"]
    rows = [{c: f"v{i}" for c in cols} for i in range(60)]
    original_result = make_result("Big result.\n\ndata", rows=rows, total_rows=60)

    async def call_next(ctx):
        return original_result

    middleware = OutputFormatterMiddleware()
    context = make_context("sql_query")
    result = await middleware.on_call_tool(context, call_next)

    text = result_text(result)
    # TOON format header: results[N]{col1,col2,...}:
    assert "results[" in text and "{" in text
