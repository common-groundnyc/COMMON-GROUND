"""Assert that the query_sql tool emits the expected JSON schema with enum-constrained format."""
import asyncio
import importlib

from fastmcp import FastMCP

# Bypass tools/__init__.py shadow — see tests/test_query_wrapper.py for the pattern.
_ct = importlib.import_module("tools.catalog_tools")


def _make_mcp_with_query_sql():
    mcp = FastMCP("eval-test")
    mcp.tool()(_ct.query_sql)
    return mcp


def _get_tool_params():
    async def _probe():
        mcp = _make_mcp_with_query_sql()
        tool = await mcp.get_tool("query_sql")
        return tool.parameters

    return asyncio.run(_probe())


def test_query_sql_format_is_enum_in_schema():
    """query_sql.format must be typed Literal so FastMCP emits a JSON schema enum."""
    params = _get_tool_params()

    # Pydantic v2 dereferences to inline types in FastMCP 3.x
    format_schema = params["properties"]["format"]
    assert format_schema.get("enum") == ["text", "xlsx", "csv"], (
        f"Expected enum=['text','xlsx','csv'], got {format_schema}"
    )
    assert format_schema.get("type") == "string"


def test_query_sql_sql_is_free_text_in_schema():
    """query_sql.sql is intentionally free-text — no enum expected."""
    params = _get_tool_params()
    sql_schema = params["properties"]["sql"]
    assert sql_schema.get("type") == "string"
    assert "enum" not in sql_schema
