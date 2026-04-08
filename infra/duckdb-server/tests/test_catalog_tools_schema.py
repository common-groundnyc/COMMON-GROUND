"""Assert that the query_sql tool emits the expected JSON schema with enum-constrained format."""
import asyncio
import importlib

import pytest
from fastmcp import FastMCP

# tools/__init__.py re-exports `query_sql` as a name, which shadows the module
# when doing `from tools.catalog_tools import query_sql` in some import orders.
# importlib bypasses that. Same pattern as tests/test_query_wrapper.py.
_ct = importlib.import_module("tools.catalog_tools")


@pytest.fixture(scope="module")
def query_sql_params():
    async def _probe():
        mcp = FastMCP("eval-test")
        mcp.tool()(_ct.query_sql)
        tool = await mcp.get_tool("query_sql")
        return tool.parameters

    return asyncio.run(_probe())


def test_query_sql_format_is_enum_in_schema(query_sql_params):
    """query_sql.format must be typed Literal so FastMCP emits a JSON schema enum."""
    format_schema = query_sql_params["properties"]["format"]
    assert format_schema.get("enum") == ["text", "xlsx", "csv"], (
        f"Expected enum=['text','xlsx','csv'], got {format_schema}"
    )
    assert format_schema.get("type") == "string"
    assert format_schema.get("default") == "text"


def test_query_sql_sql_is_free_text_in_schema(query_sql_params):
    """query_sql.sql is intentionally free-text — no enum expected."""
    sql_schema = query_sql_params["properties"]["sql"]
    assert sql_schema.get("type") == "string"
    assert "enum" not in sql_schema
