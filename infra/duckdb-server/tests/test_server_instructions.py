"""Sanity check: the MCP server's INSTRUCTIONS include the tool discipline rule.

Reads mcp_server.py as text instead of importing it — the module pulls in
posthog and the full server stack on import, which we don't want in this test.
"""
from pathlib import Path

_SERVER_PY = Path(__file__).parent.parent / "mcp_server.py"


def _instructions_text() -> str:
    src = _SERVER_PY.read_text()
    start = src.index('INSTRUCTIONS = """')
    end = src.index('"""', start + len('INSTRUCTIONS = """'))
    return src[start:end]


def test_instructions_include_tool_discipline():
    text = _instructions_text()
    assert "TOOL DISCIPLINE" in text
    assert "do not invent tool names" in text.lower()


def test_instructions_route_to_new_catalog_tools():
    text = _instructions_text()
    assert "list_schemas()" in text
    assert "describe_table(" in text
    assert "query_sql(" in text
