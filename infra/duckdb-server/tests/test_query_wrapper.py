"""Regression tests for query() wrapper bugs observed in PostHog.

Issue: four branches return str instead of ToolResult, crashing the wrapper.
PostHog error: "'str' object has no attribute 'content'"
"""
import importlib
from unittest.mock import MagicMock

from fastmcp.tools.tool import ToolResult

# Import the module directly, bypassing tools/__init__.py which shadows it.
_query_mod = importlib.import_module("tools.query")
query = _query_mod.query


class _FakeCtx:
    def __init__(self, pool, catalog):
        self.lifespan_context = {
            "pool": pool,
            "catalog": catalog,
            "emb_conn": None,
            "embed_fn": None,
        }


def _fake_catalog():
    return {
        "housing": {
            "hpd_violations": {"row_count": 10_000_000, "column_count": 20, "description": ""},
        },
    }


def _text(result: ToolResult) -> str:
    """Extract text from a ToolResult regardless of whether content is str or list of TextContent."""
    if isinstance(result.content, str):
        return result.content
    return " ".join(getattr(c, "text", str(c)) for c in result.content)


def test_query_mode_schemas_returns_toolresult():
    """mode='schemas' returns a str from _schemas; wrapper must not crash."""
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="", mode="schemas", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "housing" in _text(result)


def test_query_mode_health_returns_toolresult(monkeypatch):
    """mode='health' returns a str from _health; wrapper must not crash."""
    monkeypatch.setattr(_query_mod, "_health", lambda pool, schema_filter=None: "health: ok")
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="", mode="health", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "health: ok" in _text(result)


def test_query_mode_admin_returns_toolresult(monkeypatch):
    """mode='admin' returns a str from _admin; wrapper must not crash."""
    monkeypatch.setattr(_query_mod, "_admin", lambda pool, sql, ctx: "view created")
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="CREATE OR REPLACE VIEW v AS SELECT 1", mode="admin", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "view created" in _text(result)


def test_query_sql_xlsx_export_returns_toolresult(monkeypatch):
    """mode='sql' with format='xlsx' returns a str from _sql export path; wrapper must not crash."""
    monkeypatch.setattr(_query_mod, "_sql", lambda pool, sql, fmt, ctx: "Exported 100 rows to XLSX")
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query(input="SELECT 1", mode="sql", format="xlsx", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "Exported" in _text(result)
