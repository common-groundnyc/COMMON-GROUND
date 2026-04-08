"""Tests for the 6 discrete catalog tools split out of query()."""
from unittest.mock import MagicMock

from fastmcp.tools.tool import ToolResult

from tools.catalog_tools import (
    catalog_search,
    describe_table,
    health_check,
    list_schemas,
    list_tables,
    query_sql,
)


def _text(result):
    """Extract text from ToolResult.content (str or list[TextContent])."""
    if isinstance(result.content, str):
        return result.content
    return "\n".join(getattr(c, "text", str(c)) for c in (result.content or []))


class _FakeCtx:
    def __init__(self, pool, catalog, embed_fn=None, emb_conn=None):
        self.lifespan_context = {
            "pool": pool,
            "catalog": catalog,
            "embed_fn": embed_fn,
            "emb_conn": emb_conn,
        }


def _fake_catalog():
    return {
        "housing": {
            "hpd_violations": {"row_count": 10_000_000, "column_count": 20, "description": "violations"},
            "hpd_complaints": {"row_count": 500_000, "column_count": 15, "description": "complaints"},
        },
        "public_safety": {
            "motor_vehicle_collisions": {"row_count": 2_000_000, "column_count": 30, "description": "crashes"},
        },
    }


def test_list_schemas_returns_schema_summary():
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = list_schemas(ctx=ctx)

    assert isinstance(result, ToolResult)
    text = _text(result)
    assert "housing: 2 tables" in text
    assert "public_safety: 1 tables" in text


def test_list_tables_returns_tables_in_schema(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._tables",
        lambda pool, schema_input, catalog: ToolResult(
            content="Schema 'housing': 2 tables", structured_content=None, meta={}
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = list_tables(schema="housing", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "housing" in _text(result)


def test_describe_table_returns_columns(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._describe",
        lambda pool, table_input, catalog: ToolResult(
            content="lake.housing.hpd_violations: 10,000,000 rows, 20 columns",
            structured_content=None,
            meta={},
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = describe_table(schema="housing", table="hpd_violations", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "hpd_violations" in _text(result)


def test_query_sql_executes_select(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._sql",
        lambda pool, sql, fmt, ctx: ToolResult(
            content="Query returned 5 rows (42ms).",
            structured_content=None,
            meta={"query_time_ms": 42},
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = query_sql(sql="SELECT * FROM lake.housing.hpd_violations LIMIT 5", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "5 rows" in _text(result)


def test_catalog_search_finds_tables(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._catalog",
        lambda pool, keyword, catalog, embed_fn, ctx: ToolResult(
            content="Found 2 tables matching 'eviction'", structured_content=None, meta={}
        ),
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = catalog_search(keyword="eviction", ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "eviction" in _text(result)


def test_health_check_returns_health(monkeypatch):
    monkeypatch.setattr(
        "tools.catalog_tools._health",
        lambda pool, schema_filter=None: "housing: 15 tables fresh",
    )
    ctx = _FakeCtx(pool=MagicMock(), catalog=_fake_catalog())
    result = health_check(ctx=ctx)

    assert isinstance(result, ToolResult)
    assert "fresh" in _text(result)
