"""Unit tests for FreshnessMiddleware writing freshness_hours into meta."""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta, timezone

from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent

from middleware.freshness_middleware import FreshnessMiddleware


def _iso_hours_ago(h: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat().replace("+00:00", "Z")


def _make_ctx(tool_name: str, pipeline_state: dict):
    ctx = MagicMock()
    ctx.message.name = tool_name
    ctx.message.arguments = {}
    ctx.fastmcp_context.lifespan_context = {"pipeline_state": pipeline_state}
    return ctx


def _make_result(meta=None) -> ToolResult:
    return ToolResult(
        content=[TextContent(type="text", text="ok")],
        structured_content={"rows": [{"id": 1}]},
        meta=meta or {},
    )


@pytest.mark.asyncio
async def test_fresh_source_writes_small_hours(monkeypatch):
    # Patch TOOL_SOURCES so our fake tool has a single source
    from middleware import freshness_middleware as fm
    monkeypatch.setattr(fm, "TOOL_SOURCES", {"test_tool": ["housing.hpd_violations"]})

    pipeline_state = {
        "housing.hpd_violations": {"last_run_at": _iso_hours_ago(2)}
    }

    mw = FreshnessMiddleware()
    ctx = _make_ctx("test_tool", pipeline_state)
    call_next = AsyncMock(return_value=_make_result())

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta is not None
    assert "freshness_hours" in out.meta
    assert 1.5 <= out.meta["freshness_hours"] <= 2.5


@pytest.mark.asyncio
async def test_max_age_wins_across_multiple_sources(monkeypatch):
    from middleware import freshness_middleware as fm
    monkeypatch.setattr(fm, "TOOL_SOURCES", {"test_tool": ["t1", "t2", "t3"]})

    pipeline_state = {
        "t1": {"last_run_at": _iso_hours_ago(1)},
        "t2": {"last_run_at": _iso_hours_ago(48)},  # 2 days
        "t3": {"last_run_at": _iso_hours_ago(12)},
    }

    mw = FreshnessMiddleware()
    ctx = _make_ctx("test_tool", pipeline_state)
    call_next = AsyncMock(return_value=_make_result())

    out = await mw.on_call_tool(ctx, call_next)
    assert 47 <= out.meta["freshness_hours"] <= 49


@pytest.mark.asyncio
async def test_stale_source_adds_warning_and_meta(monkeypatch):
    from middleware import freshness_middleware as fm
    monkeypatch.setattr(fm, "TOOL_SOURCES", {"test_tool": ["t1"]})

    pipeline_state = {
        "t1": {"last_run_at": _iso_hours_ago(24 * 30)}  # 30 days
    }

    mw = FreshnessMiddleware()
    ctx = _make_ctx("test_tool", pipeline_state)
    call_next = AsyncMock(return_value=_make_result())

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["freshness_hours"] >= 24 * 29
    # Warning text appended
    txt = out.content if isinstance(out.content, str) else out.content[0].text
    assert "freshness warning" in txt.lower()


@pytest.mark.asyncio
async def test_no_source_state_leaves_meta_untouched(monkeypatch):
    from middleware import freshness_middleware as fm
    monkeypatch.setattr(fm, "TOOL_SOURCES", {"test_tool": ["t_unknown"]})

    mw = FreshnessMiddleware()
    ctx = _make_ctx("test_tool", {})  # empty pipeline_state
    call_next = AsyncMock(return_value=_make_result(meta={"existing": "value"}))

    out = await mw.on_call_tool(ctx, call_next)
    assert out.meta == {"existing": "value"}
    assert "freshness_hours" not in out.meta


@pytest.mark.asyncio
async def test_skip_tool_bypasses_freshness():
    mw = FreshnessMiddleware()
    ctx = _make_ctx("list_schemas", {"anything": {"last_run_at": _iso_hours_ago(100)}})
    call_next = AsyncMock(return_value=_make_result())

    out = await mw.on_call_tool(ctx, call_next)
    assert "freshness_hours" not in (out.meta or {})
