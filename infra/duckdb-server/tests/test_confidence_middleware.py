import pytest
from unittest.mock import AsyncMock, MagicMock

from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent

from middleware.confidence_middleware import ConfidenceMiddleware


def _make_result(meta: dict) -> ToolResult:
    return ToolResult(
        content=[TextContent(type="text", text="ok")],
        structured_content={"rows": [{"id": 1}]},
        meta=meta,
    )


def _make_ctx(tool_name: str):
    ctx = MagicMock()
    ctx.message.name = tool_name
    ctx.message.arguments = {}
    return ctx


@pytest.mark.asyncio
async def test_fresh_data_produces_high_confidence():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("building_profile")
    call_next = AsyncMock(
        return_value=_make_result({"freshness_hours": 2})
    )

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] >= 0.9
    assert any("fresh" in r.lower() for r in out.meta["confidence_reasons"])


@pytest.mark.asyncio
async def test_stale_data_produces_low_confidence():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("building_profile")
    call_next = AsyncMock(
        return_value=_make_result({"freshness_hours": 24 * 60})  # 60 days
    )

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] <= 0.6
    assert any("stale" in r.lower() for r in out.meta["confidence_reasons"])


@pytest.mark.asyncio
async def test_skip_tools_receive_no_confidence():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("list_tables")
    call_next = AsyncMock(return_value=_make_result({"freshness_hours": 1}))

    out = await mw.on_call_tool(ctx, call_next)

    assert "confidence" not in (out.meta or {})


@pytest.mark.asyncio
async def test_completeness_lowers_confidence_for_partial_results():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("entity_xray")
    call_next = AsyncMock(
        return_value=_make_result({
            "freshness_hours": 2,
            "rows_returned": 5,
            "rows_expected": 100,
        })
    )

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] < 0.9
    assert any("5/100" in r for r in out.meta["confidence_reasons"])


@pytest.mark.asyncio
async def test_strong_splink_match_contributes_high_confidence():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("person_crossref")
    call_next = AsyncMock(
        return_value=_make_result({
            "freshness_hours": 2,
            "match_probability": 0.97,
        })
    )

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] >= 0.9
    assert any("strong entity match" in r for r in out.meta["confidence_reasons"])


@pytest.mark.asyncio
async def test_weak_splink_match_lowers_confidence():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("person_crossref")
    call_next = AsyncMock(
        return_value=_make_result({
            "freshness_hours": 2,
            "match_probability": 0.55,
        })
    )

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] < 0.9
    assert any("p=0.55" in r for r in out.meta["confidence_reasons"])


@pytest.mark.asyncio
async def test_confidence_with_no_signals_defaults_to_one():
    mw = ConfidenceMiddleware()
    ctx = _make_ctx("building_profile")
    call_next = AsyncMock(return_value=_make_result({}))

    out = await mw.on_call_tool(ctx, call_next)

    assert out.meta["confidence"] == 1.0
    assert out.meta["confidence_reasons"] == ["no signals available"]
