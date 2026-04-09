"""Tests for SummarizeMiddleware — intelligent response summarization."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent

from middleware.summarize_middleware import SummarizeMiddleware, _build_explore_link


def _make_result(sc: dict, meta: dict | None = None) -> ToolResult:
    return ToolResult(
        content=[TextContent(type="text", text="big response")],
        structured_content=sc,
        meta=meta or {"confidence": 0.95, "freshness_hours": 1.2},
    )


def _make_ctx(tool_name: str = "entity"):
    ctx = MagicMock()
    ctx.message.name = tool_name
    ctx.message.arguments = {}
    return ctx


@pytest.mark.asyncio
async def test_small_response_passes_through():
    """Responses under the limit are returned unchanged."""
    sc = {"name": "test", "rows": [{"id": 1}]}
    result = _make_result(sc)
    mw = SummarizeMiddleware(max_size=100_000)
    ctx = _make_ctx()
    out = await mw.on_call_tool(ctx, AsyncMock(return_value=result))
    assert out is result


@pytest.mark.asyncio
async def test_large_response_gets_summarized():
    """Responses over the limit get a summary with section counts."""
    sc = {
        "name": "BLACKSTONE",
        "nys_corps": [{"id": i} for i in range(50)],
        "campaign_contributions": [{"id": i} for i in range(200)],
        "acris_transactions": [{"id": i} for i in range(100)],
        "city_payroll": [{"id": i} for i in range(30)],
    }
    result = _make_result(sc)
    # Set a very small limit so the summary triggers
    mw = SummarizeMiddleware(max_size=500)
    ctx = _make_ctx("entity")
    out = await mw.on_call_tool(ctx, AsyncMock(return_value=result))

    # Should be summarized
    text = out.content if isinstance(out.content, str) else out.content[0].text
    assert "380 total records" in text
    assert "Campaign Contributions: 200" in text
    assert "Acris Transactions: 100" in text
    assert "common-ground.nyc/explore" in text
    assert "max_tokens" in text


@pytest.mark.asyncio
async def test_summary_preserves_meta():
    """Meta (confidence, freshness, budget) must survive summarization."""
    sc = {"rows": [{"id": i, "data": "x" * 200} for i in range(500)]}
    meta = {"confidence": 0.85, "confidence_reasons": ["fresh"], "freshness_hours": 2.1}
    result = _make_result(sc, meta)
    mw = SummarizeMiddleware(max_size=500)
    ctx = _make_ctx()
    out = await mw.on_call_tool(ctx, AsyncMock(return_value=result))

    assert out.meta["confidence"] == 0.85
    assert out.meta["freshness_hours"] == 2.1


@pytest.mark.asyncio
async def test_summary_structured_content_has_counts():
    """Summarized structured_content has section counts for agent routing."""
    sc = {
        "name": "test",
        "corps": [{"id": i} for i in range(10)],
        "violations": [{"id": i} for i in range(25)],
    }
    result = _make_result(sc)
    mw = SummarizeMiddleware(max_size=100)
    ctx = _make_ctx()
    out = await mw.on_call_tool(ctx, AsyncMock(return_value=result))

    assert out.structured_content["summary"] is True
    assert out.structured_content["total_items"] == 35
    assert out.structured_content["sections"]["Corps"] == 10
    assert out.structured_content["sections"]["Violations"] == 25
    assert out.structured_content["name"] == "test"


@pytest.mark.asyncio
async def test_skip_tools_never_summarized():
    """export_data and query pass through regardless of size."""
    sc = {"rows": [{"id": i, "data": "x" * 200} for i in range(500)]}
    result = _make_result(sc)
    mw = SummarizeMiddleware(max_size=100)
    ctx = _make_ctx("query")
    out = await mw.on_call_tool(ctx, AsyncMock(return_value=result))
    assert out is result


def test_explore_link_with_zip():
    assert _build_explore_link({"zipcode": "10025"}, "neighborhood") == "https://common-ground.nyc/explore?zip=10025"


def test_explore_link_with_bbl():
    assert _build_explore_link({"bbl": "1000670001"}, "building") == "https://common-ground.nyc/explore"


def test_explore_link_fallback():
    assert _build_explore_link({}, "entity") == "https://common-ground.nyc/explore"
