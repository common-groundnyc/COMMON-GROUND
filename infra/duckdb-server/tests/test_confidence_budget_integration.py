import pytest
from unittest.mock import AsyncMock, MagicMock

from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent

from middleware.percentile_middleware import PercentileMiddleware


@pytest.mark.asyncio
async def test_percentile_middleware_honors_max_tokens():
    rows = [{"id": i, "text": "x" * 70} for i in range(200)]
    result = ToolResult(
        content=[TextContent(type="text", text=f"{len(rows)} rows")],
        structured_content={"rows": rows, "total": len(rows)},
        meta={"freshness_hours": 1},
    )

    ctx = MagicMock()
    ctx.message.name = "worst_landlords"
    ctx.message.arguments = {"max_tokens": 500}

    call_next = AsyncMock(return_value=result)
    mw = PercentileMiddleware()

    out = await mw.on_call_tool(ctx, call_next)

    kept = out.structured_content["rows"]
    assert 0 < len(kept) < 200
    assert out.meta["budget_truncated"] is True
    assert out.meta["budget_max_tokens"] == 500
    assert out.meta["budget_rows_kept"] == len(kept)


@pytest.mark.asyncio
async def test_percentile_middleware_noop_without_budget():
    rows = [{"id": i} for i in range(10)]
    result = ToolResult(
        content=[TextContent(type="text", text="10 rows")],
        structured_content={"rows": rows, "total": 10},
        meta={},
    )

    ctx = MagicMock()
    ctx.message.name = "worst_landlords"
    ctx.message.arguments = {}

    call_next = AsyncMock(return_value=result)
    mw = PercentileMiddleware()

    out = await mw.on_call_tool(ctx, call_next)

    assert len(out.structured_content["rows"]) == 10
    assert "budget_truncated" not in (out.meta or {})
