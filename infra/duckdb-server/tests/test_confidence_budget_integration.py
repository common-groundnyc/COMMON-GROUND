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
async def test_percentile_middleware_strips_max_tokens_from_arguments():
    """Regression: tools with additionalProperties=False reject unknown args.
    PercentileMiddleware must remove max_tokens before call_next so the tool
    validator never sees it."""
    rows = [{"id": i} for i in range(5)]
    result = ToolResult(
        content=[TextContent(type="text", text="5 rows")],
        structured_content={"rows": rows, "total": 5},
        meta={},
    )

    ctx = MagicMock()
    ctx.message.name = "worst_landlords"
    ctx.message.arguments = {"owner_name": "acme", "max_tokens": 800}

    # Capture what arguments the downstream call sees
    seen = {}
    async def capturing_next(c):
        seen.update(c.message.arguments)
        return result

    mw = PercentileMiddleware()
    await mw.on_call_tool(ctx, capturing_next)

    assert "max_tokens" not in seen, "middleware must strip max_tokens before call_next"
    assert seen == {"owner_name": "acme"}


@pytest.mark.asyncio
async def test_percentile_middleware_strips_nested_max_tokens():
    rows = [{"id": i} for i in range(5)]
    result = ToolResult(
        content=[TextContent(type="text", text="5 rows")],
        structured_content={"rows": rows, "total": 5},
        meta={},
    )

    ctx = MagicMock()
    ctx.message.name = "worst_landlords"
    ctx.message.arguments = {"owner": "acme", "options": {"max_tokens": 600, "verbose": True}}

    seen = {}
    async def capturing_next(c):
        seen["args"] = dict(c.message.arguments)
        seen["options"] = dict(c.message.arguments.get("options") or {})
        return result

    mw = PercentileMiddleware()
    await mw.on_call_tool(ctx, capturing_next)

    assert "max_tokens" not in seen["options"]
    assert seen["options"] == {"verbose": True}


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

from fastmcp import FastMCP
from fastmcp.client import Client
from middleware.confidence_middleware import ConfidenceMiddleware


@pytest.mark.asyncio
async def test_full_chain_writes_confidence_and_respects_budget():
    """End-to-end: build a minimal FastMCP server with ConfidenceMiddleware +
    PercentileMiddleware, call a tool with max_tokens, assert both features
    show up in the response envelope.
    """
    app = FastMCP("test")

    @app.tool()
    def fake_worst_landlords() -> ToolResult:
        # Intentionally does NOT declare max_tokens — the middleware must strip
        # it from the call arguments before the tool validator sees it.
        rows = [{"rank": i, "owner": f"LLC_{i}", "violations": 1000 - i}
                for i in range(150)]
        return ToolResult(
            content=[TextContent(type="text", text=f"{len(rows)} landlords")],
            structured_content={"rows": rows, "total": len(rows)},
            meta={"freshness_hours": 3, "rows_returned": 150, "rows_expected": 150},
        )

    app.add_middleware(ConfidenceMiddleware())
    app.add_middleware(PercentileMiddleware())

    async with Client(app) as client:
        result = await client.call_tool("fake_worst_landlords", {"max_tokens": 400})

    meta = None
    for attr in ("meta", "_meta"):
        if hasattr(result, attr) and getattr(result, attr):
            meta = getattr(result, attr)
            break
    if meta is None and hasattr(result, "structured_content"):
        sc = result.structured_content
        if isinstance(sc, dict) and isinstance(sc.get("_meta"), dict):
            meta = sc["_meta"]
    assert meta is not None, f"Could not find meta on result: {dir(result)}"

    assert "confidence" in meta, f"meta missing confidence: {meta}"
    assert meta["confidence"] >= 0.9
    assert isinstance(meta["confidence_reasons"], list)
    assert meta.get("budget_truncated") is True
    assert meta["budget_rows_kept"] < 150
