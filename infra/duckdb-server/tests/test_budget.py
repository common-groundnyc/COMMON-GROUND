from middleware.budget import extract_budget


def test_extract_budget_returns_none_when_missing():
    assert extract_budget({}) is None
    assert extract_budget({"bbl": "1000010001"}) is None


def test_extract_budget_reads_max_tokens():
    assert extract_budget({"max_tokens": 1500}) == 1500


def test_extract_budget_reads_nested_in_options():
    assert extract_budget({"options": {"max_tokens": 800}}) == 800


def test_extract_budget_clamps_below_minimum():
    # Below 100 is pointless — return minimum usable budget
    assert extract_budget({"max_tokens": 10}) == 100


def test_extract_budget_clamps_above_maximum():
    # Cap at 20k to protect the server
    assert extract_budget({"max_tokens": 999_999}) == 20_000


def test_extract_budget_ignores_non_integer():
    assert extract_budget({"max_tokens": "lots"}) is None
    assert extract_budget({"max_tokens": None}) is None


from fastmcp.tools.tool import ToolResult
from mcp.types import TextContent
from middleware.budget import truncate_to_budget


def _make_result(rows: list[dict]) -> ToolResult:
    return ToolResult(
        content=[TextContent(type="text", text=f"{len(rows)} rows")],
        structured_content={"rows": rows, "total": len(rows)},
        meta={"tool": "test_tool"},
    )


def test_truncate_noop_when_under_budget():
    r = _make_result([{"id": 1, "name": "alice"}])
    out = truncate_to_budget(r, max_tokens=500)
    assert out.structured_content["rows"] == [{"id": 1, "name": "alice"}]
    assert "budget_truncated" not in (out.meta or {})


def test_truncate_drops_lowest_rows_when_over_budget():
    rows = [{"id": i, "description": "x" * 40} for i in range(100)]
    r = _make_result(rows)
    out = truncate_to_budget(r, max_tokens=200)
    kept = out.structured_content["rows"]
    assert 0 < len(kept) < 100
    assert kept[0] == rows[0]
    assert out.meta["budget_truncated"] is True
    assert out.meta["budget_rows_kept"] == len(kept)
    assert out.meta["budget_rows_dropped"] == 100 - len(kept)
    assert out.meta["budget_max_tokens"] == 200


def test_truncate_noop_when_no_rows_field():
    r = ToolResult(
        content=[TextContent(type="text", text="ok")],
        structured_content={"summary": "all good"},
        meta={},
    )
    out = truncate_to_budget(r, max_tokens=50)
    assert out is r
