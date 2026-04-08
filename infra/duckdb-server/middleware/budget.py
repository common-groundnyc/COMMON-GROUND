"""
Budget helpers for agent-native tool responses.

extract_budget() pulls an optional `max_tokens` value from tool call arguments
(either top-level or nested in an `options` dict) and clamps it into a safe range.

truncate_to_budget() trims an already-ranked result payload from the bottom
until its serialized size fits within the budget. Bottom = lowest-ranked rows,
so PercentileMiddleware must run before this helper.
"""
from __future__ import annotations

import json
from typing import Any

_MIN_BUDGET = 100
_MAX_BUDGET = 20_000
# Rough heuristic: 1 token ≈ 4 characters for mixed JSON/English content
_CHARS_PER_TOKEN = 4


def extract_budget(args: dict[str, Any] | None) -> int | None:
    """Return a clamped integer budget, or None if no budget was supplied."""
    if not args:
        return None
    raw = args.get("max_tokens")
    if raw is None and isinstance(args.get("options"), dict):
        raw = args["options"].get("max_tokens")
    if not isinstance(raw, int) or isinstance(raw, bool):
        return None
    return max(_MIN_BUDGET, min(_MAX_BUDGET, raw))


def _approx_tokens(obj: Any) -> int:
    return len(json.dumps(obj, default=str)) // _CHARS_PER_TOKEN


def truncate_to_budget(result: "ToolResult", max_tokens: int) -> "ToolResult":
    """
    Truncate result.structured_content['rows'] from the bottom until the
    serialized payload fits in `max_tokens`. No-op if the payload already fits
    or if the result has no 'rows' list.
    """
    from fastmcp.tools.tool import ToolResult

    sc = result.structured_content
    if not isinstance(sc, dict):
        return result
    rows = sc.get("rows")
    if not isinstance(rows, list) or not rows:
        return result

    if _approx_tokens(sc) <= max_tokens:
        return result

    # Binary-search the largest prefix that fits
    lo, hi = 0, len(rows)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        trial = {**sc, "rows": rows[:mid]}
        if _approx_tokens(trial) <= max_tokens:
            lo = mid
        else:
            hi = mid - 1

    truncated_rows = rows[:lo]
    new_sc = {**sc, "rows": truncated_rows}
    new_meta = dict(result.meta or {})
    new_meta["budget_truncated"] = True
    new_meta["budget_max_tokens"] = max_tokens
    new_meta["budget_rows_kept"] = len(truncated_rows)
    new_meta["budget_rows_dropped"] = len(rows) - len(truncated_rows)

    return ToolResult(
        content=result.content,
        structured_content=new_sc,
        meta=new_meta,
    )
