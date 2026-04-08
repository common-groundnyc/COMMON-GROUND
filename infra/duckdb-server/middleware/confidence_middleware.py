"""
ConfidenceMiddleware — attaches a machine-readable confidence score to every
tool response based on signals written by upstream middleware.

Signals (each optional, weight redistributed when missing):
  - freshness_hours   (from FreshnessMiddleware)
  - completeness      (rows_returned / rows_expected, if both in meta)
  - match_probability (Splink m_probability, if the tool set one)
  - source_quality    (static per-source weight, future extension)

The final score is clamped to [0.0, 1.0] and written to meta['confidence'].
A parallel meta['confidence_reasons'] lists the short, human-readable
explanations agents actually need to decide follow-ups.
"""
from __future__ import annotations

from dataclasses import dataclass

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from constants import CONFIDENCE_SKIP_TOOLS


@dataclass
class _Signal:
    weight: float
    score: float  # 0.0 - 1.0
    reason: str


def _freshness_signal(meta: dict) -> _Signal | None:
    hours = meta.get("freshness_hours")
    if not isinstance(hours, (int, float)):
        return None
    if hours <= 24:
        return _Signal(0.40, 1.0, f"fresh (updated {int(hours)}h ago)")
    if hours <= 24 * 7:
        return _Signal(0.40, 0.85, f"recent (updated {int(hours / 24)}d ago)")
    if hours <= 24 * 30:
        days = int(hours / 24)
        return _Signal(0.40, 0.6, f"aging ({days}d old)")
    days = int(hours / 24)
    return _Signal(0.40, 0.2, f"stale ({days}d old)")


def _completeness_signal(meta: dict) -> _Signal | None:
    returned = meta.get("rows_returned")
    expected = meta.get("rows_expected")
    if not (isinstance(returned, int) and isinstance(expected, int) and expected > 0):
        return None
    ratio = min(returned / expected, 1.0)
    return _Signal(0.25, ratio, f"result set {returned}/{expected}")


def _match_signal(meta: dict) -> _Signal | None:
    p = meta.get("match_probability")
    if not isinstance(p, (int, float)):
        return None
    reason = "strong entity match" if p >= 0.9 else f"match p={p:.2f}"
    return _Signal(0.20, float(p), reason)


def _source_quality_signal(meta: dict) -> _Signal | None:
    q = meta.get("source_quality")
    if not isinstance(q, (int, float)):
        return None
    return _Signal(0.15, float(q), f"source quality {q:.2f}")


_EXTRACTORS = (
    _freshness_signal,
    _completeness_signal,
    _match_signal,
    _source_quality_signal,
)


def _compute(meta: dict) -> tuple[float, list[str]]:
    signals = [s for ex in _EXTRACTORS if (s := ex(meta)) is not None]
    if not signals:
        return 1.0, ["no signals available"]
    total_weight = sum(s.weight for s in signals)
    weighted = sum(s.weight * s.score for s in signals) / total_weight
    return round(max(0.0, min(1.0, weighted)), 3), [s.reason for s in signals]


class ConfidenceMiddleware(Middleware):
    async def on_call_tool(self, context: MiddlewareContext, call_next) -> ToolResult:
        result = await call_next(context)
        tool_name = getattr(context.message, "name", "")
        if tool_name in CONFIDENCE_SKIP_TOOLS:
            return result
        try:
            meta = dict(result.meta or {})
            score, reasons = _compute(meta)
            meta["confidence"] = score
            meta["confidence_reasons"] = reasons
            return ToolResult(
                content=result.content,
                structured_content=result.structured_content,
                meta=meta,
            )
        except Exception:
            # Never break a tool call over metadata
            return result
