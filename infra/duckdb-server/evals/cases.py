"""Tool-use eval cases for the Common Ground MCP server.

Each case is a dataclass. Grader matches on:
  - expected_tool (exact tool name)
  - expected_required_params (subset match; extra optional params allowed)

25-case dataset lives in Task B3. This file starts with one smoke case
to exercise the runner end-to-end.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ToolEvalCase:
    """One eval task with a deterministic verifiable outcome."""
    id: str
    prompt: str
    expected_tool: str
    expected_required_params: dict = field(default_factory=dict)
    # Optional: soft constraints for v2 (tags, categories). Not graded in v1.
    tags: tuple = field(default_factory=tuple)


SMOKE_CASE = ToolEvalCase(
    id="smoke_building_address",
    prompt="Tell me about the building at 305 Linden Blvd, Brooklyn.",
    expected_tool="building",
    expected_required_params={"identifier": "305 Linden Blvd, Brooklyn"},
    tags=("building", "address", "smoke"),
)

CASES: list[ToolEvalCase] = [SMOKE_CASE]
