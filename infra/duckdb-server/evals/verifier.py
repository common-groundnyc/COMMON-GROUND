"""Deterministic tool-call grader. No LLM-as-judge in v1."""
from dataclasses import dataclass

from evals.cases import ToolEvalCase


@dataclass(frozen=True)
class CaseGrade:
    case_id: str
    tool_selected: bool
    required_params_ok: bool
    passed: bool
    error: str | None
    trial: int


def grade_case(
    case: ToolEvalCase,
    tool_calls: list[dict],
    error: str | None = None,
    trial: int = 0,
) -> CaseGrade:
    """Grade one agent run against one case.

    tool_calls: list of {"name": str, "input": dict} as returned by Anthropic SDK.
    """
    if error is not None:
        return CaseGrade(
            case_id=case.id,
            tool_selected=False,
            required_params_ok=False,
            passed=False,
            error=error,
            trial=trial,
        )

    matching = [tc for tc in tool_calls if tc["name"] == case.expected_tool]
    tool_selected = len(matching) > 0

    if not tool_selected:
        return CaseGrade(
            case_id=case.id,
            tool_selected=False,
            required_params_ok=False,
            passed=False,
            error=None,
            trial=trial,
        )

    # Subset match: every required param must be present AND equal.
    # Extra params on the tool call are fine (optional args, defaults).
    first_match = matching[0]
    actual_params = first_match.get("input", {})
    required_ok = all(
        actual_params.get(k) == v for k, v in case.expected_required_params.items()
    )

    return CaseGrade(
        case_id=case.id,
        tool_selected=True,
        required_params_ok=required_ok,
        passed=required_ok,  # in v1, passing = tool correct AND params correct
        error=None,
        trial=trial,
    )
