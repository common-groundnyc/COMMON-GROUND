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

    Special case: if case.expected_tool == "__no_tool_call_expected__", the
    case passes iff the agent made NO tool calls matching a real tool (i.e.,
    the agent correctly refused). Any tool call fails the case.

    tool_calls: list of {"name": str, "input": dict} as returned by Anthropic SDK.
    """
    # max_turns_exceeded is NOT a tool-selection failure — the agent may have
    # called the right tool correctly but kept exploring. Grade tool selection
    # normally and report the error alongside. Other errors (network, auth,
    # server crash) still fail the case because there's no reliable signal.
    is_max_turns = error is not None and "max_turns_exceeded" in error
    if error is not None and not is_max_turns:
        return CaseGrade(
            case_id=case.id,
            tool_selected=False,
            required_params_ok=False,
            passed=False,
            error=error,
            trial=trial,
        )

    # Anti-hallucination case: pass iff NO tool calls were made.
    if case.expected_tool == "__no_tool_call_expected__":
        return CaseGrade(
            case_id=case.id,
            tool_selected=len(tool_calls) == 0,
            required_params_ok=True,
            passed=len(tool_calls) == 0,
            error=None,
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

    # Subset match on required params.
    first_match = matching[0]
    actual_params = first_match.get("input", {})
    required_ok = all(
        actual_params.get(k) == v for k, v in case.expected_required_params.items()
    )

    return CaseGrade(
        case_id=case.id,
        tool_selected=True,
        required_params_ok=required_ok,
        passed=required_ok,
        error=error if is_max_turns else None,
        trial=trial,
    )
