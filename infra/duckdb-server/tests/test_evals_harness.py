"""Unit tests for the eval harness grader + metrics. No live API calls."""
import importlib

_verifier = importlib.import_module("evals.verifier")
_metrics = importlib.import_module("evals.metrics")
_cases = importlib.import_module("evals.cases")


def _fake_tool_call(name, params):
    return {"name": name, "input": params}


def test_verifier_tool_selection_exact_match():
    case = _cases.ToolEvalCase(
        id="smoke_building",
        prompt="Tell me about 305 Linden Blvd, Brooklyn",
        expected_tool="building",
        expected_required_params={"identifier": "305 Linden Blvd, Brooklyn"},
    )
    grade = _verifier.grade_case(
        case,
        tool_calls=[_fake_tool_call("building", {"identifier": "305 Linden Blvd, Brooklyn"})],
    )
    assert grade.tool_selected is True
    assert grade.required_params_ok is True
    assert grade.passed is True


def test_verifier_wrong_tool_fails():
    case = _cases.ToolEvalCase(
        id="smoke_building",
        prompt="Tell me about 305 Linden Blvd, Brooklyn",
        expected_tool="building",
        expected_required_params={"identifier": "305 Linden Blvd, Brooklyn"},
    )
    grade = _verifier.grade_case(
        case,
        tool_calls=[_fake_tool_call("address_report", {"address": "305 Linden Blvd, Brooklyn"})],
    )
    assert grade.tool_selected is False
    assert grade.passed is False


def test_verifier_missing_required_param_fails():
    case = _cases.ToolEvalCase(
        id="network_owner",
        prompt="Find Steven Croman's ownership network",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "ownership"},
    )
    grade = _verifier.grade_case(
        case,
        tool_calls=[_fake_tool_call("network", {"name": "Steven Croman"})],  # missing type
    )
    assert grade.tool_selected is True
    assert grade.required_params_ok is False
    assert grade.passed is False


def test_verifier_accepts_extra_params():
    """Extra optional params on the tool call should NOT fail the grade."""
    case = _cases.ToolEvalCase(
        id="network_owner",
        prompt="Find Steven Croman's ownership network",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "ownership"},
    )
    grade = _verifier.grade_case(
        case,
        tool_calls=[_fake_tool_call("network", {
            "name": "Steven Croman",
            "type": "ownership",
            "depth": 3,  # extra, should be fine
        })],
    )
    assert grade.passed is True


def test_metrics_pass_any_k_all_passing():
    grades = [
        _verifier.CaseGrade(case_id="a", tool_selected=True, required_params_ok=True, passed=True, error=None, trial=t)
        for t in range(3)
    ]
    assert _metrics.pass_any_k(grades, k=1) == 1.0
    assert _metrics.pass_any_k(grades, k=3) == 1.0
    assert _metrics.pass_all_k(grades, k=3) == 1.0


def test_metrics_pass_any_k_one_of_three():
    grades = [
        _verifier.CaseGrade(case_id="a", tool_selected=False, required_params_ok=False, passed=False, error=None, trial=0),
        _verifier.CaseGrade(case_id="a", tool_selected=True, required_params_ok=True, passed=True, error=None, trial=1),
        _verifier.CaseGrade(case_id="a", tool_selected=False, required_params_ok=False, passed=False, error=None, trial=2),
    ]
    assert _metrics.pass_any_k(grades, k=3) == 1.0  # at least one trial passed
    assert _metrics.pass_all_k(grades, k=3) == 0.0  # not all 3 passed
