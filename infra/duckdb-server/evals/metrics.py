"""Aggregate CaseGrade lists into metrics."""
from collections import defaultdict

from evals.cases import ToolEvalCase
from evals.verifier import CaseGrade


def pass_any_k(grades: list[CaseGrade], k: int) -> float:
    """Fraction of cases where at least one of the first k trials passed (lenient)."""
    by_case: dict[str, list[CaseGrade]] = defaultdict(list)
    for g in grades:
        by_case[g.case_id].append(g)
    if not by_case:
        return 0.0
    passing = 0
    for case_id, trials in by_case.items():
        trials_sorted = sorted(trials, key=lambda g: g.trial)[:k]
        if any(t.passed for t in trials_sorted):
            passing += 1
    return passing / len(by_case)


def pass_all_k(grades: list[CaseGrade], k: int) -> float:
    """Fraction of cases where ALL first k trials passed (strict, Anthropic's pass^k)."""
    by_case: dict[str, list[CaseGrade]] = defaultdict(list)
    for g in grades:
        by_case[g.case_id].append(g)
    if not by_case:
        return 0.0
    passing = 0
    for case_id, trials in by_case.items():
        trials_sorted = sorted(trials, key=lambda g: g.trial)[:k]
        if len(trials_sorted) == k and all(t.passed for t in trials_sorted):
            passing += 1
    return passing / len(by_case)


def error_rate(grades: list[CaseGrade]) -> float:
    """Fraction of TRIALS that hit an error (not pass/fail, but an actual exception)."""
    if not grades:
        return 0.0
    errors = sum(1 for g in grades if g.error is not None)
    return errors / len(grades)


def per_tool_accuracy(
    grades: list[CaseGrade], cases: list[ToolEvalCase]
) -> dict[str, float]:
    """Returns {tool_name: accuracy} grouped by expected_tool."""
    by_tool: dict[str, list[CaseGrade]] = defaultdict(list)
    case_lookup = {c.id: c for c in cases}
    for g in grades:
        case = case_lookup.get(g.case_id)
        if case is None:
            continue
        by_tool[case.expected_tool].append(g)
    return {
        tool: sum(1 for tg in tool_grades if tg.passed) / len(tool_grades)
        for tool, tool_grades in by_tool.items()
    }
