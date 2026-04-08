"""CLI entry point for the tool-use eval harness.

Usage:
    cd infra/duckdb-server
    ANTHROPIC_API_KEY=sk-ant-... EVAL_BUDGET_USD=2.00 \\
        uv run --with anthropic --with mcp --with pydantic \\
        python -m evals.run_eval --mode=local

Modes:
    --mode=local   hits http://localhost:8000/mcp (default)
    --mode=remote  hits https://mcp.common-ground.nyc/mcp

Other flags:
    --trials=3     number of trials per case (default 3)
    --case-id=ID   run just one case by id
    --out=PATH     JSON output (default evals/runs/<timestamp>.json)
"""
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from evals.cases import CASES
from evals.metrics import error_rate, pass_all_k, pass_any_k, per_tool_accuracy
from evals.runner import run_case
from evals.verifier import grade_case

MODEL = os.environ.get("EVAL_MODEL", "claude-sonnet-4-6")

MODE_URLS = {
    "local": "http://localhost:8000/mcp",
    "remote": "https://mcp.common-ground.nyc/mcp",
}

# Rough cost estimate per case per trial (Sonnet 4.6 pricing as of Apr 2026):
# Assume ~3K input + ~500 output tokens avg per tool-use loop at 10 max turns.
COST_PER_CASE_PER_TRIAL_USD = 0.025


def estimate_cost(n_cases: int, n_trials: int) -> float:
    return n_cases * n_trials * COST_PER_CASE_PER_TRIAL_USD


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["local", "remote"], default="local")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--case-id", default=None)
    p.add_argument("--out", default=None)
    return p.parse_args()


async def main():
    args = parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY env var required", file=sys.stderr)
        sys.exit(2)

    cases = CASES if args.case_id is None else [c for c in CASES if c.id == args.case_id]
    if not cases:
        print(f"ERROR: no cases match id={args.case_id}", file=sys.stderr)
        sys.exit(2)

    estimated_cost = estimate_cost(len(cases), args.trials)
    budget = float(os.environ.get("EVAL_BUDGET_USD", "0.0"))
    print(f"Eval plan: {len(cases)} cases × {args.trials} trials against {args.mode}")
    print(f"Estimated cost: ${estimated_cost:.2f}")
    print(f"Budget (EVAL_BUDGET_USD): ${budget:.2f}")
    if budget < estimated_cost:
        print(
            f"ERROR: estimated cost ${estimated_cost:.2f} exceeds "
            f"EVAL_BUDGET_USD=${budget:.2f}. Set a higher budget to proceed.",
            file=sys.stderr,
        )
        sys.exit(3)

    mcp_url = MODE_URLS[args.mode]
    print(f"Running eval against {mcp_url} with model {MODEL}")
    print()

    all_grades = []
    all_runs = []
    for case in cases:
        for trial in range(args.trials):
            print(f"  [{case.id}] trial {trial + 1}/{args.trials}...", end=" ", flush=True)
            result = await run_case(case, mcp_url=mcp_url, trial=trial, model=MODEL)
            all_runs.append(result)
            grade = grade_case(
                case,
                tool_calls=list(result.tool_calls),
                error=result.error,
                trial=trial,
            )
            all_grades.append(grade)
            status = "PASS" if grade.passed else ("ERROR" if grade.error else "FAIL")
            print(status)

    # Report
    print()
    print("=" * 60)
    print(f"PASS@1 (strict): {pass_all_k(all_grades, k=1):.1%}")
    print(f"PASS-ANY@{args.trials} (lenient): {pass_any_k(all_grades, k=args.trials):.1%}")
    print(f"PASS-ALL@{args.trials} (consistent): {pass_all_k(all_grades, k=args.trials):.1%}")
    print(f"Error rate (trials): {error_rate(all_grades):.1%}")
    print()
    print("Per-tool accuracy:")
    for tool, acc in sorted(per_tool_accuracy(all_grades, cases).items()):
        print(f"  {tool:20s} {acc:.1%}")

    # Save artifact
    out_path = Path(args.out) if args.out else Path("evals/runs") / (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + ".json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
        "model": MODEL,
        "n_cases": len(cases),
        "n_trials": args.trials,
        "metrics": {
            "pass_at_1": pass_all_k(all_grades, k=1),
            "pass_any_k": pass_any_k(all_grades, k=args.trials),
            "pass_all_k": pass_all_k(all_grades, k=args.trials),
            "error_rate": error_rate(all_grades),
            "per_tool": per_tool_accuracy(all_grades, cases),
        },
        "grades": [
            {
                "case_id": g.case_id,
                "trial": g.trial,
                "passed": g.passed,
                "tool_selected": g.tool_selected,
                "required_params_ok": g.required_params_ok,
                "error": g.error,
            }
            for g in all_grades
        ],
        "runs": [
            {
                "case_id": r.case_id,
                "trial": r.trial,
                "turns": r.turns,
                "tool_calls": list(r.tool_calls),
                "final_text": r.final_text,
                "error": r.error,
            }
            for r in all_runs
        ],
    }, indent=2))
    print()
    print(f"Artifact: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
