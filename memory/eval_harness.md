# MCP tool-use eval harness

**TL;DR:** `infra/duckdb-server/evals/` is a 25-case deterministic grader that measures whether Claude picks the right MCP tool with the right params for realistic Common Ground prompts. Ships in April 2026.

**Why:** Without an eval harness there is no way to iterate on tool descriptions or routing accuracy; you're flying blind. PostHog tells you error rate after the fact; the eval harness tells you BEFORE you ship.

**How to apply:**

- Before deploying anything that touches tool descriptions, `INSTRUCTIONS`, or tool routing: run the eval
- After a PostHog regression: run the eval to confirm the hypothesis before changing code
- Monthly: run as a baseline, save artifact to `evals/runs/`

**Invocation:**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
ANTHROPIC_API_KEY=... EVAL_BUDGET_USD=3.00 \
    uv run --with anthropic --with mcp --with pydantic \
    python -m evals.run_eval --mode=local --trials=3
```

**Cost:** ~$1.88 per full run (25 cases × 3 trials on Sonnet 4.6). Cost-gated via `EVAL_BUDGET_USD` env var — script refuses to run if estimate exceeds budget.

**Metrics to watch:**
- `PASS@1` (strict first-try) — headline number
- `PASS-ALL@3` (consistency) — customer-facing reliability signal
- `Error rate` — infrastructure health, fix first if high

**Artifacts:** `infra/duckdb-server/evals/runs/<timestamp>.json` — includes per-trial tool_calls, turns, final_text, and grades. Git-ignored.

**Runbook:** `docs/runbooks/eval-harness.md`

**Plan:** `docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md`
