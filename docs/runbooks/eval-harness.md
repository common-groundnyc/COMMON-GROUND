# MCP Tool-Use Eval Harness Runbook

## What it is

A 25-case deterministic grader for the Common Ground MCP server. Measures whether Claude picks the right tool and passes the right required params for realistic NYC civic-intelligence prompts. Lives in `infra/duckdb-server/evals/`.

## When to run

- Before deploying a change that touches tool descriptions, tool routing, or the `INSTRUCTIONS` constant
- When PostHog shows an error-rate regression
- Monthly as a regression baseline
- After adding/removing/renaming a tool (update `evals/cases.py` first)

## How to run

### Prerequisites

1. `ANTHROPIC_API_KEY` env var set
2. `EVAL_BUDGET_USD` env var set to at least the estimated cost (script will refuse to run otherwise)
3. For `--mode=local`: a running duckdb-server on `http://localhost:8000/mcp`:
   ```bash
   cd infra/duckdb-server
   uv run --with fastmcp --with starlette --with httpx --with sqlglot \
       --with duckdb --with anthropic --with mcp \
       python -m mcp_server &
   ```
4. For `--mode=remote`: the production Cloudflare-tunneled URL (no auth, public)

### Full run

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
ANTHROPIC_API_KEY=sk-ant-... EVAL_BUDGET_USD=3.00 \
    uv run --with anthropic --with mcp --with pydantic \
    python -m evals.run_eval --mode=local --trials=3
```

Expected cost: ~$1.88 for 25 cases × 3 trials at ~$0.025/trial on Sonnet 4.6.

### Single-case debugging

```bash
EVAL_BUDGET_USD=0.10 python -m evals.run_eval \
    --mode=local --trials=1 --case-id=routing_worst_landlords
```

## How to read the output

After a run you'll see:

```
============================================================
PASS@1 (strict): 72.0%
PASS-ANY@3 (lenient): 88.0%
PASS-ALL@3 (consistent): 60.0%
Error rate (trials): 4.0%

Per-tool accuracy:
  building             83.3%
  entity               66.7%
  network              50.0%
  ...
```

**Which metric matters?**

- **PASS@1** — strict first-try correctness. This is the headline number. Moves up as tool descriptions improve.
- **PASS-ANY@3** — lenient. Useful for spotting fragile routing (if pass@1 is 60% but pass-any@3 is 95%, the model knows the right tool but picks it only sometimes — a description-tuning opportunity).
- **PASS-ALL@3** — consistency. Matches Anthropic's `pass^k` semantics. This is the number you want for customer-facing reliability. A high pass@1 with a low pass-all@3 means non-determinism is hurting you.
- **Error rate** — trials that raised an exception (network, server crash, rate limit). High error rate blocks meaningful interpretation; fix infra first.

## Iterating on a regression

1. Run the eval, capture the artifact in `evals/runs/`
2. Open the JSON, find the failing cases (`"passed": false`)
3. The artifact includes the full `runs` array with `turns`, `tool_calls`, and `final_text` per trial — use these to debug without re-running
4. Read the tool descriptions in `mcp_server.py` `INSTRUCTIONS` + the failing tool's docstring
5. Tune the description (per Anthropic's Sept 2025 post, minor wording changes move the needle)
6. Re-run — compare artifacts

## When to add cases

Add a case when:
- PostHog surfaces a real user prompt that failed
- A new tool is added
- A new investigation workflow is documented in `INSTRUCTIONS`

Never add a case that relies on `hpd_violations` filtered queries (parquet orphan, per `memory/hpd_violations_parquet_orphans.md`).

## Out of scope for v1

- LLM-as-judge verifier (response quality grading) — comes later
- CI gating — comes later
- Multi-model comparison — Anthropic-only for v1
- Parallel trials — sequential only

## Source plan

`docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md`
