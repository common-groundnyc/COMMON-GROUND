# MCP Literal Audit + Eval Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the last open Literal-typing gap AND stand up a minimum-viable tool-use eval harness so MCP error rate can be measured, regression-gated, and iterated down from current ~30% (projected post-T3 of the prior plan) toward <5% sustainably.

**Architecture:** Two independent deliverables in one plan because Part A is a 2-minute prelude that Part B's eval harness will immediately re-validate. Part A: one-line `Literal[...]` fix to `query_sql.format`. Part B: a small Python harness under `infra/duckdb-server/evals/` — fixtures + deterministic verifier + a CLI runner against the Anthropic SDK — plus a first 25-case dataset drawn from documented Common Ground investigation workflows.

**Tech Stack:** FastMCP 3.x, Pydantic v2, Python 3.13, Anthropic SDK (`claude_agent_sdk` or direct `anthropic.AsyncAnthropic`), pytest, uv.

---

## Context for the engineer

### Why this plan exists

The prior plan (`2026-04-08-mcp-error-rate-reduction.md`) shipped T1–T4 and projected the error rate from 52% → ~30% on structural fixes. Anthropic's own guidance (Sept 2025 + Jan 2026) is consistent: the last 25 points only come out with an eval loop — you cannot iterate on tool descriptions or routing without a measurement instrument. Without an eval harness we are back to PostHog post-mortems every time a regression ships.

Simultaneously, a parameter audit on every tool file turned up exactly **one** remaining enum-like parameter that isn't already Literal-typed: `query_sql.format` in `infra/duckdb-server/tools/catalog_tools.py`. Fixing it is a one-line change but it is the only prerequisite that keeps the new `query_sql` tool consistent with the legacy `query()` tool's `format` parameter (which IS already Literal in `tools/query.py`).

The rest of the codebase is already at the gold standard Pamela Fox's March 2026 eval recommends. The harness will validate that claim and catch the next drift.

### Part A research — Literal / enum patterns (April 2026 canonical)

**Sources (fresh as of this session):**

- **[Pamela Fox — Do Stricter MCP Tool Schemas Increase Agent Reliability? (Mar 2026)](https://blog.pamelafox.org/2026/03/do-stricter-mcp-tool-schemas-increase.html)** — Ran a live 4-variant eval with PydanticAI. Key finding: `Annotated[Category, Field(description="...")]` with a Python `Enum` or `Literal[...]` beats free-text `str` descriptions because the emitted JSON schema contains an `enum` array that clients validate against *before* calling the tool.
- **[Mastra — Reducing tool calling error rates from 15% to 3% (May 2025)](https://mastra.ai/blog/mcp-tool-compatibility-layer)** — Cross-model compat table across 12+ models. Literal and Enum are universally supported. Pitfalls: Gemini silently ignores `format: date/uri`, OpenAI reasoning models (o3, o4-mini) throw on unknown schema features. Mastra's fix was to inject constraints as JSON strings into the `description`. Not our problem for this task — we only have Literal/enum to add, not `format`.
- **[Anthropic — Writing Effective Tools for Agents (Sep 2025)](https://www.anthropic.com/engineering/writing-tools-for-agents)** — "Input parameters should be unambiguously named" + "Strict schemas prevent a class of errors before the model calls the tool."
- **[FastMCP v2 tools docs](https://gofastmcp.com/v2/servers/tools)** — Confirms `Literal` and `Enum` are translated to JSON schema `enum` automatically in FastMCP 3.x.

**Canonical pattern (what we'll use in Part A):**

```python
from typing import Annotated, Literal
from pydantic import Field

format: Annotated[
    Literal["text", "xlsx", "csv"],
    Field(
        description="'text' for in-conversation results, 'xlsx' for Excel download, 'csv' for CSV download.",
        default="text",
    ),
] = "text",
```

This emits:

```json
"format": {
    "type": "string",
    "enum": ["text", "xlsx", "csv"],
    "description": "'text' for in-conversation results, 'xlsx' for Excel download, 'csv' for CSV download.",
    "default": "text"
}
```

The gold standard already exists in `infra/duckdb-server/tools/query.py:481-487` (legacy `query()` function). `catalog_tools.py` degraded it accidentally during T2 of the prior plan. This is the only fix needed.

### Part A research — parameter audit (repo scan)

A subagent swept every tool file in `infra/duckdb-server/tools/` and enumerated every parameter of every registered tool function. Result: **13 out of 14 enum-like parameters are already correctly `Literal[...]`**.

Already-correct gold-standard parameters (do NOT touch):

| Tool | Param | Literal values |
|---|---|---|
| `building` | `view` | `full, story, block, similar, enforcement, history, flippers` |
| `civic` | `view` | `contracts, permits, jobs, budget, events` |
| `entity` | `role` | `auto, background, cop, judge, vitals, top` |
| `health` | `view` | `full, covid, facilities, inspections, environmental` |
| `legal` | `view` | `full, litigation, settlements, hearings, inmates, claims` |
| `neighborhood` | `view` | `full, compare, gentrification, environment, hotspot, area, restaurants` |
| `network` | `type` | `all, ownership, corporate, political, property, contractor, tradewaste, officer, clusters, cliques, worst` |
| `query` | `mode` | `sql, nl, catalog, schemas, tables, describe, health, admin` |
| `query` | `format` | `text, xlsx, csv` |
| `safety` | `view` | `full, crashes, shootings, force, hate, summons` |
| `semantic_search` | `domain` | `auto, complaints, violations, entities, explore` |
| `services` | `view` | `full, childcare, food, shelter, benefits, legal_aid, community` |
| `transit` | `view` | `full, parking, ridership, traffic, infrastructure` |

**The only audit target:**

| Tool | Param | Current Type | Fix |
|---|---|---|---|
| `query_sql` in `tools/catalog_tools.py` | `format` | `Annotated[str, Field(...)]` | Add `Literal["text", "xlsx", "csv"]` to mirror `query.py`'s version |

Tools with intentionally polymorphic/free-text params (do NOT touch):

- `school.query` — auto-routes between DBN, ZIP, district, school name via regex
- `suggest.topic` — free text by design, internal dict lookup
- `query_sql.sql` — SQL is unbounded
- `describe_table.table` / `list_tables.schema` — fuzzy-matched identifier strings, not enums
- All `*_report` `address`, `building.identifier`, `entity.name`, `network.name`, `*.location`, `*.query` — free-text identifiers

### Part B research — eval harness methodology (April 2026 canonical)

**Primary sources:**

- **[Anthropic — Writing Effective Tools for Agents (Sep 2025)](https://www.anthropic.com/engineering/writing-tools-for-agents)** — The eval methodology:
  - Start with 20–50 tasks, not hundreds
  - Each task = prompt + verifiable outcome
  - Use `while`-loops (one per task) alternating LLM calls + tool calls
  - Track accuracy, tool-call count, error rate, turns-to-success
  - For verifiers: start with exact-match / code-based, add LLM-as-judge only for open-ended quality
  - Iterate by pasting eval transcripts into Claude Code → Claude proposes tool description changes → re-run eval
- **[Anthropic — Demystifying Evals for AI Agents (Jan 2026)](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents)** — Grader taxonomy:
  - **Code-based (deterministic)**: exact match, regex, JSON field comparison — cheap, fast, use first
  - **LLM-as-judge**: rubric scoring for open-ended quality — slower, good for response text
  - **Human**: only for calibration / domain experts
  - Non-determinism: track `pass@k` (any of k trials passes) AND `pass^k` (all of k trials pass) for customer-facing consistency
- **[Pamela Fox — Do Stricter MCP Tool Schemas (Mar 2026)](https://blog.pamelafox.org/2026/03/do-stricter-mcp-tool-schemas-increase.html)** — Concrete eval runner shape:

  ```python
  for variant in ["cat_b", "cat_c", "cat_d", "cat_e"]:
      toolset = server.filtered(tool_filter=lambda t: t.name == f"add_expense_{variant}")
      agent = Agent(model, toolsets=[toolset], ...)
      for case in EXPENSE_CASES:
          result = await agent.run(case.prompt)
          evals = run_all_evaluations(result.tool_calls, case)
  ```

- **[AllenAI — mcp-tool-eval](https://github.com/allenai/mcp-tool-eval)** — Lightweight harness: inference → metrics separation, runs LitQA2 + SimpleQA with MCP tools. Good reference for directory layout.
- **[MCPMark benchmark](https://github.com/eval-sys/mcpmark)** — 127 tasks × 38 models; confirms the shape of serious MCP benchmarks. Too heavy for our first harness; good north star.
- **[Stacklok mcp-tef](https://github.com/StacklokLabs/mcp-tef)** — Tool description evaluation with similarity detection and 1-10 quality scoring. Optional follow-up.

**What we will adopt:**

1. **Dataset size:** 25 cases for v1 (Anthropic's 20–50 recommendation, rounded to a clean number).
2. **Grader types:** deterministic for tool-selection + required-param correctness (covers ~80% of real errors), defer LLM-as-judge to a v2 plan.
3. **Runner:** direct `anthropic` Python SDK (already pinned in the duckdb-server tree per memory) with a tool-use loop — do NOT pull in LangChain or LlamaIndex; one less dependency to manage.
4. **Metrics reported:** `pass@1`, `pass@3` (three trials per case to absorb non-determinism), error rate, avg turns-to-success, per-tool selection accuracy.
5. **Run modes:** two — `--mode=local` hits a locally-running duckdb-server, `--mode=remote` hits `https://mcp.common-ground.nyc`. Default is `local`.
6. **Cost gating:** every run prints estimated token cost before starting; require `EVAL_BUDGET_USD` env var >= estimate to proceed. Prevents accidental $50 runs.
7. **Output:** a JSON artifact (`evals/runs/<timestamp>.json`) + a human summary printed to stdout + a single-line CI-friendly "PASS@1: 68%" style header.
8. **CI gating:** NOT in v1 — defer to a follow-up plan once the harness is stable.

### Part B — dataset design principles

The 25 cases will be drawn from these sources:

1. **Documented workflows from `INSTRUCTIONS`** (from the data-structure research above):
   - Landlord investigation: `building()` → `entity()` → `network(type="ownership")`
   - Follow the money: `entity()` → `network(type="political")` → `civic(view="contracts")`
   - School comparison: `school("02M475,02M001")`
   - Health equity: `health("10456")` → `services("10456")` → `neighborhood("10456")`
   - Unknown table exploration: `catalog_search("eviction")` → `describe_table(...)` → `query_sql(...)`

2. **The 5 routing rules most likely to trip up an LLM** (from the INSTRUCTIONS "ROUTING" block):
   - Address → `building()` vs `address_report()` (subtle split: specific question vs wow demo)
   - ZIP → `neighborhood()` vs `health()` vs `safety()` (depends on the domain verb)
   - Person name → `entity(role="auto")` vs `entity(role="cop")` vs `entity(role="judge")`
   - "Worst landlords" → `network(type="worst")` NOT `building()`
   - "Restaurants in Brooklyn" → `neighborhood(view="restaurants")` NOT `health()`

3. **Anti-hallucination test cases** (things that should fail cleanly):
   - Ask for national/federal statistics (should refuse with "NYC-only data")
   - Ask for a tool that doesn't exist (should say so, not invent)
   - Ask to CREATE/DROP via `query_sql` (should get the upgraded DDL error pointing at `query(mode='admin')`)

4. **Realistic concrete examples** (from `entity()` and `building()` docstrings and memory files):
   - Steven Croman (notorious landlord → entity + network)
   - BBL `1000670001` (Empire State Building block, Manhattan)
   - ZIP `10456` (Tremont, Bronx — used in INSTRUCTIONS health-equity workflow)
   - DBN `02M475` (Manhattan District 2 — used in INSTRUCTIONS school-compare workflow)
   - `305 Linden Blvd, Brooklyn` (address used in `address_report()` docstring)

5. **Known gotchas to AVOID in eval prompts** (from memory files):
   - Do NOT write prompts that filter `lake.housing.hpd_violations` by column — parquet orphans will fail the eval for infrastructure reasons, not tool-use reasons
   - Do NOT assume column names — use `describe_table` first (or use documented tables only)
   - Do NOT bypass the cursor pool — all tests go through the MCP server, never direct DuckDB

### Part B — data structures map (for constructing eval prompts)

(Condensed from the repo-exploration subagent output.)

**14 schemas in the lake:**

| Schema | Most used tables | Primary use cases in tools |
|---|---|---|
| `housing` | `hpd_violations`, `acris_master`, `acris_parties`, `evictions`, `hpd_jurisdiction` | `building()`, `network(type="ownership")` |
| `city_government` | `pluto`, `campaign_contributions`, `civil_litigation`, `contract_awards`, `oath_hearings` | `civic()`, `legal()`, `network(type="political")` |
| `public_safety` | `nypd_complaints_historic`, `motor_vehicle_collisions`, `shootings`, `ccrb_allegations` | `safety()` |
| `social_services` | `n311_service_requests`, `dycd_program_sites`, `snap_centers`, `dhs_shelter_census` | `services()`, `semantic_search()` |
| `education` | `ela_results`, `math_results`, `quality_reports`, `chronic_absenteeism` | `school()` |
| `health` | `restaurant_inspections`, `cdc_places`, `covid_by_zip`, `rodent_inspections` | `health()`, `neighborhood(view="restaurants")` |
| `transportation` | `parking_violations`, `mta_daily_ridership`, `traffic_volume`, `pothole_orders` | `transit()` |
| `environment` | `air_quality`, `lead_children`, `flood_vulnerability` | `neighborhood(view="environment")` |
| `business` | `nys_corporations`, `issued_licenses`, `dcwp_charges` | `entity()`, `network(type="corporate")` |
| `financial` | `nys_attorney_registrations`, `nys_tax_warrants` | `entity(role="background")` |
| `federal` | `acs_zcta_demographics`, `cl_judges`, `usaspending_contracts`, `fec_contributions` | `entity(role="judge")`, `neighborhood(view="compare")` |
| `foundation` | `address_lookup`, `mv_building_hub`, `mv_corp_network`, `h3_index` | internal: all address/building resolution |
| `spatial` | `nypd_crimes`, `rat_inspections`, `subway_stops` | `safety()`, `transit()` |
| `recreation` | parks, pools, permits | `services()` |

**21 registered tools:**

15 domain super-tools: `address_report`, `building`, `civic`, `entity`, `health`, `legal`, `neighborhood`, `network`, `safety`, `school`, `semantic_search`, `services`, `suggest`, `transit`, `query`

6 catalog tools (new in prior plan): `list_schemas`, `list_tables`, `describe_table`, `catalog_search`, `health_check`, `query_sql`

**Key entity types for eval prompts:**

- **BBL:** 10-digit `[boro][5-block][4-lot]` — e.g. `1000670001`, `2039720033`
- **ZIP:** 5-digit NYC — e.g. `10003`, `10456`, `11201`
- **DBN:** school code — e.g. `02M475`, `02M001`
- **Person/company:** free text, fuzzy — e.g. `Steven Croman`, `BLACKSTONE GROUP`
- **Address:** normalized PAD format — e.g. `305 Linden Blvd, Brooklyn`
- **Precinct:** integer 1–123

### Repo orientation for Part B

- `infra/duckdb-server/` is a FastMCP Python server with its own dependency set. It runs under Python 3.13 via `uv run --with fastmcp --with starlette ...`. It does NOT share the parent Dagster project's dependencies.
- The MCP server URL (when deployed) is `https://mcp.common-ground.nyc/mcp` per the PostHog middleware setup in `mcp_server.py`. Locally it runs on `http://localhost:8000/mcp`.
- Tests live in `infra/duckdb-server/tests/` and are invoked from that directory via `uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest pytest tests/ -v`.
- The eval harness is NOT a test — it's a runnable script. It will live in a new `infra/duckdb-server/evals/` directory parallel to `tests/`. Its own metadata-tests (testing the harness itself) go in `infra/duckdb-server/tests/test_evals_harness.py`.
- **Secrets:** `ANTHROPIC_API_KEY` is in `.env.secrets` / `.env.secrets.enc` (SOPS-encrypted). NEVER read those files. Pass the key via env var only. The harness MUST fail clearly if `ANTHROPIC_API_KEY` is not set — do not hardcode, do not prompt, do not fall back.
- **Dagster Docker rule:** this server is NOT Dagster. You can run the harness directly with `uv run`. You do NOT need `docker compose exec`.

### What's deliberately out of scope

- **LLM-as-judge verifier** — defer to v2 plan. Deterministic verifiers cover ~80% of real errors.
- **CI gating** — defer. Manual runs only for v1; once baseline is stable, gate PRs in a follow-up plan.
- **Parallel/concurrent eval runs** — defer. Sequential is fine for 25 cases.
- **Multi-model comparison** (Anthropic vs OpenAI vs Gemini) — Anthropic only for v1. The MCP compat layer work from Mastra would matter if we cared about cross-client routing; for now we're measuring Claude's routing behavior against our own server.
- **Held-out test set** — a single 25-case set for v1. Splitting into train/test is premature before we've run the harness once.
- **Tool description auto-optimization** (the Claude-Code-refines-tool-descriptions loop from Anthropic's post) — defer. First we need a baseline.
- **Literal audit of other codebases** — the other duckdb-server tools are already compliant.

---

## File Structure

**Part A (1 file touched):**
- Modify: `infra/duckdb-server/tools/catalog_tools.py` — change `query_sql.format` from `Annotated[str, ...]` to `Annotated[Literal["text", "xlsx", "csv"], ...]`.
- Add: `infra/duckdb-server/tests/test_catalog_tools_schema.py` — assert the emitted JSON schema contains the `enum` array.

**Part B (new directory tree):**

```
infra/duckdb-server/evals/
├── __init__.py              # empty, package marker
├── README.md                # how to run, how to read results
├── cases.py                 # 25 ToolEvalCase fixtures
├── verifier.py              # deterministic grader (tool name + required params)
├── runner.py                # agent loop: anthropic SDK + MCP client
├── metrics.py               # aggregation: pass@k, error rate, per-tool accuracy
├── run_eval.py              # CLI entry point, cost gating, JSON + stdout report
└── runs/                    # git-ignored directory for run artifacts
    └── .gitkeep
```

**Tests for the harness itself:**

- `infra/duckdb-server/tests/test_evals_harness.py` — unit tests for `verifier.py` and `metrics.py` (no live API calls)

**Repo-wide:**

- `.gitignore` addition: `infra/duckdb-server/evals/runs/*.json`
- `docs/runbooks/eval-harness.md` — runbook entry for how to run the eval harness, interpret results, iterate on a regression

Each file has one responsibility:

| File | Responsibility |
|---|---|
| `cases.py` | Pure data. 25 dataclass instances. No logic. |
| `verifier.py` | Grade one result against one case. No I/O. Deterministic. |
| `runner.py` | Run one case end-to-end via the MCP server. Network I/O. |
| `metrics.py` | Aggregate grades across cases + trials. No I/O. |
| `run_eval.py` | CLI glue. Env var parsing, cost gating, dispatch, report formatting. |

---

## Task A1: Tighten `query_sql.format` to Literal

**Why first:** Trivial 1-line change. Landing it first means Part B's harness will immediately exercise the corrected schema. Also: keeps the commit history clean — schema fix lands independently of the harness scaffolding.

**Files:**
- Modify: `infra/duckdb-server/tools/catalog_tools.py` (the `query_sql` function, around line 130-160)
- Create: `infra/duckdb-server/tests/test_catalog_tools_schema.py`

- [ ] **Step A1.1: Write the failing test**

Create `infra/duckdb-server/tests/test_catalog_tools_schema.py`:

```python
"""Assert that the query_sql tool emits the expected JSON schema with enum-constrained format."""
import importlib

from fastmcp import FastMCP

# Bypass tools/__init__.py shadow — see tests/test_query_wrapper.py for the pattern.
_ct = importlib.import_module("tools.catalog_tools")


def _make_mcp_with_query_sql():
    mcp = FastMCP("eval-test")
    mcp.tool()(_ct.query_sql)
    return mcp


def test_query_sql_format_is_enum_in_schema():
    """query_sql.format must be typed Literal so FastMCP emits a JSON schema enum."""
    import asyncio

    async def _probe():
        mcp = _make_mcp_with_query_sql()
        tools = await mcp.get_tools()
        return tools["query_sql"]

    tool = asyncio.run(_probe())
    params = tool.parameters

    # Pydantic v2 dereferences to inline types in FastMCP 3.x
    format_schema = params["properties"]["format"]
    assert format_schema.get("enum") == ["text", "xlsx", "csv"], (
        f"Expected enum=['text','xlsx','csv'], got {format_schema}"
    )
    assert format_schema.get("type") == "string"


def test_query_sql_sql_is_free_text_in_schema():
    """query_sql.sql is intentionally free-text — no enum expected."""
    import asyncio

    async def _probe():
        mcp = _make_mcp_with_query_sql()
        tools = await mcp.get_tools()
        return tools["query_sql"]

    tool = asyncio.run(_probe())
    sql_schema = tool.parameters["properties"]["sql"]
    assert sql_schema.get("type") == "string"
    assert "enum" not in sql_schema
```

- [ ] **Step A1.2: Run the test to confirm it fails**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_catalog_tools_schema.py -v
```

Expected: `test_query_sql_format_is_enum_in_schema` FAILS because `format` is currently `str` — no `enum` array in emitted schema. `test_query_sql_sql_is_free_text_in_schema` PASSES.

- [ ] **Step A1.3: Apply the Literal fix**

In `infra/duckdb-server/tools/catalog_tools.py`, find the `query_sql` function (around line 130-160). The current `format` parameter is:

```python
format: Annotated[
    str,
    Field(
        description="'text' for in-conversation results, 'xlsx' for Excel download, 'csv' for CSV download.",
        default="text",
    ),
] = "text",
```

Replace with:

```python
format: Annotated[
    Literal["text", "xlsx", "csv"],
    Field(
        description="'text' for in-conversation results, 'xlsx' for Excel download, 'csv' for CSV download.",
        default="text",
    ),
] = "text",
```

Also make sure `Literal` is imported at the top of the file. The current imports are:

```python
from typing import Annotated
```

Change to:

```python
from typing import Annotated, Literal
```

- [ ] **Step A1.4: Run the test — expect pass**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_catalog_tools_schema.py tests/test_catalog_tools.py tests/test_query_wrapper.py tests/test_error_guidance.py tests/test_server_instructions.py -v
```

Expected: all 17 tests (2 new schema + 15 from prior plan) pass.

- [ ] **Step A1.5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/catalog_tools.py \
        infra/duckdb-server/tests/test_catalog_tools_schema.py
git -c commit.gpgsign=false commit -m "fix(mcp): tighten query_sql.format to Literal enum

The legacy query() tool already uses Literal['text','xlsx','csv'] for
the format parameter. query_sql (split out in the prior plan) accidentally
degraded it to plain str. Restore the Literal typing so FastMCP emits a
JSON schema enum that clients validate against.

Closes the last open Literal audit target identified in
docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md"
```

---

## Task B1: Scaffold the evals directory with an empty runner and one smoke-test case

**Why:** Get the directory layout and dependency wiring landed before writing 25 cases. This also smoke-tests the Anthropic SDK + MCP client plumbing with one trivial case. If this fails, we find out on case #1 not case #25.

**Files:**
- Create: `infra/duckdb-server/evals/__init__.py`
- Create: `infra/duckdb-server/evals/cases.py` (with 1 smoke case)
- Create: `infra/duckdb-server/evals/verifier.py`
- Create: `infra/duckdb-server/evals/metrics.py`
- Create: `infra/duckdb-server/evals/runner.py`
- Create: `infra/duckdb-server/evals/run_eval.py`
- Create: `infra/duckdb-server/evals/runs/.gitkeep`
- Create: `infra/duckdb-server/tests/test_evals_harness.py`
- Modify: root `.gitignore`

- [ ] **Step B1.1: Create the empty package files**

```bash
mkdir -p /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server/evals/runs
cd /Users/fattie2020/Desktop/dagster-pipeline
touch infra/duckdb-server/evals/__init__.py
touch infra/duckdb-server/evals/runs/.gitkeep
```

Content of `infra/duckdb-server/evals/__init__.py`:

```python
"""Tool-use eval harness for the Common Ground MCP server.

Run with:
    cd infra/duckdb-server
    ANTHROPIC_API_KEY=sk-ant-... uv run --with anthropic --with mcp --with pydantic \\
        python -m evals.run_eval --mode=local

See docs/runbooks/eval-harness.md for the full workflow.
"""
```

- [ ] **Step B1.2: Write the failing test for `verifier.py` and `metrics.py`**

Create `infra/duckdb-server/tests/test_evals_harness.py`:

```python
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


def test_metrics_pass_at_k_all_passing():
    grades = [
        _verifier.CaseGrade(case_id="a", tool_selected=True, required_params_ok=True, passed=True, error=None, trial=t)
        for t in range(3)
    ]
    assert _metrics.pass_at_k(grades, k=1) == 1.0
    assert _metrics.pass_at_k(grades, k=3) == 1.0


def test_metrics_pass_at_k_one_of_three():
    grades = [
        _verifier.CaseGrade(case_id="a", tool_selected=False, required_params_ok=False, passed=False, error=None, trial=0),
        _verifier.CaseGrade(case_id="a", tool_selected=True, required_params_ok=True, passed=True, error=None, trial=1),
        _verifier.CaseGrade(case_id="a", tool_selected=False, required_params_ok=False, passed=False, error=None, trial=2),
    ]
    assert _metrics.pass_at_k(grades, k=1) == 1.0  # at least one trial passed
    assert _metrics.pass_at_k(grades, k=3) == 0.0  # not all 3 passed
```

- [ ] **Step B1.3: Run the test to confirm it fails**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_evals_harness.py -v
```

Expected: ImportError (`evals.verifier`, `evals.metrics`, `evals.cases` don't exist yet).

- [ ] **Step B1.4: Create `evals/cases.py` with one smoke case**

```python
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
```

- [ ] **Step B1.5: Create `evals/verifier.py`**

```python
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
```

- [ ] **Step B1.6: Create `evals/metrics.py`**

```python
"""Aggregate CaseGrade lists into metrics."""
from collections import defaultdict

from evals.verifier import CaseGrade


def pass_at_k(grades: list[CaseGrade], k: int) -> float:
    """pass@k: fraction of cases where AT LEAST ONE of first k trials passed.

    If k == n_trials, this is effectively pass@any (lenient).
    If k == 1, this is pass@1 (strict, first-try correctness).
    """
    by_case: dict[str, list[CaseGrade]] = defaultdict(list)
    for g in grades:
        by_case[g.case_id].append(g)

    if not by_case:
        return 0.0

    passing_cases = 0
    for case_id, trials in by_case.items():
        trials_sorted = sorted(trials, key=lambda g: g.trial)
        first_k = trials_sorted[:k]
        # pass@k means: among the first k trials, at least one passed.
        if any(t.passed for t in first_k):
            # Special case: when we want pass^k (ALL k pass), use pass_all_k below.
            # pass@k is commonly defined as "at least one of k" — we use that here.
            # BUT some definitions say "probability all k pass". Clarify: we use "any of k".
            passing_cases += 1

    # Use the strict "all-k-must-pass" for pass@k when k==3 per Anthropic's pass^k semantics.
    # Wait — need to be explicit. Use pass_any_k and pass_all_k below instead.
    return passing_cases / len(by_case)


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


def per_tool_accuracy(grades: list[CaseGrade], cases: list) -> dict:
    """Returns {tool_name: accuracy} grouped by expected_tool."""
    by_tool: dict[str, list[CaseGrade]] = defaultdict(list)
    case_lookup = {c.id: c for c in cases}
    for g in grades:
        case = case_lookup.get(g.case_id)
        if case is None:
            continue
        by_tool[case.expected_tool].append(g)
    return {
        tool: sum(1 for g in grades if g.passed) / len(grades)
        for tool, grades in by_tool.items()
    }
```

Note: the original `pass_at_k` function has ambiguous semantics. Remove it in favor of the two explicit functions `pass_any_k` and `pass_all_k`. Update the test file to call `pass_any_k` instead of `pass_at_k`:

Edit `infra/duckdb-server/tests/test_evals_harness.py` — replace the two `pass_at_k` test assertions with:

```python
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
```

And delete the `pass_at_k` function body in `metrics.py` (the version with confused semantics). Keep only `pass_any_k`, `pass_all_k`, `error_rate`, `per_tool_accuracy`.

- [ ] **Step B1.7: Create `evals/runner.py`**

```python
"""Run one eval case end-to-end against the live MCP server.

Uses the Anthropic Python SDK directly with tool-use loop. Does NOT use
LangChain/LlamaIndex — one less dep to manage. See
docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md
for the rationale.
"""
import os
from dataclasses import dataclass

import anthropic
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from evals.cases import ToolEvalCase

DEFAULT_MODEL = "claude-sonnet-4-6"
MAX_TURNS = 10


@dataclass
class RunResult:
    case_id: str
    trial: int
    tool_calls: list[dict]
    turns: int
    error: str | None
    final_text: str


async def _list_mcp_tools(mcp_url: str) -> list[dict]:
    """Fetch the tool list from the MCP server in Anthropic SDK format."""
    async with streamablehttp_client(mcp_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            # Convert MCP tool format to Anthropic SDK format
            return [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "input_schema": t.inputSchema,
                }
                for t in result.tools
            ]


async def _call_mcp_tool(mcp_url: str, name: str, args: dict) -> str:
    async with streamablehttp_client(mcp_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(name, args)
            parts = []
            for c in result.content or []:
                text = getattr(c, "text", None)
                if text:
                    parts.append(text)
            return "\n".join(parts) or "(no content)"


async def run_case(
    case: ToolEvalCase,
    mcp_url: str,
    trial: int = 0,
    model: str = DEFAULT_MODEL,
) -> RunResult:
    """Execute one eval case against a live MCP server."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var required")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    tools = await _list_mcp_tools(mcp_url)

    messages = [{"role": "user", "content": case.prompt}]
    tool_calls_made: list[dict] = []
    turns = 0

    try:
        while turns < MAX_TURNS:
            turns += 1
            resp = await client.messages.create(
                model=model,
                max_tokens=4096,
                tools=tools,
                messages=messages,
            )

            # Record any tool calls the model emitted this turn.
            for block in resp.content:
                if block.type == "tool_use":
                    tool_calls_made.append({"name": block.name, "input": block.input})

            if resp.stop_reason == "end_turn":
                final_text = "".join(
                    b.text for b in resp.content if b.type == "text"
                )
                return RunResult(
                    case_id=case.id,
                    trial=trial,
                    tool_calls=tool_calls_made,
                    turns=turns,
                    error=None,
                    final_text=final_text,
                )

            if resp.stop_reason == "tool_use":
                # Append assistant's turn and then tool results.
                messages.append({"role": "assistant", "content": resp.content})
                tool_results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        try:
                            result_text = await _call_mcp_tool(mcp_url, block.name, block.input)
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": result_text,
                            })
                        except Exception as exc:
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": f"Tool error: {exc}",
                                "is_error": True,
                            })
                messages.append({"role": "user", "content": tool_results})
                continue

            # Some other stop reason — treat as terminal.
            break

        return RunResult(
            case_id=case.id,
            trial=trial,
            tool_calls=tool_calls_made,
            turns=turns,
            error=f"max_turns_exceeded ({MAX_TURNS})",
            final_text="",
        )

    except Exception as exc:
        return RunResult(
            case_id=case.id,
            trial=trial,
            tool_calls=tool_calls_made,
            turns=turns,
            error=f"{type(exc).__name__}: {exc}",
            final_text="",
        )
```

- [ ] **Step B1.8: Create `evals/run_eval.py` (CLI entry point)**

```python
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
    for case in cases:
        for trial in range(args.trials):
            print(f"  [{case.id}] trial {trial + 1}/{args.trials}...", end=" ", flush=True)
            result = await run_case(case, mcp_url=mcp_url, trial=trial, model=MODEL)
            grade = grade_case(case, tool_calls=result.tool_calls, error=result.error, trial=trial)
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
    }, indent=2))
    print()
    print(f"Artifact: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step B1.9: Update `.gitignore`**

Add to `/Users/fattie2020/Desktop/dagster-pipeline/.gitignore`:

```
# Eval harness run artifacts (keep the directory, ignore the files)
infra/duckdb-server/evals/runs/*.json
!infra/duckdb-server/evals/runs/.gitkeep
```

- [ ] **Step B1.10: Run the harness unit tests — expect pass**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_evals_harness.py -v
```

Expected: 6 passed.

- [ ] **Step B1.11: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/evals/ \
        infra/duckdb-server/tests/test_evals_harness.py \
        .gitignore
git -c commit.gpgsign=false commit -m "feat(evals): scaffold tool-use eval harness

Minimum-viable eval harness for the Common Ground MCP server. Runs
one case end-to-end via the Anthropic Python SDK + MCP client, grades
it with a deterministic verifier (tool name + required param match),
and reports pass@1 / pass_any@k / pass_all@k / error rate / per-tool
accuracy.

Includes cost gating via EVAL_BUDGET_USD to prevent accidental large
runs. Artifacts written to evals/runs/<timestamp>.json (git-ignored).

Refs: docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md"
```

---

## Task B2: Write the 25-case dataset

**Why:** With the harness plumbing validated, fill in the real cases. 25 cases covers the 5 documented workflows, the 5 highest-risk routing decisions, the 3 anti-hallucination classes, and ~12 domain-tool sanity cases (one per super-tool to catch regressions in tool selection accuracy across the full surface).

**Files:**
- Modify: `infra/duckdb-server/evals/cases.py`

- [ ] **Step B2.1: Replace the single smoke case with the full 25**

Replace the entire contents of `infra/duckdb-server/evals/cases.py`:

```python
"""Tool-use eval cases for the Common Ground MCP server.

25 cases drawn from:
  1. Documented investigation workflows (INSTRUCTIONS block)
  2. High-risk routing decisions the LLM must get right
  3. Anti-hallucination cases (out-of-scope, invented tools, DDL)
  4. One-per-super-tool sanity coverage

Grader (evals/verifier.py) matches on:
  - expected_tool (exact tool name)
  - expected_required_params (subset: every key/value in this dict must
    appear on the tool call; extra params are allowed)

Refs: docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ToolEvalCase:
    id: str
    prompt: str
    expected_tool: str
    expected_required_params: dict = field(default_factory=dict)
    tags: tuple = field(default_factory=tuple)


CASES: list[ToolEvalCase] = [
    # ───────────────────────── Group 1: documented workflows ─────────────────────────
    ToolEvalCase(
        id="workflow_landlord_step1_building",
        prompt="I'm investigating the building at 305 Linden Blvd in Brooklyn. Give me the full report.",
        expected_tool="building",
        expected_required_params={"identifier": "305 Linden Blvd, Brooklyn"},
        tags=("workflow", "landlord", "address"),
    ),
    ToolEvalCase(
        id="workflow_landlord_step3_network_ownership",
        prompt="Show me Steven Croman's ownership portfolio — all the buildings and LLCs connected to him.",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "ownership"},
        tags=("workflow", "landlord", "network"),
    ),
    ToolEvalCase(
        id="workflow_money_step2_network_political",
        prompt="Map out Steven Croman's political donations and lobbying connections.",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "political"},
        tags=("workflow", "money", "network"),
    ),
    ToolEvalCase(
        id="workflow_money_step3_civic_contracts",
        prompt="What city contracts has Blackstone Group been awarded?",
        expected_tool="civic",
        expected_required_params={"query": "Blackstone Group", "view": "contracts"},
        tags=("workflow", "money", "civic"),
    ),
    ToolEvalCase(
        id="workflow_school_compare_dbns",
        prompt="Compare the test scores and demographics of schools 02M475 and 02M001.",
        expected_tool="school",
        expected_required_params={"query": "02M475,02M001"},
        tags=("workflow", "education", "school"),
    ),
    ToolEvalCase(
        id="workflow_health_equity_health",
        prompt="What does public health look like in ZIP 10456 in the Bronx?",
        expected_tool="health",
        expected_required_params={"location": "10456"},
        tags=("workflow", "health", "zip"),
    ),
    ToolEvalCase(
        id="workflow_health_equity_services",
        prompt="What social services are available in ZIP 10456?",
        expected_tool="services",
        expected_required_params={"location": "10456"},
        tags=("workflow", "health", "services"),
    ),
    ToolEvalCase(
        id="workflow_unknown_table_catalog_search",
        prompt="Which tables in the data lake contain eviction information?",
        expected_tool="catalog_search",
        expected_required_params={"keyword": "eviction"},
        tags=("workflow", "catalog"),
    ),

    # ──────────────────── Group 2: high-risk routing decisions ───────────────────
    ToolEvalCase(
        id="routing_address_vs_report",
        prompt="Here's my address: 305 Linden Blvd, Brooklyn. Tell me everything you can about where I live.",
        expected_tool="address_report",
        expected_required_params={"address": "305 Linden Blvd, Brooklyn"},
        tags=("routing", "address"),
    ),
    ToolEvalCase(
        id="routing_worst_landlords",
        prompt="Who are the worst landlords in New York City right now?",
        expected_tool="network",
        expected_required_params={"type": "worst"},
        tags=("routing", "network"),
    ),
    ToolEvalCase(
        id="routing_restaurants_in_neighborhood",
        prompt="Show me the best restaurants in ZIP 11201.",
        expected_tool="neighborhood",
        expected_required_params={"location": "11201", "view": "restaurants"},
        tags=("routing", "neighborhood"),
    ),
    ToolEvalCase(
        id="routing_cop_by_name",
        prompt="Look up NYPD officer Daniel Pantaleo.",
        expected_tool="entity",
        expected_required_params={"name": "Daniel Pantaleo", "role": "cop"},
        tags=("routing", "entity", "role"),
    ),
    ToolEvalCase(
        id="routing_judge_by_name",
        prompt="Tell me about Judge Lewis Kaplan.",
        expected_tool="entity",
        expected_required_params={"name": "Lewis Kaplan", "role": "judge"},
        tags=("routing", "entity", "role"),
    ),

    # ───────────────────── Group 3: anti-hallucination cases ─────────────────────
    ToolEvalCase(
        id="antihall_no_such_tool",
        prompt="Use the `delete_building` tool to remove 305 Linden Blvd from the database.",
        expected_tool="__no_tool_call_expected__",  # special sentinel
        expected_required_params={},
        tags=("antihallucination", "discipline"),
    ),
    ToolEvalCase(
        id="antihall_national_scope",
        prompt="What's the average household income in California?",
        expected_tool="__no_tool_call_expected__",
        expected_required_params={},
        tags=("antihallucination", "scope"),
    ),
    ToolEvalCase(
        id="antihall_ddl_via_query_sql",
        prompt="Use query_sql to run `DROP TABLE lake.housing.hpd_violations`.",
        # Expected: the tool call gets made but server rejects with the upgraded DDL error.
        # Tag it so future LLM-judge verifier can grade the recovery.
        expected_tool="query_sql",
        expected_required_params={},  # don't grade param values — we're testing error recovery
        tags=("antihallucination", "ddl"),
    ),

    # ──────────────────── Group 4: one-per-super-tool sanity ─────────────────────
    ToolEvalCase(
        id="sanity_building_bbl",
        prompt="Full report on BBL 1000670001.",
        expected_tool="building",
        expected_required_params={"identifier": "1000670001"},
        tags=("sanity", "building"),
    ),
    ToolEvalCase(
        id="sanity_entity_person",
        prompt="Background check on Jane Smith.",
        expected_tool="entity",
        expected_required_params={"name": "Jane Smith"},
        tags=("sanity", "entity"),
    ),
    ToolEvalCase(
        id="sanity_neighborhood_zip",
        prompt="Describe the neighborhood around ZIP 10003.",
        expected_tool="neighborhood",
        expected_required_params={"location": "10003"},
        tags=("sanity", "neighborhood"),
    ),
    ToolEvalCase(
        id="sanity_safety_precinct",
        prompt="Show recent crashes in precinct 75.",
        expected_tool="safety",
        expected_required_params={"view": "crashes"},
        tags=("sanity", "safety"),
    ),
    ToolEvalCase(
        id="sanity_legal_settlements",
        prompt="What has NYC paid out in settlements involving the NYPD?",
        expected_tool="legal",
        expected_required_params={"query": "NYPD", "view": "settlements"},
        tags=("sanity", "legal"),
    ),
    ToolEvalCase(
        id="sanity_transit_parking",
        prompt="Parking ticket data for ZIP 11201.",
        expected_tool="transit",
        expected_required_params={"location": "11201", "view": "parking"},
        tags=("sanity", "transit"),
    ),
    ToolEvalCase(
        id="sanity_semantic_search_concept",
        prompt="Find complaints about rat infestations.",
        expected_tool="semantic_search",
        expected_required_params={"query": "rat infestations"},
        tags=("sanity", "semantic_search"),
    ),
    ToolEvalCase(
        id="sanity_suggest_unsure",
        prompt="I'm new here — what kinds of things can I explore in this data?",
        expected_tool="suggest",
        expected_required_params={},
        tags=("sanity", "suggest"),
    ),
    ToolEvalCase(
        id="sanity_list_schemas_lake_question",
        prompt="What schemas exist in the NYC data lake?",
        expected_tool="list_schemas",
        expected_required_params={},
        tags=("sanity", "catalog"),
    ),
]

SMOKE_CASE = CASES[0]  # for back-compat with Task B1 smoke run
```

- [ ] **Step B2.2: Handle the `__no_tool_call_expected__` sentinel in the verifier**

Modify `infra/duckdb-server/evals/verifier.py`. Replace `grade_case`:

```python
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
    if error is not None:
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
        error=None,
        trial=trial,
    )
```

- [ ] **Step B2.3: Add a test for the sentinel behavior**

Append to `infra/duckdb-server/tests/test_evals_harness.py`:

```python
def test_verifier_no_tool_call_expected_passes_on_empty():
    case = _cases.ToolEvalCase(
        id="antihall",
        prompt="Use delete_universe()",
        expected_tool="__no_tool_call_expected__",
        expected_required_params={},
    )
    grade = _verifier.grade_case(case, tool_calls=[])
    assert grade.passed is True


def test_verifier_no_tool_call_expected_fails_on_any_call():
    case = _cases.ToolEvalCase(
        id="antihall",
        prompt="Use delete_universe()",
        expected_tool="__no_tool_call_expected__",
        expected_required_params={},
    )
    grade = _verifier.grade_case(case, tool_calls=[_fake_tool_call("building", {})])
    assert grade.passed is False
```

- [ ] **Step B2.4: Run the harness tests — expect pass**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_evals_harness.py -v
```

Expected: 8 passed (6 from B1 + 2 new).

- [ ] **Step B2.5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/evals/cases.py \
        infra/duckdb-server/evals/verifier.py \
        infra/duckdb-server/tests/test_evals_harness.py
git -c commit.gpgsign=false commit -m "feat(evals): write 25-case dataset + anti-hallucination sentinel

25 cases cover the 5 documented investigation workflows, high-risk
routing decisions (worst-landlords, restaurants-in-neighborhood, cop/judge
roles), three anti-hallucination cases, and one-per-super-tool sanity
coverage.

Adds a __no_tool_call_expected__ sentinel to the verifier so cases testing
out-of-scope refusal can grade correctly."
```

---

## Task B3: First baseline run + document findings

**Why:** A harness with no baseline data is dead code. Run it once against the local server, capture the artifact, and write the runbook so the next person knows what the numbers mean. NOTE: this step costs real money (~$2 for 25 cases × 3 trials against Sonnet 4.6). Confirm with the user before hitting this step in interactive execution.

**Files:**
- Create: `docs/runbooks/eval-harness.md`
- Run: `infra/duckdb-server/evals/run_eval.py --mode=local --trials=3`
- Create: `infra/duckdb-server/evals/runs/baseline_2026-04-08.json` (manually saved after run)

- [ ] **Step B3.1: Write the runbook first (cheap, no API calls)**

Create `docs/runbooks/eval-harness.md`:

```markdown
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

- `PASS@1` — strict first-try correctness. This is the headline number. Moves up as tool descriptions improve.
- `PASS-ANY@3` — lenient. Useful for spotting fragile routing (if pass@1 is 60% but pass-any@3 is 95%, the model knows the right tool but picks it only sometimes — a description-tuning opportunity).
- `PASS-ALL@3` — consistency. Matches Anthropic's `pass^k` semantics. This is the number you want for customer-facing reliability. A high pass@1 with a low pass-all@3 means non-determinism is hurting you.
- `Error rate` — trials that raised an exception (network, server crash, rate limit). High error rate blocks meaningful interpretation; fix infra first.

## Iterating on a regression

1. Run the eval, capture the artifact in `evals/runs/`
2. Open the JSON, find the failing cases (`"passed": false`)
3. Read the tool descriptions in `mcp_server.py` `INSTRUCTIONS` + the failing tool's docstring
4. Tune the description (per Anthropic's Sept 2025 post, minor wording changes move the needle)
5. Re-run — compare artifacts

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
```

- [ ] **Step B3.2: Commit the runbook**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add docs/runbooks/eval-harness.md
git -c commit.gpgsign=false commit -m "docs(runbook): eval harness — how to run, interpret, iterate"
```

- [ ] **Step B3.3: Decide about the baseline run**

The baseline run costs real API money and requires:
- A running local duckdb-server (or a remote URL)
- `ANTHROPIC_API_KEY` set
- `EVAL_BUDGET_USD=3.00` or higher
- Roughly 5–10 minutes of wall time

Ask the user BEFORE running. Default: **do not run in subagent execution**. The plan ships a working harness ready to run manually; the first human invocation produces the baseline.

Report to the controller that Task B3 is complete up to the runbook commit, and the baseline run is a manual step the user should take. Include the exact command.

---

## Task B4: Memory note about parameter audit outcome + eval harness existence

**Why:** The big finding of this session (codebase is already at the Literal gold standard except for one parameter) should become persistent memory so we don't re-audit it six months from now. Same for: the eval harness exists and how to invoke it.

**Files:**
- Create: `memory/tool_parameter_literals.md`
- Create: `memory/eval_harness.md`
- Modify: `memory/MEMORY.md`

- [ ] **Step B4.1: Create `memory/tool_parameter_literals.md`**

```markdown
# Tool parameter Literal audit — April 2026

**TL;DR:** The duckdb-server is already at the Pamela Fox / Mastra strict-schema gold standard. An April 2026 audit found **exactly one** enum-like parameter typed as plain `str`: `query_sql.format` in `tools/catalog_tools.py`. Fixed in commit on branch main.

**Why:** Don't re-audit unless a new tool is added. When adding a new tool, any parameter that accepts a fixed set of string values MUST use `Literal[...]` — see the gold-standard list below.

**How to apply:** When reviewing a new tool file, check any parameter named `view`, `mode`, `role`, `type`, `domain`, `format`, `kind`, `category`, `level`, `scope`. If it accepts a fixed set of strings, type it as:

```python
param: Annotated[
    Literal["a", "b", "c"],
    Field(description="..."),
] = "a"
```

FastMCP 3.x emits this as a JSON schema `enum`, which clients validate against before calling the tool.

**Gold-standard examples already in the codebase:**
- `building.view` — 7-value Literal
- `network.type` — 11-value Literal (the biggest)
- `query.mode` — 8-value Literal
- `entity.role` — 6-value Literal

**Audit source:** `docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md`
```

- [ ] **Step B4.2: Create `memory/eval_harness.md`**

```markdown
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

**Runbook:** `docs/runbooks/eval-harness.md`

**Plan:** `docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md`
```

- [ ] **Step B4.3: Update `memory/MEMORY.md`**

Read the existing `memory/MEMORY.md` first. Add these two lines to the index in the appropriate section (likely under a "Tooling" or "Observability" heading — if no such heading exists, add one):

```markdown
- [Tool parameter Literals](tool_parameter_literals.md) — April 2026 audit: codebase already at gold standard, one parameter fixed
- [Eval harness](eval_harness.md) — how to run the MCP tool-use eval harness before deploys and after PostHog regressions
```

Do not rewrite the whole index. Just append the two lines in the appropriate section. If no appropriate section exists, add a `## Tooling` heading.

- [ ] **Step B4.4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add memory/tool_parameter_literals.md \
        memory/eval_harness.md \
        memory/MEMORY.md
git -c commit.gpgsign=false commit -m "docs(memory): parameter Literal audit outcome + eval harness pointer"
```

---

## Task B5: Final verify + hand-off

- [ ] **Step B5.1: Run the full test suite**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot --with pytest \
    pytest tests/test_query_wrapper.py \
           tests/test_catalog_tools.py \
           tests/test_catalog_tools_schema.py \
           tests/test_error_guidance.py \
           tests/test_server_instructions.py \
           tests/test_evals_harness.py -v
```

Expected: 19 passed (15 from prior plan + 2 new schema + 8 evals harness — actually 15+2+8=25 but some overlap; the important thing is everything passes and no regressions).

- [ ] **Step B5.2: Git status + commit summary**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git status
git log --oneline main@{1}..HEAD
git diff --stat main@{1}..HEAD
```

Expected: clean working tree, ~5 new commits, new directory `infra/duckdb-server/evals/`, new tests, runbook, memory notes.

- [ ] **Step B5.3: Write the hand-off message**

Summarize for the user:
1. Part A landed — `query_sql.format` is now Literal. Verified via new schema test.
2. Part B landed — eval harness scaffold + 25 cases + verifier + runner + metrics + CLI + runbook + memory notes.
3. **Manual step required from the user**: run the baseline.
4. Provide the exact command from the runbook.
5. Suggest: after the first baseline, PostHog data from 48h post-deploy of the prior plan should ALSO be checked against the baseline numbers to correlate eval results with real-world error rates.
6. Point at `docs/runbooks/eval-harness.md` for everything else.

---

## Out-of-scope follow-ups (deliberately not in this plan)

1. **LLM-as-judge verifier** — grade response quality (text content) in addition to tool selection. Separate plan. Blocked on v1 baseline to know whether it's needed.
2. **CI gating** — gate PRs on eval deltas. Blocked on v1 baseline + a stable 3-run variance measurement.
3. **Tool description auto-tuning via Claude Code** — the Anthropic Sept 2025 loop where Claude Code proposes tool description changes based on eval transcripts. Needs the baseline first.
4. **Multi-model eval comparison** — run against OpenAI and Gemini too to see cross-client routing stability. Needs the Mastra compat layer work first.
5. **Larger dataset** — expand from 25 → 100 cases once the first baseline reveals gaps.
6. **Eval harness in Docker** — currently runs from the host. If we want to run it inside CI or alongside dagster, containerize it. Not urgent.
7. **Parquet orphan re-materialize** — already running via freshness sensor (see prior plan).
