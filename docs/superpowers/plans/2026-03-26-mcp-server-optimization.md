# MCP Server Optimization — March 2026 Best Practices

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize the Common Ground FastMCP server for LLM token efficiency, tool routing accuracy, and multi-step query performance using late-March 2026 best practices.

**Architecture:** Add semantic tags to all 43 tools for future per-session filtering. Switch output to Markdown tables for ~30% token savings. Enable Code Mode (FastMCP 3.1 experimental) to collapse multi-step queries into single round-trips. Fix 2 tools missing `annotations=READONLY`. Keep existing BM25SearchTransform + 10 always-visible tools unchanged (already optimized).

**Tech Stack:** FastMCP 3.1.1 (already installed), Python 3.12, Pydantic `Field` + `Annotated`, DuckDB 1.5.0

**Research basis:** [MCP Performance Optimization Guide](https://mcpguide.dev/blog/mcp-performance-optimization), [10 Strategies to Reduce MCP Token Bloat (The New Stack)](https://thenewstack.io/how-to-reduce-mcp-token-bloat/), [FastMCP 3.1 Code Mode](https://www.jlowin.dev/blog/fastmcp-3-1-code-mode), [Stricter MCP Tool Schemas (Pamela Fox)](https://blog.pamelafox.org/2026/03/do-stricter-mcp-tool-schemas-increase.html)

**Supersedes:** `docs/superpowers/plans/2026-03-26-mcp-tool-communication.md` (Tasks 1-3 from that plan are already deployed; this plan covers the remaining work plus new optimizations from research).

**Already deployed** (from prior plan):
- [x] Compact INSTRUCTIONS to routing-only fallback
- [x] Reduce ALWAYS_VISIBLE from 26 → 10
- [x] `Annotated[..., Field()]` imports and type aliases (BBL, ZIP, NAME)
- [x] All 43 tool function signatures annotated with Field descriptions
- [x] All 43 tool docstrings rewritten with routing guidance and cross-references

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `infra/duckdb-server/mcp_server.py` | Modify | Tags, READONLY fixes, Markdown output, Code Mode |

Single file, ~9610 lines. Changes to decorators, one utility function, and the server constructor. No SQL logic changes. No new files.

---

### Task 1: Add semantic tags to all 43 tools

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — all 43 `@mcp.tool()` decorators

Tags enable future per-session visibility filtering via `mcp.disable(tags={"admin"})` or `mcp.enable(tags={"graph"}, only=True)`. They also improve Code Mode's tool discovery (Task 4). No behavior change today — groundwork only.

**Note:** `gentrification_tracker` (line 3291) and `safety_report` (line 3752) currently use bare `@mcp.tool()` without `annotations=READONLY`. This task adds both tags AND the missing `annotations=READONLY` to those 2 tools. This is an intentional fix — both are read-only query tools that were missing the annotation.

- [ ] **Step 1: Add tags to all `@mcp.tool()` decorators**

Apply tag sets to each tool decorator. The `@mcp.tool()` line is the only change per tool — function body is untouched. Find each tool's `@mcp.tool(...)` line and update it.

**discovery** — data exploration entry points:
```python
@mcp.tool(annotations=READONLY, tags={"discovery"})  # sql_query (line ~2347)
@mcp.tool(annotations=ADMIN, tags={"discovery"})      # sql_admin (line ~2363)
@mcp.tool(annotations=READONLY, tags={"discovery"})   # list_schemas (line ~2389)
@mcp.tool(annotations=READONLY, tags={"discovery"})   # list_tables (line ~2401)
@mcp.tool(annotations=READONLY, tags={"discovery"})   # describe_table (line ~2416)
@mcp.tool(annotations=READONLY, tags={"discovery"})   # data_catalog (line ~2645)
@mcp.tool(annotations=READONLY, tags={"discovery"})   # text_search (line ~2673)
```

**building** — building-centric investigation:
```python
@mcp.tool(annotations=READONLY, tags={"building"})    # building_profile (line ~2465)
@mcp.tool(annotations=READONLY, tags={"building"})    # building_story (line ~5376)
@mcp.tool(annotations=READONLY, tags={"building"})    # building_context (line ~6248)
@mcp.tool(annotations=READONLY, tags={"building"})    # nyc_twins (line ~5848)
@mcp.tool(annotations=READONLY, tags={"building"})    # block_timeline (line ~6026)
```

**housing** — landlord/tenant/violation analysis:
```python
@mcp.tool(annotations=READONLY, tags={"housing"})     # landlord_watchdog (line ~4101)
@mcp.tool(annotations=READONLY, tags={"housing"})     # owner_violations (line ~2618)
@mcp.tool(annotations=READONLY, tags={"housing"})     # complaints_by_zip (line ~2588)
```

**investigation** — person/entity lookup:
```python
@mcp.tool(annotations=READONLY, tags={"investigation"})  # entity_xray (line ~7116)
@mcp.tool(annotations=READONLY, tags={"investigation"})  # person_crossref (line ~7500 approx)
@mcp.tool(annotations=READONLY, tags={"investigation"})  # top_crossrefs (line ~9378)
@mcp.tool(annotations=READONLY, tags={"investigation"})  # llc_piercer (line ~7700 approx)
@mcp.tool(annotations=READONLY, tags={"investigation"})  # marriage_search (line ~7875)
```

**neighborhood** — area/ZIP-level analysis:
```python
@mcp.tool(annotations=READONLY, tags={"neighborhood"})   # neighborhood_portrait (line ~5637)
@mcp.tool(annotations=READONLY, tags={"neighborhood"})   # neighborhood_compare (line ~2986)
@mcp.tool(annotations=READONLY, tags={"neighborhood"})   # area_snapshot (line ~2863)
@mcp.tool(annotations=READONLY, tags={"neighborhood"})   # gentrification_tracker (line 3291) ← ADDING annotations=READONLY (was bare @mcp.tool())
```

**safety** — crime/policing:
```python
@mcp.tool(annotations=READONLY, tags={"safety"})      # safety_report (line 3752) ← ADDING annotations=READONLY (was bare @mcp.tool())
```

**environment** — environmental justice:
```python
@mcp.tool(annotations=READONLY, tags={"environment"})  # environmental_justice (line ~4700 approx)
```

**services** — public services lookup:
```python
@mcp.tool(annotations=READONLY, tags={"services"})    # resource_finder (line ~4793)
@mcp.tool(annotations=READONLY, tags={"services"})    # school_report (line ~5020)
@mcp.tool(annotations=READONLY, tags={"services"})    # restaurant_lookup (line ~2758)
@mcp.tool(annotations=READONLY, tags={"services"})    # commercial_vitality (line ~3500 approx)
```

**graph** — DuckPGQ network analysis:
```python
@mcp.tool(annotations=READONLY, tags={"graph"})       # landlord_network (line ~6405)
@mcp.tool(annotations=READONLY, tags={"graph"})       # ownership_graph (line ~6666)
@mcp.tool(annotations=READONLY, tags={"graph"})       # ownership_clusters (line ~6716)
@mcp.tool(annotations=READONLY, tags={"graph"})       # ownership_cliques (line ~6796)
@mcp.tool(annotations=READONLY, tags={"graph"})       # worst_landlords (line ~6847)
@mcp.tool(annotations=READONLY, tags={"graph"})       # transaction_network (line ~7500 approx)
@mcp.tool(annotations=READONLY, tags={"graph"})       # corporate_web (line ~8600 approx)
@mcp.tool(annotations=READONLY, tags={"graph"})       # shell_detector (line ~9166)
@mcp.tool(annotations=READONLY, tags={"graph"})       # contractor_network (line ~9014)
```

**finance** — transactions/enforcement/political:
```python
@mcp.tool(annotations=READONLY, tags={"finance"})     # property_history (line ~7979)
@mcp.tool(annotations=READONLY, tags={"finance"})     # enforcement_web (line ~8124)
@mcp.tool(annotations=READONLY, tags={"finance"})     # flipper_detector (line ~8398)
@mcp.tool(annotations=READONLY, tags={"finance"})     # pay_to_play (line ~8700 approx)
```

- [ ] **Step 2: Verify all 43 tools still load**

Run:
```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
import asyncio
from mcp_server import mcp
async def check():
    tools = await mcp.list_tools()
    print(f'{len(tools)} tools loaded')
asyncio.run(check())
"
```
Expected: 43+ tools loaded (43 original + BM25 synthetic tools)

- [ ] **Step 3: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add semantic tags to all 43 MCP tools

Tags: discovery, building, housing, investigation, neighborhood, safety,
environment, services, graph, finance. Enables per-session visibility filtering.
Also adds missing annotations=READONLY to gentrification_tracker and safety_report."
```

---

### Task 2: Switch output to Markdown tables

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:347-371` (`format_text_table` function)

The current `format_text_table` produces pipe-delimited text with `ljust()` padding — almost Markdown but not quite. Switching to proper Markdown tables saves tokens because padding is eliminated. The `structured_content` field in `ToolResult` (line 380) still returns JSON dicts, so clients using structured data are unaffected — this only changes the `content` text field.

- [ ] **Step 1: Replace `format_text_table` function**

Replace lines 347-371 with:

```python
def format_text_table(cols, rows, max_rows=MAX_LLM_ROWS):
    if not cols:
        return "(no columns)"
    if not rows:
        return "(no rows)"

    display = rows[:max_rows]

    def cell(v):
        s = str(v)[:40] if v is not None else ""
        return s.replace("|", "\\|")

    lines = [
        "| " + " | ".join(str(c) for c in cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    lines.extend(
        "| " + " | ".join(cell(v) for v in row) + " |"
        for row in display
    )

    total = len(rows)
    if total > max_rows:
        lines.append(f"({max_rows} of {total} rows shown)")
    return "\n".join(lines)
```

Key changes from original:
- Proper Markdown header separator (`| --- | --- |`) instead of `-+-` dashes
- No `ljust()` padding — eliminates trailing whitespace on every cell
- Pipe characters in cell values escaped with `\|` (new behavior — prevents broken Markdown when violation descriptions contain `|`)
- Cell width cap stays at 40 chars (same as before — no change)

- [ ] **Step 2: Verify output format**

Run:
```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
from mcp_server import format_text_table
result = format_text_table(['name', 'age', 'city'], [('Alice', 30, 'New York'), ('Bob', 25, 'Brooklyn')])
print(result)
print()
# Test pipe escaping
result2 = format_text_table(['desc'], [('A | B',)])
print(result2)
"
```

Expected:
```
| name | age | city |
| --- | --- | --- |
| Alice | 30 | New York |
| Bob | 25 | Brooklyn |

| desc |
| --- |
| A \| B |
```

- [ ] **Step 3: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "perf: switch tool output to Markdown tables

Drops ljust() padding — fewer tokens per tool response.
Escapes pipe chars in cell values for valid Markdown rendering."
```

---

### Task 3: Audit MAX_LLM_ROWS enforcement

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` — any tools that bypass `make_result`

`MAX_LLM_ROWS = 20` is enforced inside `format_text_table`, which is called by `make_result`. But some tools return `str` instead of `ToolResult`, building their own output strings. These could dump hundreds of rows into context.

- [ ] **Step 1: Find tools that return `str` instead of `ToolResult`**

Run:
```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
grep -n "def.*ctx.*-> str:" mcp_server.py | head -20
```

Expected matches: `list_schemas` (line ~2390), `sql_admin` (line ~2364), and possibly others.

- [ ] **Step 2: Review each `str`-returning tool**

For each match, check if the returned string could exceed ~2000 characters:
- `list_schemas` — returns 12 lines (one per schema). SAFE — bounded by schema count.
- `sql_admin` — returns "OK — view created successfully." SAFE — fixed string.
- Any others — check if they concatenate unbounded result rows.

If any tool builds text with unbounded rows, truncate:
```python
# Pattern: cap output lines
if len(all_lines) > MAX_LLM_ROWS:
    all_lines = all_lines[:MAX_LLM_ROWS]
    all_lines.append(f"({total} total, showing {MAX_LLM_ROWS})")
```

- [ ] **Step 3: Spot-check `entity_xray` output size**

`entity_xray` (line ~7117) queries 23 datasets and concatenates results. Check that each section's SQL uses `LIMIT` and that the final output builder respects row caps. Read the function body (~lines 7117-7600) to verify.

- [ ] **Step 4: Commit (only if changes made)**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "perf: enforce MAX_LLM_ROWS on all tool output paths

Caps unbounded text output to prevent context window flooding."
```

---

### Task 4: Enable Code Mode (experimental)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:17` (imports)
- Modify: `infra/duckdb-server/mcp_server.py:2330-2340` (transforms list)
- Modify: `infra/duckdb-server/mcp_server.py:2288-2306` (INSTRUCTIONS constant)

Code Mode lets the LLM write a Python script that calls multiple tools in one round-trip. It adds 3 meta-tools (`search_code_mode_tools`, `get_code_mode_tool_schema`, `execute_code`) and sandboxes execution server-side. This is additive — existing direct tool calls still work unchanged.

**Important:** Code Mode is `fastmcp.experimental` — interface is stable but may change. It composes with BM25SearchTransform as a second transform in the list.

- [ ] **Step 1: Add Code Mode import**

After line 17 (`from fastmcp.server.transforms.search import BM25SearchTransform`), add:

```python
from fastmcp.experimental.transforms.code_mode import CodeMode
```

- [ ] **Step 2: Add CodeMode to transforms list**

Change the `mcp = FastMCP(...)` block (lines ~2330-2340) from:

```python
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
    ],
)
```

To:

```python
mcp = FastMCP(
    "Common Ground",
    instructions=INSTRUCTIONS,
    lifespan=app_lifespan,
    transforms=[
        BM25SearchTransform(
            max_results=5,
            always_visible=ALWAYS_VISIBLE,
        ),
        CodeMode(),
    ],
)
```

- [ ] **Step 3: Update INSTRUCTIONS to mention Code Mode**

Add to the end of the INSTRUCTIONS constant (before the closing `"""`), after the "SQL example" line:

```
POWER USER: Use execute_code to chain multiple tool calls in one step.
```

- [ ] **Step 4: Verify transforms compose**

Run:
```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
import asyncio
from mcp_server import mcp
async def check():
    tools = await mcp.list_tools()
    names = [t.name for t in tools]
    print(f'{len(tools)} tools loaded')
    for cm in ['search_code_mode_tools', 'get_code_mode_tool_schema', 'execute_code']:
        print(f'  {cm}: {\"YES\" if cm in names else \"MISSING\"}'  )
asyncio.run(check())
"
```

Expected: 43+ tools loaded, all 3 Code Mode tools present.

If the import fails (`ModuleNotFoundError`), Code Mode may not be available in 3.1.1. In that case, **skip this task entirely** — revert the import and transforms change. Code Mode is a nice-to-have, not a requirement.

- [ ] **Step 5: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: enable FastMCP Code Mode (experimental)

LLMs can now chain multiple tool calls in a single Python script execution.
Collapses multi-turn exploratory patterns into one round-trip."
```

---

### Task 5: Deploy and validate

**Files:**
- No file changes — deployment and testing only

- [ ] **Step 1: Sync to server**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
rsync -avz --delete \
  -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  infra/duckdb-server/ \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
```

- [ ] **Step 2: Rebuild Docker image**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server"
```
Expected: Build succeeds. Watch for import errors in the build log.

- [ ] **Step 3: Deploy**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d duckdb-server"
```
Expected: Container starts, logs show "Catalog cached: N tables across 12 schemas"

- [ ] **Step 4: Validate Markdown output**

Call `building_profile(bbl="1000670001")` and verify output uses `| col | col |` format with `| --- | --- |` header separator (not padded fixed-width text).

- [ ] **Step 5: Validate Code Mode (if enabled)**

Check that Code Mode meta-tools appear in the tool list:
- `search_code_mode_tools` — should exist
- `get_code_mode_tool_schema` — should exist
- `execute_code` — should exist

Test a multi-step script via `execute_code`:
```python
profile = await call_tool("building_profile", bbl="1000670001")
violations = await call_tool("owner_violations", bbl="1000670001")
return {"profile": profile, "violations": violations}
```

- [ ] **Step 6: End-to-end Claude Code test**

In Claude Code with the MCP server connected, test:
1. "Tell me about building 1000670001" → should use `building_profile`, output in Markdown table
2. "Who is Barton Perlbinder?" → should use `entity_xray`
3. "Compare 10003 and 11201" → should use `neighborhood_compare`
4. "Show me property flips in Brooklyn" → should use `flipper_detector` (via BM25)

- [ ] **Step 7: If any tests fail, fix and redeploy**

Common failure modes:
- Import error on CodeMode → remove CodeMode from transforms, redeploy
- Tags break BM25 → tags are metadata only, shouldn't affect BM25. If they do, remove tags
- Code Mode conflicts with BM25 → try reversing transform order (CodeMode first, BM25 second)
- Markdown output breaks a client → revert `format_text_table` (1 function)

---

## Token Impact Estimate

| Metric | Before | After |
|--------|--------|-------|
| Tool output format | padded fixed-width | Markdown (no padding) |
| Output tokens per tool call | ~baseline | ~70-80% of baseline |
| Multi-step query round-trips | 4-6 turns | 1 turn (Code Mode) |
| Semantic tags | none | 10 tag categories |
| Code Mode meta-tools | none | 3 (search, schema, execute) |
| READONLY annotations | 41/43 tools | 43/43 tools |

## Risk Mitigation

- **Markdown output breaks a client?** The `structured_content` field in `ToolResult` still returns JSON dicts — Markdown is only in the `content` text field. Clients using structured data are unaffected. Rollback: revert `format_text_table` (1 function, 1 commit).
- **Code Mode unstable?** It's behind `fastmcp.experimental` — remove the `CodeMode()` transform (1 line) to disable. All direct tool calls continue working.
- **Tags cause issues?** Tags are pure metadata. Removing them is a decorator-only change, no logic impact.
- **Rollback?** Each task is a separate commit. `git revert` any individual commit without affecting others.
