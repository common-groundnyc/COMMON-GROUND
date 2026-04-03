# MCP Tool Parallelization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parallelize all sequential independent queries across MCP tools — entity_xray (23 queries), safety (8), school (13), network (10+), legal (11), health (7), civic (5-7 per view). Estimated 3-15x speedup per tool.

**Architecture:** Every tool already has access to `parallel_queries(pool, [(name, sql, params), ...])` from `shared.db`. The pattern: identify independent queries, batch them into one `parallel_queries()` call, unpack results by name. Same pattern already working in `building.py` and `address_report.py`.

**Tech Stack:** Python, DuckDB, FastMCP, `concurrent.futures.ThreadPoolExecutor` (inside `parallel_queries`)

**Audit source:** Performance audit completed 2026-04-03, found all issues below.

---

## File Structure

| Action | File | What Changes |
|--------|------|-------------|
| Modify | `infra/duckdb-server/tools/entity.py` | Parallelize _entity_xray (22 queries), _due_diligence (7), _cop_sheet (5) |
| Modify | `infra/duckdb-server/tools/safety.py` | Parallelize _view_full (5 independent queries after crime MV) |
| Modify | `infra/duckdb-server/tools/school.py` | Parallelize _report (13 queries) |
| Modify | `infra/duckdb-server/tools/network.py` | Parallelize _ownership_for_bbl (8 independent after building lookup), _all_types (4 type calls) |
| Modify | `infra/duckdb-server/tools/legal.py` | Parallelize _view_full (5 sequential view calls) |
| Modify | `infra/duckdb-server/tools/health.py` | Parallelize _view_full (7 queries) |
| Modify | `infra/duckdb-server/tools/civic.py` | Replace _run_queries loop with parallel_queries |
| Modify | `infra/duckdb-server/tools/neighborhood.py` | Parallelize portrait _view_full (8 queries) |

---

### Task 1: Parallelize entity_xray (22 sequential → parallel)

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`

This is the single biggest win — 22 independent `safe_query` calls that each do `LIKE '%SEARCH%'` on different tables.

- [ ] **Step 1: Add parallel_queries import**

At the top of `entity.py`, change:
```python
from shared.db import execute, safe_query, fill_placeholders
```
to:
```python
from shared.db import execute, safe_query, fill_placeholders, parallel_queries
```

- [ ] **Step 2: Rewrite _entity_xray to use parallel_queries**

The function currently has ~22 blocks like:
```python
corp_cols, corp_rows = [], []
if _should_query("nys_corporations"):
    corp_cols, corp_rows = safe_query(pool, """...""", [params])
```

Replace ALL of these blocks (from "# 1. NYS corps" through "# notaries") with a single parallel batch. Build the query list conditionally based on `_should_query()`:

```python
    # Build query batch — only include routed sources
    queries = []

    if _should_query("nys_corporations"):
        queries.append(("corp", """
            SELECT ...existing SQL...
        """, [f"%{search}%"]))

    if _should_query("issued_licenses"):
        queries.append(("biz", """
            SELECT ...existing SQL...
        """, [existing_params]))

    # ... repeat for all 22 query blocks, keeping exact same SQL and params ...

    if _should_query("nys_notaries"):
        queries.append(("notary", """
            SELECT ...existing SQL...
        """, [f"%{search}%"] * 2))

    # Execute all in parallel
    results = parallel_queries(pool, queries)

    # Unpack results
    corp_cols, corp_rows = results.get("corp", ([], []))
    # ... same for all 22 ...
```

**IMPORTANT:** Keep the exact same SQL for each query — just move it from sequential `safe_query()` calls into the `queries` list. The `_should_query()` gate stays as a conditional append.

For queries that have special SQL variants (like `dob` which has different SQL for multi-word vs single-word names), build the SQL before appending to the list.

- [ ] **Step 3: Verify the output format is unchanged**

The unpacked variables (`corp_cols`, `corp_rows`, `biz_cols`, `biz_rows`, etc.) feed into the formatting section below. Since `parallel_queries` returns `(cols, rows)` tuples via `safe_query`, the format is identical. BUT: `parallel_queries` uses `safe_query` internally which returns `([], [])` on error — so `corp_cols` will be `[]` not the column names. Check if the formatting code uses column names. If it does, you need to unpack differently:

```python
corp_result = results.get("corp", ([], []))
corp_cols, corp_rows = corp_result[0], corp_result[1]
```

The existing formatting code at line ~847 uses `for r in corp_rows:` and accesses by index `r[0]`, `r[1]`, etc. — NOT by column name. So `([], [])` is fine.

- [ ] **Step 4: Test locally**

Run: `cd ~/Desktop/dagster-pipeline && PYTHONPATH=infra/duckdb-server uv run python3 -c "from tools.entity import _entity_xray; print('import OK')"`

This will fail on fastmcp import but confirms no syntax errors in the rewrite.

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/entity.py
git commit -m "perf: parallelize entity_xray — 22 queries run concurrently via parallel_queries"
```

---

### Task 2: Parallelize _due_diligence and _cop_sheet in entity.py

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`

- [ ] **Step 1: Read _due_diligence (around line 1236)**

It has ~7 sequential `safe_query` calls for financial/legal checks. Identify which are independent.

- [ ] **Step 2: Batch independent queries**

Replace sequential calls with `parallel_queries(pool, [...])`. Keep any queries that depend on earlier results sequential.

- [ ] **Step 3: Read _cop_sheet (around line 1390)**

It has ~5 sequential queries for police misconduct data.

- [ ] **Step 4: Batch independent queries in _cop_sheet**

Same pattern as _due_diligence.

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/tools/entity.py
git commit -m "perf: parallelize _due_diligence (7 queries) and _cop_sheet (5 queries)"
```

---

### Task 3: Parallelize safety _view_full (8 queries)

**Files:**
- Modify: `infra/duckdb-server/tools/safety.py`

- [ ] **Step 1: Add parallel_queries import**

```python
from shared.db import execute, safe_query, parallel_queries
```

- [ ] **Step 2: Identify independent queries in _view_full**

After the crime MV lookup (lines 508-535), these are independent:
- `ARRESTS_SQL` (line 537)
- `SHOOTINGS_YEARLY_SQL` (line 538)
- `SUMMONS_SQL` (line 539)
- `HATE_SQL` (line 540)
- `CITY_AVERAGES_SQL` (line 541)

Plus CCRB queries later in the function (check exact location).

- [ ] **Step 3: Batch into parallel_queries**

```python
    # After crime MV lookup...
    results = parallel_queries(pool, [
        ("arrests", ARRESTS_SQL, [pct, pct]),
        ("shootings", SHOOTINGS_YEARLY_SQL, [pct]),
        ("summons", SUMMONS_SQL, [pct]),
        ("hate", HATE_SQL, [pct]),
        ("city_avg", CITY_AVERAGES_SQL, []),
    ])

    _, arrest_rows = results.get("arrests", ([], []))
    _, shoot_rows = results.get("shootings", ([], []))
    _, summ_rows = results.get("summons", ([], []))
    _, hate_rows = results.get("hate", ([], []))
    _, avg_rows = results.get("city_avg", ([], []))
```

Also check for CCRB, UOF, and crashes queries later in the function — add those to the batch too.

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/tools/safety.py
git commit -m "perf: parallelize safety _view_full — 8 queries run concurrently"
```

---

### Task 4: Parallelize school _report (13 queries)

**Files:**
- Modify: `infra/duckdb-server/tools/school.py`

- [ ] **Step 1: Add parallel_queries import**

- [ ] **Step 2: Read _report function (around line 393)**

It has ~13 sequential queries: directory, performance, ELA, math, quality, demographics, attendance, class_size, survey, safety, regents, cafeteria, specialized. ALL are independent — they each filter by DBN.

- [ ] **Step 3: Batch all 13 into parallel_queries**

Build the queries list with all 13 SQL constants and `[dbn]` params. Unpack by name.

- [ ] **Step 4: Verify formatting still works**

The formatting code accesses results by column name via `dict(zip(cols, row))`. Make sure `cols` is correctly unpacked from each result.

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/tools/school.py
git commit -m "perf: parallelize school _report — 13 queries run concurrently"
```

---

### Task 5: Parallelize network _ownership_for_bbl and _all_types

**Files:**
- Modify: `infra/duckdb-server/tools/network.py`

- [ ] **Step 1: Add parallel_queries import**

- [ ] **Step 2: Read _ownership_for_bbl (around line 382)**

After the initial building lookup (gets registrationid), the remaining queries are independent: portfolio, violations, complaints, evictions, DOB, AEP, CONH, underlying, city_averages. Batch them.

- [ ] **Step 3: Read _all_types (around line 291)**

It calls ownership, corporate, political, property lookups sequentially. These are independent — run them in parallel using Python's ThreadPoolExecutor directly (they're function calls, not SQL queries, so `parallel_queries` doesn't apply).

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=4) as ex:
    futures = {
        ex.submit(_ownership_lookup, ...): "ownership",
        ex.submit(_corporate_lookup, ...): "corporate",
        ex.submit(_political_lookup, ...): "political",
        ex.submit(_property_lookup, ...): "property",
    }
    results = {}
    for fut in as_completed(futures):
        results[futures[fut]] = fut.result()
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/tools/network.py
git commit -m "perf: parallelize network _ownership_for_bbl (8 queries) and _all_types (4 lookups)"
```

---

### Task 6: Parallelize legal, health, civic, neighborhood

**Files:**
- Modify: `infra/duckdb-server/tools/legal.py`
- Modify: `infra/duckdb-server/tools/health.py`
- Modify: `infra/duckdb-server/tools/civic.py`
- Modify: `infra/duckdb-server/tools/neighborhood.py`

These are all the same mechanical pattern. For each file:

- [ ] **Step 1: Add parallel_queries import**

- [ ] **Step 2: legal.py — _view_full has 5 sequential view function calls**

Read the function, identify the independent calls, batch them. If they're function calls (not raw SQL), use ThreadPoolExecutor directly.

- [ ] **Step 3: health.py — _view_full has 7 queries**

CDC, HIV, deaths, flu, SPARCS, CHS, pregnancy are all independent. Batch into parallel_queries.

- [ ] **Step 4: civic.py — _run_queries is a sequential for-loop**

Replace the for-loop with parallel_queries. The function takes a list of `(label, sql, limit)` tuples — transform to `(label, sql, [params])` format.

- [ ] **Step 5: neighborhood.py — portrait _view_full has ~8 queries**

Read the function, identify independent queries, batch them.

- [ ] **Step 6: Commit all four**

```bash
git add infra/duckdb-server/tools/legal.py infra/duckdb-server/tools/health.py \
        infra/duckdb-server/tools/civic.py infra/duckdb-server/tools/neighborhood.py
git commit -m "perf: parallelize legal (5), health (7), civic (5-7), neighborhood (8) queries"
```

---

### Task 7: Deploy and benchmark

**Files:** None (operational)

- [ ] **Step 1: Deploy all changes to Hetzner**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
  --exclude 'model/' --exclude 'tests/' \
  infra/duckdb-server/ \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server && docker compose up -d duckdb-server"
```

- [ ] **Step 2: Wait for startup (~90s)**

```bash
sleep 90 && ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker compose -f /opt/common-ground/docker-compose.yml logs duckdb-server --tail 5"
```

Verify: `Uvicorn running on http://0.0.0.0:4213`

- [ ] **Step 3: Benchmark all parallelized tools**

Test each tool via curl to `https://mcp.common-ground.nyc/mcp`:
- `entity(name="BLACKSTONE")` — was ~30s+ with 22 sequential queries
- `safety(location="14")` — was ~15s+ with 8 sequential queries
- `school(query="brooklyn tech")` → get DBN → `school(query="DBN", view="report")` — was ~20s+
- `network(query="KUSHNER")` — was ~20s+
- `building(identifier="1000670001")` — baseline, should still be ~4s

- [ ] **Step 4: Check server logs for per-query timing**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "docker compose -f /opt/common-ground/docker-compose.yml logs duckdb-server 2>&1 | grep '\[parallel\]'"
```

- [ ] **Step 5: Report results**

Record before/after times for each tool. Expected improvements:
| Tool | Before (est.) | After (est.) |
|------|---------------|-------------|
| entity_xray | 30-60s | 5-10s |
| safety | 15-20s | 3-5s |
| school report | 20-30s | 3-5s |
| network ownership | 20-30s | 5-10s |
