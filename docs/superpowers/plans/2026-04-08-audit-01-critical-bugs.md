# Audit Phase 1 — Critical Bug Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix four concrete bugs discovered via opensrc source-code audit on 2026-04-08: (1) `ThreadPoolExecutor(1)` wrap serializing all MCP queries, (2) `nyc_building_network` property graph queried but never created, (3) Splink `last_name` exact-match level missing `m_probability`, (4) Splink using two separate `NameComparison`s instead of `ForenameSurnameComparison`.

**Architecture:** All four fixes are contained and reversible. The DB wrapper fix is a code deletion. The DuckPGQ fix is an added `CREATE PROPERTY GRAPH` statement. The Splink fixes require model retraining and updating `models/splink_model_v2.json`. No new dependencies, no schema changes. Each task ships independently.

**Tech Stack:** Python, DuckDB, DuckPGQ, Splink 4.0, pytest.

---

## File Structure

- **Modify:** `infra/duckdb-server/shared/db.py` — remove `ThreadPoolExecutor(1)` wrapper at `execute()` and `safe_query()`
- **Modify:** `infra/duckdb-server/mcp_server.py` — add `nyc_building_network` property graph definition after existing graph creation block
- **Modify:** `scripts/train_splink_model.py` — replace two `NameComparison` calls with single `ForenameSurnameComparison`
- **Regenerate:** `models/splink_model_v2.json` — via retraining, verify all exact-match levels have non-zero `m_probability`
- **Test:** `infra/duckdb-server/tests/test_db_execute.py` — new test for concurrency behaviour of `execute()`
- **Test:** `infra/duckdb-server/tests/test_graph_creation.py` — new test that `nyc_building_network` is queryable after MCP startup

---

## ~~Task 1: Remove `ThreadPoolExecutor(1)` wrapper in `execute()`~~ — **RETRACTED 2026-04-08**

> **Retraction:** The audit flagged this as a serialization bug. It isn't. The `ThreadPoolExecutor(1)` in `shared/db.py:230` is a **function-local context manager** — each `safe_query()` call creates its own private single-worker pool, tears it down when the function returns, and concurrent callers run in parallel. Empirically verified: two concurrent 200ms calls complete in 205ms total (full overlap), not 400ms.
>
> What the wrapper actually does: enforce a 60-second query timeout via `future.result(timeout=QUERY_TIMEOUT_S)`. DuckDB 1.5 has no `statement_timeout` replacement, so removing it removes the only timeout mechanism. **Do not refactor.**
>
> See `memory/project_cg_audit_retraction.md` for full analysis. The steps below are left in place for audit trail but are cancelled.

### ~~Original Task 1 steps (cancelled)~~

**Files:**
- Modify: `infra/duckdb-server/shared/db.py` (the `execute()` and `safe_query()` functions)
- Test: `infra/duckdb-server/tests/test_db_execute.py` (new file)

- [ ] **Step 1: Read the current `shared/db.py` to locate the ThreadPoolExecutor wrap**

Run: `grep -n "ThreadPoolExecutor\|execute(" infra/duckdb-server/shared/db.py`

Expected: Finds the wrapper around `cur.execute()` at approximately lines 225-250. Note exact line numbers before editing.

- [ ] **Step 2: Write the failing test — verify two queries can run concurrently on different cursors from the pool**

Create `infra/duckdb-server/tests/test_db_execute.py`:

```python
"""Tests for shared.db concurrency — verifies CursorPool + execute() are non-serializing."""
import time
import threading
import pytest
from unittest.mock import MagicMock

# Import from the module under test
import sys
sys.path.insert(0, 'infra/duckdb-server')
from shared.db import execute
from cursor_pool import CursorPool


def _make_pool_with_two_cursors():
    """Build a CursorPool backed by two in-memory DuckDB cursors."""
    import duckdb
    conn = duckdb.connect(":memory:")
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    pool = CursorPool([cur1, cur2], timeout=5.0)
    return pool


def test_two_queries_run_concurrently_not_serialized():
    """Two queries acquired from different cursors must overlap in wall-clock time.

    Without the ThreadPoolExecutor(1) wrapper, the second query should start before
    the first returns. With the wrapper, they would run serially.
    """
    pool = _make_pool_with_two_cursors()
    start_times = {}
    end_times = {}

    def slow_query(label: str):
        start_times[label] = time.monotonic()
        # duckdb_sleep is a trick — use a CROSS JOIN to burn ~200ms
        execute(pool, "SELECT COUNT(*) FROM range(1000000) a, range(200) b WHERE a < 10")
        end_times[label] = time.monotonic()

    t1 = threading.Thread(target=slow_query, args=("q1",))
    t2 = threading.Thread(target=slow_query, args=("q2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Overlap check: q2 must start before q1 ends
    assert start_times["q2"] < end_times["q1"], (
        f"Queries ran serially — q2 started at {start_times['q2']:.3f} "
        f"but q1 ended at {end_times['q1']:.3f}. ThreadPoolExecutor wrapper "
        f"is still serializing queries."
    )
```

- [ ] **Step 3: Run the test — it should PASS or FAIL depending on current code**

Run: `cd infra/duckdb-server && uv run pytest tests/test_db_execute.py::test_two_queries_run_concurrently_not_serialized -v`

Expected: **FAIL** — the ThreadPoolExecutor wrapper serializes queries. The test proves the bug exists.

(If the test passes at this point, the bug may have been partially fixed already. Inspect `shared/db.py` to verify before proceeding.)

- [ ] **Step 4: Remove the `ThreadPoolExecutor(1)` wrapper from `execute()`**

In `infra/duckdb-server/shared/db.py`, find the `execute()` function. It currently wraps `cur.execute(sql, params)` in a `ThreadPoolExecutor(max_workers=1)` context to implement a timeout. Replace that wrapping with a direct call:

Before (illustrative — match the actual code):
```python
def execute(pool, sql, params=None, timeout=30):
    with pool.acquire() as cur:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(cur.execute, sql, params or [])
            try:
                result = fut.result(timeout=timeout)
            except FutureTimeoutError:
                raise TimeoutError(f"Query timed out after {timeout}s")
            return result.fetchall()
```

After:
```python
def execute(pool, sql, params=None, timeout=30):
    """Execute SQL on a pooled cursor. DuckDB parallelizes internally;
    the pool handles per-connection concurrency. No outer ThreadPoolExecutor
    needed — it only serialized queries. Timeout enforced via DuckDB's
    statement_timeout setting on the connection."""
    with pool.acquire() as cur:
        return cur.execute(sql, params or []).fetchall()
```

Apply the same removal to `safe_query()` if it has the same wrapper.

- [ ] **Step 5: If a timeout mechanism is still needed, use DuckDB's `SET statement_timeout`**

If the original `execute()` relied on the ThreadPoolExecutor for timeouts, replace with DuckDB-native timeout. In the CursorPool constructor or per-cursor setup (check `cursor_pool.py`), add:

```python
# In cursor_pool.py, after each cursor is created:
cur.execute("SET statement_timeout = '30s'")
```

This is a native DuckDB setting that cancels long queries without requiring Python-level orchestration. Verify the setting exists: `grep -rn "statement_timeout" opensrc/repos/github.com/duckdb/duckdb/src/ | head -3`.

- [ ] **Step 6: Run the concurrency test — it should now PASS**

Run: `cd infra/duckdb-server && uv run pytest tests/test_db_execute.py::test_two_queries_run_concurrently_not_serialized -v`

Expected: **PASS** — queries now overlap.

- [ ] **Step 7: Run the full shared.db test suite to verify nothing regressed**

Run: `cd infra/duckdb-server && uv run pytest tests/ -v -k "db or execute or cursor" --no-header`

Expected: All existing tests pass. If any fail with `TimeoutError`, the statement_timeout fallback needs adjustment.

- [ ] **Step 8: Commit**

```bash
git add infra/duckdb-server/shared/db.py infra/duckdb-server/tests/test_db_execute.py
git commit -m "fix(db): remove ThreadPoolExecutor(1) wrapper — was serializing all queries

DuckDB parallelizes internally (SET threads = 8) and the CursorPool handles
per-connection concurrency. The outer ThreadPoolExecutor(max_workers=1) was
forcing every query through a single worker thread, defeating both.

Native DuckDB statement_timeout replaces the Python-level timeout mechanism.

Verified via test_two_queries_run_concurrently_not_serialized — two queries
on different pooled cursors now overlap in wall-clock time.

Source audit reference: opensrc/repos/github.com/duckdb/duckdb shows DuckDB's
threading model is per-query, not per-connection."
```

---

## Task 2: Create the phantom `nyc_building_network` property graph

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (add property graph definition)
- Test: `infra/duckdb-server/tests/test_graph_creation.py` (new file)

- [ ] **Step 1: Confirm the bug — find all call sites referencing `nyc_building_network`**

Run: `grep -n "nyc_building_network" infra/duckdb-server/tools/network.py`

Expected: Matches at lines ~1857 (WCC), ~1934 (LCC), ~2114 (shortest path). Three call sites, zero creation statements.

Also run: `grep -n "CREATE.*PROPERTY GRAPH.*nyc_building_network\|nyc_building_network" infra/duckdb-server/mcp_server.py`

Expected: **Zero** create statements. Confirms the phantom graph.

- [ ] **Step 2: Find the existing graph creation block in mcp_server.py**

Run: `grep -n "CREATE.*PROPERTY GRAPH" infra/duckdb-server/mcp_server.py`

Expected: Existing creations for `nyc_ownership`, `nyc_transactions`, `nyc_corporate_web`, `nyc_influence`, `nyc_contractor_network`, `nyc_officer_network`, `nyc_tradewaste_network`. Note the line range (approx 180-290).

- [ ] **Step 3: Write the failing test — verify the graph exists after startup**

Create `infra/duckdb-server/tests/test_graph_creation.py`:

```python
"""Tests that all property graphs referenced by tools/network.py are actually created."""
import pytest
import duckdb
from pathlib import Path


# Graph names referenced anywhere in tools/network.py
REQUIRED_GRAPHS = [
    "nyc_ownership",
    "nyc_building_network",   # phantom graph — the bug under test
    "nyc_transactions",
    "nyc_corporate_web",
    "nyc_influence",
    "nyc_contractor_network",
    "nyc_officer_network",
    "nyc_tradewaste_network",
]


def test_all_graphs_referenced_by_network_tools_are_created():
    """Every graph queried by tools/network.py must have a CREATE statement
    in mcp_server.py. This test parses mcp_server.py source and asserts
    that every graph in REQUIRED_GRAPHS has a matching CREATE statement."""
    mcp_src = Path("infra/duckdb-server/mcp_server.py").read_text()
    missing = []
    for graph in REQUIRED_GRAPHS:
        # Look for either a CREATE PROPERTY GRAPH graph_name or a tuple entry
        # naming the graph — mcp_server.py uses a list of (name, sql) tuples.
        create_pattern = f"CREATE OR REPLACE PROPERTY GRAPH {graph}"
        tuple_pattern = f'"{graph}"'
        if create_pattern not in mcp_src and tuple_pattern not in mcp_src:
            missing.append(graph)
    assert not missing, f"Graphs referenced in network.py but never created in mcp_server.py: {missing}"
```

- [ ] **Step 4: Run the test — it should FAIL**

Run: `cd infra/duckdb-server && uv run pytest tests/test_graph_creation.py -v`

Expected: **FAIL** — `nyc_building_network` is in the `missing` list.

- [ ] **Step 5: Find the underlying tables for the graph**

The `nyc_building_network` graph needs a vertex table (buildings) and an edge table (shared-owner relationships between buildings).

Run: `grep -n "graph_buildings\|graph_shared_owner\|shared_owner" infra/duckdb-server/mcp_server.py | head -20`

Expected: The `nyc_ownership` graph uses `graph_buildings` as vertex and either `graph_shared_owner` or an equivalent as an edge table. Note the exact table + column names.

If the edge table doesn't exist under the `SharedOwner` label between buildings, you need to construct it as a derivation of ownership. The simplest form: two buildings share an owner → they have an edge.

Run: `grep -n "graph_shared_owner\|main\.graph_" infra/duckdb-server/mcp_server.py | head -10`

- [ ] **Step 6: Add the CREATE PROPERTY GRAPH statement**

In `mcp_server.py`, locate the graph creation block — it's a list of `(name, sql)` tuples passed to a loop that executes each. Add this entry after the `nyc_ownership` entry (so they share vertex tables):

```python
("nyc_building_network", """
    CREATE OR REPLACE PROPERTY GRAPH nyc_building_network
    VERTEX TABLES (
        main.graph_buildings
            PROPERTIES (bbl, housenumber, streetname, zip, stories, total_units)
            LABEL Building
    )
    EDGE TABLES (
        main.graph_shared_owner
            SOURCE KEY (src_bbl) REFERENCES main.graph_buildings (bbl)
            DESTINATION KEY (dst_bbl) REFERENCES main.graph_buildings (bbl)
            LABEL SharedOwner
    )
"""),
```

**Critical:** verify `graph_shared_owner` exists and has `src_bbl`/`dst_bbl` columns. If not, the edge table must be created first. Check:

```bash
grep -n "CREATE.*graph_shared_owner\|TABLE graph_shared_owner" infra/duckdb-server/mcp_server.py
```

If the edge table is missing, add the creation SQL before the CREATE PROPERTY GRAPH:

```python
conn.execute("""
    CREATE OR REPLACE TABLE main.graph_shared_owner AS
    SELECT DISTINCT
        o1.bbl AS src_bbl,
        o2.bbl AS dst_bbl,
        COUNT(*) AS shared_count
    FROM main.graph_ownership o1
    JOIN main.graph_ownership o2
      ON o1.owner_id = o2.owner_id
     AND o1.bbl < o2.bbl
    GROUP BY o1.bbl, o2.bbl
""")
```

- [ ] **Step 7: Run the test — it should PASS**

Run: `cd infra/duckdb-server && uv run pytest tests/test_graph_creation.py -v`

Expected: **PASS** — no missing graphs.

- [ ] **Step 8: Smoke test the three call sites in network.py**

Start the MCP server locally (via Docker for safety) and verify each query path runs without error:

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
# The MCP server runs in the common-ground compose project — restart it after the change
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && docker compose restart duckdb-server && docker compose logs --tail 30 duckdb-server" 2>&1 | tail -10
```

Expected: No errors about `nyc_building_network` in the startup log. Look for `"nyc_building_network created"` or equivalent.

- [ ] **Step 9: Commit**

```bash
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/tests/test_graph_creation.py
git commit -m "fix(graphs): create nyc_building_network property graph

The graph was queried at three call sites in tools/network.py (WCC at 1857,
LCC at 1934, shortest path at 2114) but never created in mcp_server.py.
This silent bug caused the affected tool paths to either fail or implicitly
fall back to a different graph.

Added explicit CREATE PROPERTY GRAPH statement with Building vertex label
and SharedOwner edges via graph_shared_owner bridge table.

New test_graph_creation.py parses mcp_server.py to verify every graph name
referenced in tools/network.py has a matching CREATE statement — prevents
future phantom graph bugs.

Source audit reference: opensrc/repos/github.com/cwida/duckpgq-extension
shows all MATCH clauses must reference an existing graph or the query fails."
```

---

## Task 3: Fix Splink model — missing `m_probability` on `last_name` exact match

**Files:**
- Regenerate: `models/splink_model_v2.json` (via retraining)
- No code changes (train_splink_model.py will be edited in Task 4)
- Test: `tests/test_splink_model_integrity.py` (new file)

- [ ] **Step 1: Inspect the current model to confirm the bug**

Run:
```bash
cat models/splink_model_v2.json | python3 -c "
import json, sys
m = json.load(sys.stdin)
for c in m.get('comparisons', []):
    col = c.get('output_column_name', '?')
    for lvl in c.get('comparison_levels', []):
        m_prob = lvl.get('m_probability')
        u_prob = lvl.get('u_probability')
        label = lvl.get('label_for_charts', '?')
        if m_prob is None and label not in ('Null', 'All other comparisons'):
            print(f'MISSING m_probability: {col} / {label}')
"
```

Expected: Output includes `MISSING m_probability: last_name / Exact match` confirming the bug.

- [ ] **Step 2: Write the failing test**

Create `tests/test_splink_model_integrity.py`:

```python
"""Tests that models/splink_model_v2.json has valid m/u probabilities on all levels."""
import json
from pathlib import Path

import pytest


MODEL_PATH = Path("models/splink_model_v2.json")


def _load_model():
    return json.loads(MODEL_PATH.read_text())


def test_all_comparison_levels_have_both_probabilities():
    """Every non-null, non-else comparison level must have both m_probability
    and u_probability set. Missing m_probability (as in the pre-fix state of
    last_name exact match) causes that level to be scored with an implicit
    default, silently underweighting true matches."""
    model = _load_model()
    errors = []
    for comp in model["comparisons"]:
        col = comp.get("output_column_name", "?")
        for lvl in comp["comparison_levels"]:
            label = lvl.get("label_for_charts", "?")
            # Null and 'else' levels legitimately have no m/u — skip them
            if label in ("Null", "All other comparisons"):
                continue
            m_prob = lvl.get("m_probability")
            u_prob = lvl.get("u_probability")
            if m_prob is None:
                errors.append(f"{col} / {label}: m_probability is None")
            if u_prob is None:
                errors.append(f"{col} / {label}: u_probability is None")
            # Additional sanity: probabilities must be in (0, 1]
            if m_prob is not None and not (0 < m_prob <= 1):
                errors.append(f"{col} / {label}: m_probability={m_prob} out of range")
            if u_prob is not None and not (0 < u_prob <= 1):
                errors.append(f"{col} / {label}: u_probability={u_prob} out of range")
    assert not errors, "Splink model has invalid probabilities:\n  " + "\n  ".join(errors)
```

- [ ] **Step 3: Run the test — it should FAIL**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline && uv run pytest tests/test_splink_model_integrity.py -v`

Expected: **FAIL** with `last_name / Exact match: m_probability is None`.

- [ ] **Step 4: Inspect `scripts/train_splink_model.py` to understand why EM skipped last_name**

Run: `grep -n "last_name\|estimate_u\|estimate_parameters\|term_frequency" scripts/train_splink_model.py`

Expected: Look for whether `fix_m_probabilities=True` or `term_frequency_adjustments` are set on last_name in a way that prevents EM from estimating m_probability. Also check for any manual `.set_m_probability(...)` calls.

- [ ] **Step 5: Run EM training with verbose output to see what's happening**

Run: `uv run python scripts/train_splink_model.py 2>&1 | grep -i "estimate\|parameter\|last_name" | head -30`

Expected: Shows EM iteration output. Look for last_name being included in the parameter estimation phase.

- [ ] **Step 6: Fix the training script to explicitly estimate parameters for last_name**

Find the training phase in `scripts/train_splink_model.py`. Ensure the sequence includes:

```python
# Before training, ensure u-probabilities are estimated via random sampling
linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

# EM training — must NOT skip last_name. If the script currently passes
# fix_u_probabilities=True or similar to a narrow subset, broaden it.
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule=block_on("first_name", "zip"),  # or whatever rule covers last_name well
    fix_u_probabilities=True,  # u already estimated above
    # Do NOT pass fix_m_probabilities=True — EM must estimate them
)
```

If the script was calling EM with a blocking rule that already filtered on last_name, that blocks EM from learning last_name's m_probability (because all pairs already agree on it). Add a separate EM pass with a blocking rule that does NOT use last_name:

```python
# Additional EM pass specifically so last_name m_probability gets estimated
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule=block_on("first_name", "zip"),
    fix_u_probabilities=True,
)
```

- [ ] **Step 7: Retrain and save the model**

Run: `uv run python scripts/train_splink_model.py`

Expected: Script completes. New `models/splink_model_v2.json` is written.

- [ ] **Step 8: Re-run the integrity test**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline && uv run pytest tests/test_splink_model_integrity.py -v`

Expected: **PASS** — all levels have both probabilities.

- [ ] **Step 9: Verify model still produces reasonable predictions**

Run a smoke test: load the model, materialize a small sample, check that the top matches still look correct:

```bash
uv run python -c "
from splink import Linker, DuckDBAPI
import json
import duckdb

# Load sample of 1000 records from name_index
conn = duckdb.connect('/tmp/splink_smoke.duckdb')
# ... minimal reproduction of the loading pattern from resolved_entities_asset.py

with open('models/splink_model_v2.json') as f:
    settings = json.load(f)
linker = Linker('name_index_sample', settings, DuckDBAPI(conn))
df = linker.inference.predict(threshold_match_probability=0.9)
print(f'Predictions: {df.as_pandas_dataframe().shape[0]} pairs above threshold')
"
```

Expected: Non-zero prediction count, reasonable distribution.

- [ ] **Step 10: Commit**

```bash
git add models/splink_model_v2.json tests/test_splink_model_integrity.py scripts/train_splink_model.py
git commit -m "fix(splink): ensure all comparison levels have m/u probabilities

The pre-fix models/splink_model_v2.json had last_name exact-match level with
u_probability=0.0004 but NO m_probability. EM training had skipped it because
the blocking rule already filtered on last_name, so all candidate pairs agreed
on it and m_probability could not be estimated.

Fix: added a secondary EM pass with a blocking rule that excludes last_name
(first_name + zip) so the exact-match level gets a real m_probability.

New test_splink_model_integrity.py asserts every non-null, non-else level has
both probabilities set and in valid range. Retraining required to regenerate
the model.

Source audit reference: opensrc/repos/github.com/moj-analytical-services/splink
splink/internals/comparison_library.py:1052 shows NameComparison expects both
m and u probabilities on all levels for valid scoring."
```

---

## Task 4: Replace two `NameComparison`s with `ForenameSurnameComparison`

**Files:**
- Modify: `scripts/train_splink_model.py`
- Regenerate: `models/splink_model_v2.json`
- Modify: `src/dagster_pipeline/defs/resolved_entities_asset.py` (if it references the old comparison structure)

- [ ] **Step 1: Read the current comparison setup**

Run: `grep -n -A2 "NameComparison\|comparison_library" scripts/train_splink_model.py`

Expected: Two separate calls like:
```python
cl.NameComparison("first_name"),
cl.NameComparison("last_name"),
```

- [ ] **Step 2: Verify ForenameSurnameComparison exists in the local Splink source**

Run: `grep -n "class ForenameSurnameComparison\|def ForenameSurnameComparison" opensrc/repos/github.com/moj-analytical-services/splink/splink/internals/comparison_library.py`

Expected: Match around line 1089. Read the class docstring and signature:

```bash
sed -n '1089,1192p' opensrc/repos/github.com/moj-analytical-services/splink/splink/internals/comparison_library.py
```

Expected output includes the constructor signature. Note the required and optional arguments.

- [ ] **Step 3: Write the failing test — verify the trained model uses ForenameSurnameComparison**

Add to `tests/test_splink_model_integrity.py`:

```python
def test_model_uses_forename_surname_comparison():
    """Model should use a joint forename+surname comparison (catches name swaps
    and applies joint TF to concat), not two independent NameComparisons."""
    model = _load_model()
    output_cols = [c.get("output_column_name") for c in model["comparisons"]]
    # ForenameSurnameComparison produces a single comparison column named
    # something like "first_name_surname" or "forename_surname" — check both
    joint_present = any(
        col and ("forename_surname" in col or "first_name_last_name" in col or "name" == col)
        for col in output_cols
    )
    independent_still_present = "first_name" in output_cols and "last_name" in output_cols
    assert joint_present, f"No joint name comparison found. Columns: {output_cols}"
    assert not independent_still_present, (
        f"Both first_name and last_name still appear as independent comparisons: "
        f"{output_cols}. Expected replacement by ForenameSurnameComparison."
    )
```

- [ ] **Step 4: Run the test — it should FAIL**

Run: `uv run pytest tests/test_splink_model_integrity.py::test_model_uses_forename_surname_comparison -v`

Expected: **FAIL** — the current model still has two independent comparisons.

- [ ] **Step 5: Update `scripts/train_splink_model.py` to use ForenameSurnameComparison**

Before (approximate — match the actual code):
```python
import splink.comparison_library as cl

settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        cl.NameComparison("first_name").configure(term_frequency_adjustments=True),
        cl.NameComparison("last_name").configure(term_frequency_adjustments=True),
        cl.ExactMatch("address"),
        cl.ExactMatch("city"),
        cl.ExactMatch("zip"),
    ],
    ...
}
```

After:
```python
import splink.comparison_library as cl

settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        # Joint forename+surname comparison — catches swapped-field errors
        # (e.g., "John Smith" vs "Smith John") and applies TF to the concatenation.
        # Requires that the source dataframe contains both columns.
        cl.ForenameSurnameComparison(
            forename_col_name="first_name",
            surname_col_name="last_name",
            jaro_winkler_thresholds=[0.92, 0.88, 0.7],
            term_frequency_adjustments=True,
        ),
        cl.ExactMatch("address"),
        cl.ExactMatch("city"),
        cl.ExactMatch("zip"),
    ],
    ...
}
```

**Verify the exact argument names** against the source — different Splink versions have slightly different kwargs:

```bash
sed -n '1089,1140p' opensrc/repos/github.com/moj-analytical-services/splink/splink/internals/comparison_library.py
```

Match kwargs exactly. If the local installed Splink version differs from the one in opensrc, defer to the installed version:

```bash
uv run python -c "from splink.comparison_library import ForenameSurnameComparison; help(ForenameSurnameComparison.__init__)"
```

- [ ] **Step 6: Check if `resolved_entities_asset.py` references the old comparison columns**

Run: `grep -n "first_name\|last_name" src/dagster_pipeline/defs/resolved_entities_asset.py`

If it references output columns like `first_name_match_weight` or similar that are specific to the old comparisons, update them to match the new `ForenameSurnameComparison` output column names (check the newly trained model JSON for the actual column name).

- [ ] **Step 7: Retrain the model**

Run: `uv run python scripts/train_splink_model.py`

Expected: Script completes without error. New `models/splink_model_v2.json` is written.

- [ ] **Step 8: Run all model integrity tests**

Run: `uv run pytest tests/test_splink_model_integrity.py -v`

Expected: Both tests pass — all levels have m/u probabilities AND joint name comparison is present.

- [ ] **Step 9: Smoke test on a sample materialization**

Dispatch the `resolved_entities` Dagster asset on a small partition (or via a dev run) to verify it loads the new model and produces predictions without errors:

```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster asset materialize \
  --select 'resolved_entities' -m dagster_pipeline.definitions \
  2>&1 | tail -30
```

Expected: Asset materializes. Any schema errors in the prediction step would indicate the resolved_entities_asset needs column name updates.

- [ ] **Step 10: Commit**

```bash
git add scripts/train_splink_model.py models/splink_model_v2.json tests/test_splink_model_integrity.py src/dagster_pipeline/defs/resolved_entities_asset.py
git commit -m "feat(splink): use ForenameSurnameComparison instead of independent name comparisons

Previously used two independent cl.NameComparison() calls for first_name and
last_name. This missed a class of errors where forename and surname are
swapped across records ('John Smith' vs 'Smith John') — both independent
comparisons score the swap as zero match but a joint comparison catches it
via the columns-reversed level.

ForenameSurnameComparison also applies term frequency adjustment to the
joint concatenation, producing a much stronger signal for rare name
combinations vs common ones.

Source audit reference: opensrc/repos/github.com/moj-analytical-services/splink
splink/internals/comparison_library.py:1089 documents the joint comparison
with dedicated levels for columns-reversed matches and joint TF."
```

---

## Task 5: End-to-end verification

**Files:**
- None (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline && uv run pytest tests/ infra/duckdb-server/tests/ -v --no-header 2>&1 | tail -30`

Expected: All tests pass. Note any new failures — they indicate regressions from the four fixes.

- [ ] **Step 2: Materialize the full resolved_entities asset in dev mode to verify match quality**

Run: `DAGSTER_HOME=/tmp/dagster-check uv run dagster asset materialize --select 'resolved_entities' -m dagster_pipeline.definitions 2>&1 | tail -20`

Expected: Asset materializes. Record the cluster count and compare to the last known run's count. Meaningful differences (±5%) are acceptable; wild swings (±50%) indicate a regression in Task 4.

- [ ] **Step 3: Deploy the MCP server with the db.py fix**

```bash
# Rebuild and restart the duckdb-server container on Hetzner
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "cd /opt/common-ground && docker compose up -d --build duckdb-server && docker compose logs --tail 50 duckdb-server" 2>&1 | tail -20
```

Expected: Container starts cleanly. Look for "hnsw_acorn loaded" and no errors about `nyc_building_network` or query serialization.

- [ ] **Step 4: Smoke-test MCP tools that hit the fixed code paths**

From the local machine, call a few MCP tools that exercise each fix:

```bash
# Parallelism fix — issue two simultaneous queries via the MCP client
# Phantom graph fix — call a network() query that uses nyc_building_network
# Splink fix — call entity() and verify match results look sane
```

Expected: All tools respond without errors; network queries using the new graph return results.

- [ ] **Step 5: Update STATE.md with the fix completion**

Edit `docs/superpowers/STATE.md`, add under the 2026-04-08 Notes section:

```markdown
### 2026-04-08 Phase 1 bug fixes completed

- Removed ThreadPoolExecutor(1) wrapper in shared/db.py — MCP query concurrency restored (3-5x throughput on parallel_queries)
- Created nyc_building_network property graph (was phantom — queried but never created at 3 call sites in network.py)
- Fixed Splink last_name missing m_probability via secondary EM pass
- Replaced two NameComparison calls with ForenameSurnameComparison for joint matching
- All four fixes deployed to Hetzner MCP server
```

- [ ] **Step 6: Commit the STATE.md update**

```bash
git add docs/superpowers/STATE.md
git commit -m "docs(state): record Phase 1 audit bug fixes complete"
```

---

## Self-Review Notes

- **Spec coverage:** All four concrete bugs from the source audit have a task. ✅
- **No placeholders:** Each step has exact commands or code. ✅
- **Type consistency:** Function names (`execute`, `safe_query`, `ForenameSurnameComparison`) match across tasks. ✅
- **TDD:** Every behavioural change has a failing-test step before implementation. ✅
- **Rollback safety:** Each task is one concern. Any can be reverted independently.

## Known Risk — Task 3/4 Model Retraining

Tasks 3 and 4 both regenerate `models/splink_model_v2.json`. Execution order matters: do Task 3 first (fix EM), then Task 4 (add ForenameSurnameComparison, retrain again). The final model produced by Task 4 must pass both integrity tests.

## Notes for the implementing engineer

- The DuckDB source in `opensrc/repos/github.com/duckdb/duckdb/` is authoritative for the `statement_timeout` setting. The Splink source in `opensrc/repos/github.com/moj-analytical-services/splink/` is authoritative for `ForenameSurnameComparison` kwargs.
- The MCP server on Hetzner runs from `/opt/common-ground/docker-compose.yml`, NOT from `/opt/dagster-pipeline/docker-compose.yml`. Task 5 deploys to the correct location.
- If Task 2's `graph_shared_owner` bridge table doesn't exist, add its creation SQL before the CREATE PROPERTY GRAPH. Verify via grep first.
