# Cursor Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace global `_db_lock` with a DuckDB cursor pool so MCP tools can execute queries concurrently, targeting 50 concurrent with <5% errors.

**Architecture:** A `CursorPool` class manages 8 pre-created DuckDB cursors behind a semaphore. Tools acquire a cursor, run their query, release it. No external locking needed — DuckDB MVCC handles read isolation. The `_execute()` function signature changes from `(conn, sql, params)` to `(pool, sql, params)`.

**Tech Stack:** Python 3.12, DuckDB 1.5 cursors, threading.Semaphore, queue.Queue

**Spec:** `docs/superpowers/specs/2026-03-27-cursor-pool-design.md`

**Working directory:** `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server`

---

### Task 1: Create `cursor_pool.py` with tests

**Files:**
- Create: `cursor_pool.py`
- Create: `tests/test_cursor_pool.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_cursor_pool.py
import threading
import time
import duckdb
import pytest
from cursor_pool import CursorPool


@pytest.fixture
def pool():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_t (id INTEGER, val VARCHAR)")
    conn.execute("INSERT INTO test_t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    p = CursorPool(conn, size=3)
    yield p
    conn.close()


def test_cursor_context_manager(pool):
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3


def test_execute_convenience(pool):
    cols, rows = pool.execute("SELECT * FROM test_t ORDER BY id")
    assert cols == ["id", "val"]
    assert len(rows) == 3
    assert rows[0] == (1, "a")


def test_execute_with_params(pool):
    cols, rows = pool.execute("SELECT * FROM test_t WHERE id = ?", [2])
    assert len(rows) == 1
    assert rows[0] == (2, "b")


def test_concurrent_reads(pool):
    """8 threads reading simultaneously — no errors."""
    results = []
    errors = []

    def reader():
        try:
            with pool.cursor() as cur:
                time.sleep(0.05)  # hold cursor briefly
                r = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
                results.append(r[0])
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors: {errors}"
    assert all(r == 3 for r in results)


def test_pool_exhaustion_queues(pool):
    """When all 3 cursors are busy, 4th request waits (doesn't error)."""
    barrier = threading.Barrier(3)
    acquired = []
    errors = []

    def holder():
        try:
            with pool.cursor() as cur:
                acquired.append(threading.current_thread().name)
                barrier.wait(timeout=2)
                time.sleep(0.1)
        except Exception as e:
            errors.append(str(e))

    def waiter():
        time.sleep(0.05)  # let holders acquire first
        t0 = time.monotonic()
        try:
            with pool.cursor() as cur:
                elapsed = time.monotonic() - t0
                # should have waited for a holder to release
                assert elapsed > 0.05
                acquired.append("waiter")
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=holder, name=f"holder-{i}") for i in range(3)]
    threads.append(threading.Thread(target=waiter, name="waiter"))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors, f"Errors: {errors}"
    assert "waiter" in acquired


def test_cursor_reusable_after_error(pool):
    """A failed query doesn't break the cursor for the next user."""
    # First: cause an error
    with pool.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM nonexistent_table")

    # Second: same cursor (pool has size 3, only 1 used) should work
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python3 -m pytest tests/test_cursor_pool.py -v
```

Expected: `ModuleNotFoundError: No module named 'cursor_pool'`

- [ ] **Step 3: Implement `cursor_pool.py`**

```python
"""Thread-safe DuckDB cursor pool for concurrent MCP query execution."""

import queue
import threading
from contextlib import contextmanager


MAX_QUERY_ROWS = 1000


class CursorPool:
    """Pool of DuckDB cursors for concurrent read access.

    DuckDB supports concurrent reads via .cursor() — each cursor is a
    lightweight sub-connection that can run one query at a time. Multiple
    cursors from one connection can run queries in parallel via MVCC.
    """

    def __init__(self, conn, size=8):
        self._conn = conn
        self._size = size
        self._semaphore = threading.Semaphore(size)
        self._available = queue.Queue()
        for _ in range(size):
            self._available.put(conn.cursor())

    @contextmanager
    def cursor(self):
        """Acquire a cursor from the pool, release on exit."""
        self._semaphore.acquire()
        cur = self._available.get()
        try:
            yield cur
        finally:
            self._available.put(cur)
            self._semaphore.release()

    def execute(self, sql, params=None, max_rows=1000):
        """Acquire cursor, execute query, return (cols, rows). Raises raw duckdb.Error."""
        with self.cursor() as cur:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = result.fetchmany(max_rows)
            return cols, rows
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
python3 -m pytest tests/test_cursor_pool.py -v
```

- [ ] **Step 5: Commit**

```bash
git add cursor_pool.py tests/test_cursor_pool.py
git commit -m "feat: add CursorPool for concurrent DuckDB query execution"
```

---

### Task 2: Migrate `percentiles.py` from lock to pool

**Files:**
- Modify: `percentiles.py:41-43,172-202`

- [ ] **Step 1: Read `percentiles.py` in full**

Understand the 6 functions. Only 2 need migration (runtime query functions):
- `lookup_percentiles(db, entity_type, key, lock=None)` — line 172
- `get_population_count(db, entity_type, lock=None)` — line 192

The build functions (`build_percentile_tables`, `build_lake_percentile_tables`) run at startup on the parent connection — leave them unchanged.

- [ ] **Step 2: Migrate `lookup_percentiles`**

Change from:
```python
def lookup_percentiles(db, entity_type, key, lock=None):
    ...
    lk = _get_lock(lock)
    try:
        with lk:
            rows = db.execute(...).fetchall()
```

To:
```python
def lookup_percentiles(pool, entity_type, key):
    ...
    try:
        with pool.cursor() as cur:
            rows = cur.execute(...).fetchall()
            if not rows:
                return {}
            cols = [c[0] for c in cur.execute(f"DESCRIBE main.{table}").fetchall()]
```

- [ ] **Step 3: Migrate `get_population_count`**

Change from:
```python
def get_population_count(db, entity_type, lock=None):
    ...
    lk = _get_lock(lock)
    try:
        with lk:
            return db.execute(...).fetchone()[0]
```

To:
```python
def get_population_count(pool, entity_type):
    ...
    try:
        with pool.cursor() as cur:
            return cur.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
```

- [ ] **Step 4: Remove `_get_lock` and `_default_lock`**

Delete lines 12 and 41-42 (`_default_lock` and `_get_lock`). These are no longer needed. The `threading` import can also be removed if no other function uses it.

- [ ] **Step 5: Commit**

```bash
git add percentiles.py
git commit -m "refactor: migrate percentiles.py from lock to cursor pool"
```

---

### Task 3: Migrate `percentile_middleware.py`

**Files:**
- Modify: `percentile_middleware.py:96-111`

- [ ] **Step 1: Update the middleware to use pool instead of db+lock**

Read lines 96-111. Change from:
```python
db = lifespan.get("db")
entity_type, key = detect_entity(result.structured_content, tool_name)
if entity_type is None:
    return result

lock = lifespan.get("lock")
percentiles = lookup_percentiles(db, entity_type, key, lock=lock)
if not percentiles:
    return result

population = get_population_count(db, entity_type, lock=lock)
```

To:
```python
pool = lifespan.get("pool")
if not pool:
    return result

entity_type, key = detect_entity(result.structured_content, tool_name)
if entity_type is None:
    return result

percentiles = lookup_percentiles(pool, entity_type, key)
if not percentiles:
    return result

population = get_population_count(pool, entity_type)
```

- [ ] **Step 2: Commit**

```bash
git add percentile_middleware.py
git commit -m "refactor: migrate percentile_middleware from lock to cursor pool"
```

---

### Task 4: Migrate `_execute()` and lifespan context in `mcp_server.py`

**Files:**
- Modify: `mcp_server.py:60-68,604-620,2732-2742`

This is the core change — `_execute()` becomes pool-based, and the lifespan context exposes the pool.

- [ ] **Step 1: Add pool import and remove `_db_lock`**

At line ~5 (imports), add:
```python
from cursor_pool import CursorPool
```

At line ~68, remove:
```python
_db_lock = threading.Lock()
```

- [ ] **Step 2: Rewrite `_execute()` (lines 604-620)**

Change from:
```python
def _execute(conn, sql, params=None):
    with _db_lock:
        try:
            cur = conn.execute(sql, params or [])
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(MAX_QUERY_ROWS)
            return cols, rows
        except duckdb.Error as e:
            ...
```

To:
```python
def _execute(pool, sql, params=None):
    with pool.cursor() as cur:
        try:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = result.fetchmany(MAX_QUERY_ROWS)
            return cols, rows
        except duckdb.Error as e:
            ...
```

Keep all the error handling and hint logic identical.

- [ ] **Step 3: Update lifespan context yield (line ~2732-2742)**

Add pool creation before the yield. Change from:
```python
    try:
        yield {
            "db": conn, "catalog": catalog,
            ...
            "lock": _db_lock,
            ...
        }
```

To:
```python
    pool = CursorPool(conn, size=8)

    try:
        yield {
            "db": conn, "pool": pool, "catalog": catalog,
            ...
            # Remove "lock": _db_lock
            ...
        }
```

Keep `"db": conn` in context — some tools may need the parent connection for non-query operations. Remove `"lock": _db_lock`.

- [ ] **Step 4: Remove startup `_db_lock` usages**

Line 559 (`_build_explorations`): Remove `with _db_lock:`, just call `db.execute()` directly — this runs before `yield`.

Lines 2670, 2677 (`build_percentile_tables`): These pass `lock=_db_lock` — change to `lock=None` (they run before `yield`, no concurrency).

- [ ] **Step 5: Give embed thread a dedicated cursor**

Lines 2693-2720: The embed thread already uses `bg_conn` for Lance writes. For reads from `conn`, it currently uses `_db_lock`. Replace the `conn` references in the embed functions with a dedicated cursor.

At line ~2695 (inside `_background_embed`), after `bg_conn` creation, add:
```python
read_cursor = conn.cursor()  # dedicated cursor for embed reads
```

Then in the calls at lines 2707-2710, replace `conn` with `read_cursor`:
```python
_build_description_embeddings(read_cursor, bg_conn, embed_batch_fn, embed_dims)
...
_build_entity_name_embeddings(read_cursor, bg_conn, embed_batch_fn, embed_dims)
_build_building_vectors(read_cursor, bg_conn)
```

Also update the embed functions (lines 1103-1178) to remove their `with _db_lock:` blocks — they receive a cursor now, not a locked connection.

- [ ] **Step 6: DO NOT commit yet — Task 5 must land in the same commit**

Tasks 4 and 5 MUST be deployed atomically. If `_execute()` expects a pool but tools still pass `db`, the server crashes. Continue directly to Task 5.

---

### Task 5: Migrate all tool-level `_db_lock` usages in `mcp_server.py`

**Files:**
- Modify: `mcp_server.py` at lines 2869, 3186, 3245, 3375, 3458, 4119, 8035, 8065, 10557, 12085

This task updates every tool that directly acquires `_db_lock` (not through `_execute`).

- [ ] **Step 1: Migrate `sql_admin` (line ~2869)**

Change `with _db_lock:` to `with pool.cursor() as cur:`. Get pool from `ctx.lifespan_context["pool"]`. This tool does DDL (CREATE VIEW) which works through cursors.

- [ ] **Step 2: Migrate `data_catalog` Lance search (line ~3186)**

Change `with _db_lock: db.execute(sem_sql)` to `with pool.cursor() as cur: cur.execute(sem_sql)`.

- [ ] **Step 3: Migrate `export_data` (line ~3245)**

Change the `with _db_lock:` block to `with pool.cursor() as cur:`. Update the `db.execute()` calls inside to `cur.execute()`. Note: this tool fetches up to 100K rows — the cursor will handle this fine.

- [ ] **Step 4: Migrate `text_search` Lance search (line ~3375)**

Same pattern as Step 2.

- [ ] **Step 5: Migrate `semantic_search` (line ~3458)**

Same pattern as Step 2.

- [ ] **Step 6: Fix `gentrification_tracker` temp tables (line ~4119)**

This is the critical temp table collision risk. Change the block to:
1. Use `with pool.cursor() as cur:` instead of `with _db_lock:`
2. Append a UUID to temp table names:

```python
import uuid
suffix = uuid.uuid4().hex[:8]
tbl = f"_gent_tmp_{suffix}"
# And for each signal:
pivot_tbl = f"_gent_{signal}_{suffix}"
```

3. Replace all `db.execute(...)` with `cur.execute(...)` inside the block.
4. Ensure DROP IF EXISTS uses the suffixed names at cleanup.

- [ ] **Step 7: Migrate `similar_buildings` (lines ~8035, ~8065)**

Two separate `with _db_lock:` blocks. Change both to `with pool.cursor() as cur:`.

- [ ] **Step 8: Migrate `fuzzy_entity_search` (line ~10557)**

Same pattern.

- [ ] **Step 9: Migrate `person_crossref` Lance fallback (line ~12085)**

Same pattern.

- [ ] **Step 10: Triage and update all `ctx.lifespan_context["db"]` references**

There are ~59 occurrences of `ctx.lifespan_context["db"]` across the file. NOT all should change. Run this triage:

```bash
grep -n 'ctx.lifespan_context\["db"\]' mcp_server.py
```

For each occurrence, classify:

**Change to `ctx.lifespan_context["pool"]`:** Any line where `db` is passed to `_execute(db, ...)`. This is the majority. `_execute` now expects a pool.

**Change to `with pool.cursor() as cur:`:** Any line where `db.execute(...)` is called directly inside a `with _db_lock:` block (already covered in Steps 1-9 above, but verify none were missed).

**Keep as `ctx.lifespan_context["db"]`:** Any line where `db` is used for:
- Accessing `lifespan_context["catalog"]` (not db-related, just same dict)
- Checking `lifespan_context["graph_ready"]` or other flags
- Passing to functions that need the parent connection (none should remain after this task)

**Practical approach:** Do a find-and-replace of the pattern `db = ctx.lifespan_context["db"]` followed by `_execute(db,` → change the variable to `pool = ctx.lifespan_context["pool"]`. For tools that use `db` for both `_execute` AND direct queries, split into two variables:
```python
pool = ctx.lifespan_context["pool"]
# If this tool also does something with the raw conn:
# db = ctx.lifespan_context["db"]  # only if needed
```

- [ ] **Step 11: Grep to verify no `_db_lock` references remain**

```bash
grep -n '_db_lock' mcp_server.py
```

Expected: 0 results. If any remain, fix them.

- [ ] **Step 12: Commit Tasks 4+5 together (atomic)**

Tasks 4 and 5 MUST be a single commit. The server would break between them.

```bash
git add mcp_server.py
git commit -m "refactor: replace _db_lock with CursorPool across all tools and _execute()"
```

---

### Task 6: Run tests and stress test

**Files:**
- No new files

- [ ] **Step 1: Run all unit tests**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python3 -m pytest tests/ -v --tb=short --ignore=tests/test_embedder.py --ignore=tests/test_middleware.py --ignore=tests/test_percentile_middleware.py --ignore=tests/test_xlsx_export.py
```

Expected: All pass. The ignored tests need Docker dependencies (numpy, openpyxl, fastmcp).

- [ ] **Step 2: Run audit phase of stress test**

```bash
python3 stress_test.py --phase audit
```

Expected: 31/31 tools OK, 0 errors. Same as before — functionality unchanged.

- [ ] **Step 3: Run ramp stress test**

```bash
python3 stress_test.py --phase ramp --max-concurrency 50
```

Target results:

| Concurrency | Target RPS | Target Error Rate |
|-------------|------------|-------------------|
| 10 | >5 | 0% |
| 20 | >10 | <2% |
| 50 | >20 | <5% |

- [ ] **Step 4: Commit stress test results**

Save the ramp output to a file and commit:
```bash
python3 stress_test.py --phase ramp --max-concurrency 50 2>&1 | tee STRESS-TEST-RESULTS.md
git add STRESS-TEST-RESULTS.md
git commit -m "docs: stress test results after cursor pool migration"
```

---

### Task 7: Deploy and verify

- [ ] **Step 1: Rsync changed files to server**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  duckdb-server/cursor_pool.py \
  duckdb-server/mcp_server.py \
  duckdb-server/percentiles.py \
  duckdb-server/percentile_middleware.py \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
```

- [ ] **Step 2: Rebuild and restart**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d --build --force-recreate duckdb-server 2>&1 | tail -5"
```

- [ ] **Step 3: Verify startup**

```bash
sleep 30 && ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose logs duckdb-server --tail 10"
```

Verify: "DuckLake catalog attached", "Graph loaded from cache", no tracebacks.

- [ ] **Step 4: Run production stress test**

```bash
python3 stress_test.py --phase ramp --max-concurrency 50
```

Compare to pre-migration baseline. Target: error rate at 50 concurrent drops from 22.8% to <5%.

- [ ] **Step 5: Check PostHog for events**

Wait 60s, then verify `mcp_tool_called` events are arriving in PostHog with the stress test tool calls.
