# MCP Server Hardening v2 — Performance, Security & Reliability

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all P0–P2 audit findings from the FastMCP/DuckDB best practices review — SQL injection, error leakage, cursor pool reliability, reconnect race condition, catalog endpoint, lifespan safety, and performance tuning.

**Architecture:** Surgical fixes to existing modules. No new files except tests. The SQL builders (`entity.py`, `spatial.py`) get refactored to return `(sql, params)` tuples instead of interpolated strings. The cursor pool gets timeout, health checks, and thread-safe reconnect. The `/api/catalog` endpoint switches to the shared pool. All changes are backward-compatible with existing tool call sites in `mcp_server.py`.

**Tech Stack:** Python 3.13, DuckDB 1.5.0, FastMCP 2.x, pytest

**Codebase:** `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server/`

**Key docs:**
- DuckDB Python DB API: https://duckdb.org/docs/stable/clients/python/dbapi (parameterized queries use `?` placeholders)
- DuckDB concurrency: https://duckdb.org/docs/stable/connect/concurrency (single-writer, multi-cursor reads via MVCC)
- FastMCP middleware: https://gofastmcp.com/servers/middleware (Middleware subclass, `on_call_tool`, `call_next`)
- FastMCP ToolError: https://gofastmcp.com/servers/tools#error-handling (`ToolError` for client-visible errors, mask internals)
- DuckDB memory management: https://duckdb.org/2024/07/09/memory-management (`max_temp_directory_size`, spilling)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `sql_utils.py` | **Create** | Shared `_validate_identifier()` used by entity.py, spatial.py, quality.py |
| `entity.py` | **Modify** | Refactor all 4 functions to return `(sql, params)` tuples with `?` placeholders |
| `spatial.py` | **Modify** | Refactor all 5 functions to return `(sql, params)` tuples with `?` placeholders |
| `quality.py` | **Modify** | Import `_validate_identifier` from `sql_utils.py` instead of local copy |
| `cursor_pool.py` | **Modify** | Add timeout, cursor health check (replace-on-error), `close()` method |
| `mcp_server.py:58` | **Modify** | Thread-safe reconnect lock for `_last_reconnect` |
| `mcp_server.py:646-709` | **Modify** | Use reconnect lock in `_execute()` |
| `mcp_server.py:787-987` | **Modify** | Add `try/finally` to lifespan, add `preserve_insertion_order` |
| `mcp_server.py:13207-13408` | **Modify** | Replace `_catalog_connect()` with shared pool usage |
| `mcp_server.py:654` | **Modify** | Wrap error messages to avoid leaking internals |
| `citation_middleware.py` | **Modify** | Extract `TOOL_SOURCES` to `constants.py` |
| `freshness_middleware.py` | **Modify** | Import `TOOL_SOURCES` from `constants.py`, remove fallback copy |
| `constants.py` | **Modify** | Add `TOOL_SOURCES` dict (single source of truth) |
| `tests/test_entity_params.py` | **Exists** | Tests already written — will pass after entity.py refactor |
| `tests/test_spatial_params.py` | **Exists** | Tests already written — will pass after spatial.py refactor |
| `tests/test_cursor_pool.py` | **Modify** | Add timeout test, health check test |
| `tests/test_sql_safety.py` | **Create** | Test `_validate_sql` and error sanitization |

---

## Task 0: Create Shared `sql_utils.py` (Prerequisite)

**Files:**
- Create: `sql_utils.py`
- Modify: `quality.py` (import from sql_utils instead of local copy)
- Create: `tests/test_sql_utils.py`

**Context:** `_validate_identifier()` is duplicated in `quality.py` and will be needed by `entity.py` and `spatial.py`. Extract to a shared module before the SQL parameterization tasks.

- [ ] **Step 1: Write test for `_validate_identifier`**

Create `tests/test_sql_utils.py`:

```python
import pytest
from sql_utils import validate_identifier


def test_valid_simple_column():
    assert validate_identifier("first_name") == "first_name"


def test_valid_dotted_table():
    assert validate_identifier("lake.housing.hpd_violations") == "lake.housing.hpd_violations"


def test_rejects_sql_injection():
    with pytest.raises(ValueError):
        validate_identifier("'; DROP TABLE --")


def test_rejects_semicolon():
    with pytest.raises(ValueError):
        validate_identifier("name; DELETE")


def test_rejects_empty():
    with pytest.raises(ValueError):
        validate_identifier("")


def test_rejects_leading_number():
    with pytest.raises(ValueError):
        validate_identifier("1table")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_sql_utils.py -v
```

Expected: FAIL (module does not exist)

- [ ] **Step 3: Create `sql_utils.py`**

```python
"""Shared SQL utilities for the Common Ground MCP server."""

import re

_VALID_IDENTIFIERS = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier (table name, column name). Raises ValueError if unsafe."""
    if not _VALID_IDENTIFIERS.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name
```

- [ ] **Step 4: Run test**

```bash
python -m pytest tests/test_sql_utils.py -v
```

Expected: ALL PASS

- [ ] **Step 5: Update `quality.py` to import from `sql_utils`**

Replace the local `_VALID_IDENTIFIERS` regex and `_validate_identifier` function (lines 9-15) with:

```python
from sql_utils import validate_identifier as _validate_identifier
```

- [ ] **Step 6: Run quality tests (if any) and entity param tests**

```bash
python -m pytest tests/ -v -k "not test_middleware" --tb=short 2>&1 | tail -20
```

Expected: Existing tests still pass

- [ ] **Step 7: Commit**

```bash
git add sql_utils.py quality.py tests/test_sql_utils.py
git commit -m "refactor: extract validate_identifier to shared sql_utils module"
```

---

## Task 1: Parameterize `entity.py` (P0 — SQL Injection Fix)

**Files:**
- Modify: `entity.py` (all 4 functions)
- Test: `tests/test_entity_params.py` (already written, currently failing)

**Context:** The existing tests in `test_entity_params.py` already expect `(sql, params)` tuple returns with `?` placeholders. The call sites in `mcp_server.py` already unpack with `ph_sql, ph_params = phonetic_search_sql(...)`. So the implementation just needs to match. The `_validate_identifier` function is imported from `sql_utils.py` (created in Task 0).

**DuckDB parameterized query syntax:** `cursor.execute("SELECT * WHERE col = ?", [value])` — uses `?` positional placeholders, params as a list.

- [ ] **Step 1: Run existing tests to confirm they fail**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_entity_params.py -v 2>&1 | head -30
```

Expected: FAIL (functions return `str` not `tuple`)

- [ ] **Step 2: Rewrite `phonetic_search_sql` to return `(sql, params)`**

Replace string interpolation with `?` placeholders. The function signature stays the same, but now returns `tuple[str, list]`.

```python
from sql_utils import validate_identifier as _validate_identifier


def phonetic_search_sql(
    first_name: str | None,
    last_name: str,
    min_score: float = 0.75,
    limit: int = 50,
) -> tuple[str, list]:
    """SQL + params for phonetic person search. Returns (sql, params) tuple."""
    if first_name:
        sql = """
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS last_score,
                jaro_winkler_similarity(UPPER(first_name), UPPER(?)) AS first_score,
                (jaro_winkler_similarity(UPPER(last_name), UPPER(?)) * 0.6
                 + jaro_winkler_similarity(UPPER(first_name), UPPER(?)) * 0.4
                ) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER(?))
              AND (jaro_winkler_similarity(UPPER(last_name), UPPER(?)) * 0.6
                   + jaro_winkler_similarity(UPPER(first_name), UPPER(?)) * 0.4
                  ) >= ?
            ORDER BY combined_score DESC
            LIMIT ?
        """
        params = [last_name, first_name, last_name, first_name, last_name, last_name, first_name, min_score, limit]
        return sql, params
    else:
        sql = """
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS last_score,
                1.0 AS first_score,
                jaro_winkler_similarity(UPPER(last_name), UPPER(?)) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER(?))
              AND jaro_winkler_similarity(UPPER(last_name), UPPER(?)) >= ?
            ORDER BY combined_score DESC
            LIMIT ?
        """
        params = [last_name, last_name, last_name, last_name, min_score, limit]
        return sql, params
```

- [ ] **Step 3: Rewrite `fuzzy_name_sql` to return `(sql, params)`**

Table and column names can't be parameterized (DuckDB doesn't support `?` for identifiers), so validate them with `_validate_identifier` and interpolate only those. User-supplied search terms use `?`.

```python
def fuzzy_name_sql(
    name: str,
    table: str = "lake.federal.name_index",
    name_col: str = "last_name",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """SQL + params for fuzzy entity/company name matching."""
    _validate_identifier(table)
    _validate_identifier(name_col)
    sql = f"""
        SELECT *,
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= ?
        ORDER BY match_score DESC
        LIMIT ?
    """
    return sql, [name, name, min_score, limit]
```

- [ ] **Step 4: Rewrite `phonetic_vital_search_sql` to return `(sql, params)`**

```python
def phonetic_vital_search_sql(
    first_name: str | None,
    last_name: str,
    table: str,
    first_col: str,
    last_col: str,
    extra_cols: str = "",
    limit: int = 30,
) -> tuple[str, list]:
    """Phonetic search for historical records. Returns (sql, params) tuple."""
    _validate_identifier(table)
    _validate_identifier(first_col)
    _validate_identifier(last_col)
    if extra_cols:
        for col in extra_cols.split(","):
            _validate_identifier(col.strip())
    extra = f", {extra_cols}" if extra_cols else ""

    if first_name:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score,
                jaro_winkler_similarity(UPPER({first_col}), UPPER(?)) AS first_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC, first_score DESC
            LIMIT ?
        """
        return sql, [last_name, first_name, last_name, limit]
    else:
        sql = f"""
            SELECT {first_col}, {last_col}{extra},
                jaro_winkler_similarity(UPPER({last_col}), UPPER(?)) AS last_score
            FROM {table}
            WHERE soundex(UPPER({last_col})) = soundex(UPPER(?))
            ORDER BY last_score DESC
            LIMIT ?
        """
        return sql, [last_name, last_name, limit]
```

- [ ] **Step 5: Rewrite `fuzzy_money_search_sql` to return `(sql, params)`**

```python
def fuzzy_money_search_sql(
    name: str,
    table: str,
    name_col: str,
    extra_cols: str = "",
    min_score: int = 70,
    limit: int = 30,
) -> tuple[str, list]:
    """Fuzzy name matching for campaign finance. Returns (sql, params) tuple."""
    _validate_identifier(table)
    _validate_identifier(name_col)
    if extra_cols:
        for col in extra_cols.split(","):
            _validate_identifier(col.strip())
    extra = f", {extra_cols}" if extra_cols else ""
    sql = f"""
        SELECT {name_col}{extra},
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER(?)) >= ?
        ORDER BY match_score DESC
        LIMIT ?
    """
    return sql, [name, name, min_score, limit]
```

- [ ] **Step 6: Run entity tests**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_entity_params.py -v
```

Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add entity.py tests/test_entity_params.py
git commit -m "fix(security): parameterize entity.py SQL builders to prevent injection"
```

---

## Task 2: Parameterize `spatial.py` (P0 — SQL Injection Fix)

**Files:**
- Modify: `spatial.py` (all 5 functions)
- Test: `tests/test_spatial_params.py` (already written, currently failing)

**Context:** Same pattern as Task 1. Table names validated via `_validate_identifier`, lat/lng/zipcode use `?` placeholders. The H3 functions are nested (e.g., `h3_aggregate_sql` calls `h3_kring_sql` internally) — each returns its own `(sql, params)` and the parent merges them.

**Important DuckDB note:** DuckDB supports `?` for values but NOT for function arguments that must be compile-time constants. H3 resolution is always `9` (a constant), so it stays hardcoded. Lat/lng/zipcode are user values and get parameterized.

- [ ] **Step 1: Run existing tests to confirm they fail**

```bash
python -m pytest tests/test_spatial_params.py -v 2>&1 | head -20
```

Expected: FAIL

- [ ] **Step 2: Rewrite all 5 spatial functions**

```python
from sql_utils import validate_identifier as _validate_identifier

H3_RES = 9


def h3_kring_sql(lat: float, lng: float, radius_rings: int = 2) -> tuple[str, list]:
    """SQL + params to get H3 cells within radius of a point."""
    sql = f"""
        SELECT UNNEST(
            h3_grid_disk(h3_latlng_to_cell(?, ?, {H3_RES}), ?)
        ) AS h3_cell
    """
    return sql, [lat, lng, radius_rings]


def h3_aggregate_sql(
    source_table: str,
    filter_tables: list[str],
    lat: float,
    lng: float,
    radius_rings: int = 3,
) -> tuple[str, list]:
    """SQL + params to aggregate counts within a hex k-ring."""
    _validate_identifier(source_table)
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    placeholders = ", ".join("?" for _ in filter_tables)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        )
        SELECT
            source_table,
            COUNT(*) AS row_count,
            COUNT(DISTINCT h3_res9) AS cell_count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table IN ({placeholders})
        GROUP BY source_table
        ORDER BY row_count DESC
    """
    return sql, kring_params + filter_tables


def h3_heatmap_sql(
    source_table: str,
    filter_table: str,
    lat: float,
    lng: float,
    radius_rings: int = 5,
) -> tuple[str, list]:
    """SQL + params for per-cell heatmap counts."""
    _validate_identifier(source_table)
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        )
        SELECT
            h3_res9 AS h3_cell,
            h3_cell_to_lat(h3_res9) AS cell_lat,
            h3_cell_to_lng(h3_res9) AS cell_lng,
            COUNT(*) AS count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table = ?
        GROUP BY h3_res9
        ORDER BY count DESC
    """
    return sql, kring_params + [filter_table]


def h3_zip_centroid_sql(zipcode: str) -> tuple[str, list]:
    """SQL + params to get H3 centroid for a ZIP code."""
    sql = f"""
        SELECT h3_latlng_to_cell(
            AVG(TRY_CAST(latitude AS DOUBLE)),
            AVG(TRY_CAST(longitude AS DOUBLE)),
            {H3_RES}
        ) AS center_cell,
        AVG(TRY_CAST(latitude AS DOUBLE)) AS center_lat,
        AVG(TRY_CAST(longitude AS DOUBLE)) AS center_lng
        FROM lake.city_government.pluto
        WHERE zipcode = ?
          AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
          AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
    """
    return sql, [zipcode]


def h3_neighborhood_stats_sql(lat: float, lng: float, radius_rings: int = 8) -> tuple[str, list]:
    """SQL + params for multi-dimension neighborhood stats."""
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    sql = f"""
        WITH target_cells AS (
            {kring_sql}
        ),
        h3_stats AS (
            SELECT source_table, COUNT(*) AS cnt
            FROM lake.foundation.h3_index
            WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
            GROUP BY source_table
        )
        SELECT
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_complaints_ytd'), 0)
                AS total_crimes,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_historic'), 0)
                + COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.nypd_arrests_ytd'), 0)
                AS total_arrests,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'health.restaurant_inspections'), 0) AS restaurants,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'social_services.n311_service_requests'), 0) AS n311_calls,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'public_safety.shootings'), 0) AS shootings,
            COALESCE(MAX(cnt) FILTER (WHERE source_table = 'environment.street_trees'), 0) AS street_trees
        FROM h3_stats
    """
    return sql, kring_params
```

- [ ] **Step 3: Run spatial tests**

```bash
python -m pytest tests/test_spatial_params.py -v
```

Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add spatial.py tests/test_spatial_params.py
git commit -m "fix(security): parameterize spatial.py SQL builders to prevent injection"
```

---

## Task 3: Harden Cursor Pool (P1 — Reliability)

**Files:**
- Modify: `cursor_pool.py`
- Modify: `tests/test_cursor_pool.py`

**Context:** Three issues: (1) no timeout on `semaphore.acquire()` — blocks forever if all cursors busy; (2) no health check — broken cursors returned to pool; (3) no `close()` method for clean shutdown.

**DuckDB cursor behavior:** A cursor that threw an error on a previous query can still execute new queries — DuckDB auto-rolls back. But if the cursor is in a truly broken state (e.g., connection dropped), we need to replace it. The safest approach: on exception, try to close the cursor and create a fresh one from the connection.

- [ ] **Step 1: Write failing tests for new features**

Add to `tests/test_cursor_pool.py`:

```python
def test_cursor_timeout():
    """Pool should raise TimeoutError when all cursors are busy."""
    conn = duckdb.connect()
    p = CursorPool(conn, size=1, timeout=0.1)
    with p.cursor() as _:
        with pytest.raises(TimeoutError, match="cursor"):
            with p.cursor() as _:
                pass
    conn.close()


def test_cursor_replaced_after_fatal_error(pool):
    """A cursor that fails should be replaced, not returned broken."""
    # Force a cursor into bad state by interrupting mid-query
    # Then verify next acquire gets a working cursor
    with pool.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM nonexistent_table_xyz")
    # Next cursor should work fine
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3


def test_pool_close():
    conn = duckdb.connect()
    p = CursorPool(conn, size=2)
    p.close()
    # After close, cursor acquisition should fail
    with pytest.raises(Exception):
        with p.cursor() as _:
            pass
    conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_cursor_pool.py::test_cursor_timeout -v
python -m pytest tests/test_cursor_pool.py::test_pool_close -v
```

Expected: FAIL (no timeout param, no close method)

- [ ] **Step 3: Implement hardened `CursorPool`**

```python
"""Thread-safe DuckDB cursor pool for concurrent MCP query execution."""

import queue
import threading
from contextlib import contextmanager

_DEFAULT_TIMEOUT = 30  # seconds


class CursorPool:
    """Pool of DuckDB cursors for concurrent read access.

    DuckDB supports concurrent reads via .cursor() — each cursor is a
    lightweight sub-connection that can run one query at a time.
    """

    def __init__(self, conn, size=8, timeout=_DEFAULT_TIMEOUT):
        self._conn = conn
        self._size = size
        self._timeout = timeout
        self._semaphore = threading.Semaphore(size)
        self._available = queue.Queue()
        self._closed = False
        for _ in range(size):
            self._available.put(conn.cursor())

    @contextmanager
    def cursor(self):
        """Acquire a cursor from the pool, release on exit."""
        if self._closed:
            raise RuntimeError("CursorPool is closed")
        if not self._semaphore.acquire(timeout=self._timeout):
            raise TimeoutError(
                f"All {self._size} database cursors busy for {self._timeout}s — try again shortly"
            )
        cur = self._available.get()
        try:
            yield cur
        except Exception:
            # Replace potentially broken cursor
            try:
                cur.close()
            except Exception:
                pass
            cur = self._conn.cursor()
            raise
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

    def close(self):
        """Drain all cursors and mark pool as closed."""
        self._closed = True
        while not self._available.empty():
            try:
                cur = self._available.get_nowait()
                cur.close()
            except (queue.Empty, Exception):
                break
```

- [ ] **Step 4: Run all cursor pool tests**

```bash
python -m pytest tests/test_cursor_pool.py -v
```

Expected: ALL PASS (including new tests)

- [ ] **Step 5: Commit**

```bash
git add cursor_pool.py tests/test_cursor_pool.py
git commit -m "fix(reliability): add timeout, health check, and close() to CursorPool"
```

---

## Task 4: Thread-Safe Reconnect in `_execute` (P1 — Race Condition)

**Files:**
- Modify: `mcp_server.py:58` (add lock)
- Modify: `mcp_server.py:646-709` (use lock in `_execute`)

**Context:** `_last_reconnect` is a module-level `int` mutated without a lock. Two concurrent threads hitting a stale S3 error could both decide to reconnect, causing double DETACH/ATTACH. Fix: add a `threading.Lock` that serializes the reconnect path.

- [ ] **Step 1: Add reconnect lock at module level (line ~58)**

Replace:
```python
_last_reconnect = 0
```

With:
```python
_reconnect_lock = threading.Lock()
_last_reconnect = 0
```

- [ ] **Step 2: Use lock in `_execute` reconnect path**

In the `_execute` function (line ~646), wrap the reconnect block:

Replace the reconnect check:
```python
if any(sig in err for sig in _RECONNECT_ERRORS) and time.time() - _last_reconnect > 60:
    _last_reconnect = time.time()
    try:
        ...
```

With:
```python
if any(sig in err for sig in _RECONNECT_ERRORS):
    with _reconnect_lock:
        # Double-check after acquiring lock (another thread may have reconnected)
        if time.time() - _last_reconnect > 60:
            _last_reconnect = time.time()
            try:
                ...
```

Keep the rest of the reconnect logic identical, just indented one level deeper inside the `with _reconnect_lock:` block.

- [ ] **Step 3: Add `sanitize_error` to `sql_utils.py`**

Append to `sql_utils.py`:

```python
def sanitize_error(msg: str) -> str:
    """Strip file paths and credentials from error messages before exposing to clients."""
    # Remove absolute file paths (keep the filename part for debugging)
    msg = re.sub(r'(/[a-zA-Z0-9_./-]{3,})', lambda m: '[path]/' + m.group(1).rsplit('/', 1)[-1] if '/' in m.group(1) else m.group(0), msg)
    # Remove password= values
    msg = re.sub(r'password=[^\s&\'"]+', 'password=***', msg, flags=re.IGNORECASE)
    # Remove connection strings that might contain credentials
    msg = re.sub(r'(dbname=\w+\s+user=\w+\s+)password=[^\s]+', r'\1password=***', msg, flags=re.IGNORECASE)
    return msg
```

- [ ] **Step 4: Use `sanitize_error` in `_execute`**

At line ~709, the `ToolError` exposes the raw DuckDB exception. Import and use:

At the top of `mcp_server.py`, add to the existing imports:
```python
from sql_utils import sanitize_error
```

Replace:
```python
raise ToolError(f"SQL error: {e}{hint}")
```

With:
```python
raise ToolError(f"SQL error: {sanitize_error(str(e))}{hint}")
```

- [ ] **Step 5: Commit**

```bash
git add sql_utils.py mcp_server.py
git commit -m "fix(reliability): thread-safe reconnect lock + sanitize error messages"
```

---

## Task 5: Fix `/api/catalog` to Use Shared Pool (P1 — Performance)

**Files:**
- Modify: `mcp_server.py:13207-13408` (delete `_catalog_connect`, rewrite endpoint)

**Context:** Currently `/api/catalog` creates a brand-new DuckDB connection, loads all extensions, attaches DuckLake, runs queries, and closes — every single request. This takes seconds. The lifespan already has a pool with 16 cursors attached to the same lake. Just use it.

- [ ] **Step 1: Delete `_catalog_connect` function**

Remove lines 13207-13229 entirely.

- [ ] **Step 2: Rewrite `catalog_json` to use lifespan pool**

The `@mcp.custom_route` handler receives a `Request` from Starlette. To access lifespan context, we need the pool reference. Since `mcp_server.py` already stores the pool in a module-level variable during lifespan (or we can access it via the app state), the simplest approach: store pool as a module-level ref set during lifespan.

At the end of `app_lifespan` (just before `yield`), add:
```python
global _shared_pool
_shared_pool = pool
```

At module level (near line 58):
```python
_shared_pool = None
```

Then rewrite the endpoint. The full function body (query logic is identical to current, only the connection source changes):

```python
@mcp.custom_route("/api/catalog", methods=["GET", "OPTIONS"])
async def catalog_json(request: Request) -> JSONResponse:
    """Return table catalog as JSON for the data health page."""

    # Handle CORS preflight
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )

    pool = _shared_pool
    if pool is None:
        return JSONResponse({"error": "Server starting up"}, status_code=503)

    try:
        # Query table stats from DuckDB metadata
        cols, rows = _execute(pool, """
            SELECT
                t.schema_name,
                t.table_name,
                t.estimated_size,
                (SELECT COUNT(*) FROM duckdb_columns() c
                 WHERE c.schema_name = t.schema_name AND c.table_name = t.table_name) AS column_count
            FROM duckdb_tables() t
            WHERE t.schema_name NOT LIKE '%staging%'
              AND t.schema_name NOT LIKE 'test%'
              AND t.schema_name NOT LIKE 'ducklake%'
              AND t.schema_name NOT LIKE 'information%'
              AND t.schema_name != 'lake'
              AND t.schema_name != 'foundation'
              AND t.table_name NOT LIKE '%__null'
              AND t.table_name NOT LIKE '%__footnotes'
            ORDER BY t.schema_name, t.estimated_size DESC
        """)

        # DuckLake file metadata
        ducklake_info = {}
        try:
            _, dl_rows = _execute(pool, "SELECT table_name, file_count, file_size_bytes, table_uuid FROM ducklake_table_info('lake')")
            for dlr in dl_rows:
                ducklake_info[dlr[0]] = {"file_count": dlr[1], "file_size_bytes": dlr[2], "table_uuid": str(dlr[3]) if dlr[3] else None}
        except Exception:
            pass

        # Pipeline cursor state
        pipeline_state = {}
        try:
            try:
                _, ps_rows = _execute(pool, """
                    SELECT dataset_name, last_updated_at, row_count, last_run_at,
                           source_rows, sync_status, source_checked_at
                    FROM lake._pipeline_state
                """)
                for psr in ps_rows:
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                        "source_rows": psr[4],
                        "sync_status": psr[5],
                        "source_checked_at": str(psr[6]) if psr[6] else None,
                    }
            except Exception:
                _, ps_rows = _execute(pool, """
                    SELECT dataset_name, last_updated_at, row_count, last_run_at
                    FROM lake._pipeline_state
                """)
                for psr in ps_rows:
                    pipeline_state[psr[0]] = {
                        "cursor": str(psr[1]) if psr[1] else None,
                        "rows_written": psr[2],
                        "last_run_at": str(psr[3]) if psr[3] else None,
                        "source_rows": None, "sync_status": None, "source_checked_at": None,
                    }
        except Exception:
            pass

        tables = []
        schema_stats = {}
        total_rows = 0

        for row in rows:
            schema, table, est_size, col_count = row[:4]
            if table.startswith('_dlt_') or table.startswith('_pipeline'):
                continue

            dl = ducklake_info.get(table, {})
            file_count = dl.get("file_count", 0)
            file_size = dl.get("file_size_bytes", 0)
            table_uuid = dl.get("table_uuid")

            created_at = None
            if table_uuid:
                try:
                    hex_str = table_uuid.replace('-', '')
                    if len(hex_str) >= 13 and hex_str[12] == '7':
                        epoch_ms = int(hex_str[:12], 16)
                        created_at = _dt.datetime.fromtimestamp(epoch_ms / 1000, tz=_dt.timezone.utc).isoformat()
                except (ValueError, OSError):
                    pass

            ps_key = f"{schema}.{table}"
            ps = pipeline_state.get(ps_key, {})

            tables.append({
                "schema": schema, "table": table,
                "rows": est_size or 0, "columns": col_count or 0,
                "files": file_count or 0, "size_bytes": file_size or 0,
                "created_at": created_at,
                "last_run_at": ps.get("last_run_at"),
                "cursor": ps.get("cursor"),
                "rows_written": ps.get("rows_written"),
                "source_rows": ps.get("source_rows"),
                "sync_status": ps.get("sync_status"),
                "source_checked_at": ps.get("source_checked_at"),
            })
            total_rows += (est_size or 0)
            if schema not in schema_stats:
                schema_stats[schema] = {"tables": 0, "rows": 0}
            schema_stats[schema]["tables"] += 1
            schema_stats[schema]["rows"] += (est_size or 0)

        result = {
            "as_of": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "summary": {"schemas": len(schema_stats), "tables": len(tables), "total_rows": total_rows},
            "schemas": schema_stats,
            "tables": tables,
        }

        return JSONResponse(
            result,
            headers={
                "Access-Control-Allow-Origin": _cors_origin(request),
                "Vary": "Origin",
                "Cache-Control": "public, max-age=300",
            },
        )
    except Exception as e:
        print(f"[catalog] Error: {e}", flush=True)
        return JSONResponse({"error": "Internal server error"}, status_code=500)
```

- [ ] **Step 3: Verify no other references to `_catalog_connect`**

```bash
grep -n "_catalog_connect" mcp_server.py
```

Expected: no results after deletion.

- [ ] **Step 4: Commit**

```bash
git add mcp_server.py
git commit -m "perf: /api/catalog uses shared cursor pool instead of per-request connection"
```

---

## Task 6: Add `try/finally` to Lifespan + Performance Settings (P2)

**Files:**
- Modify: `mcp_server.py:787-987` (lifespan function)

**Context:** If the server is cancelled after `yield`, cleanup (closing the connection, stopping background threads) won't run without `try/finally`. Also add `preserve_insertion_order = false` for analytics perf.

- [ ] **Step 1: Wrap lifespan yield in try/finally**

Find the `yield` statement in `app_lifespan` (it yields the context dict). Wrap it:

```python
    # ... all setup code above ...

    try:
        yield {
            "db": conn,
            "pool": pool,
            "catalog": catalog,
            # ... all other context keys ...
        }
    finally:
        print("Shutting down: cleaning up resources...", flush=True)
        try:
            pool.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        print("Shutdown complete", flush=True)
```

- [ ] **Step 2: Add `preserve_insertion_order` setting**

After the existing performance tuning block (around line 814-817), add:

```python
conn.execute("SET preserve_insertion_order = false")
```

This tells DuckDB it doesn't need to maintain row insertion order for queries that don't have ORDER BY — reduces memory usage for analytics.

- [ ] **Step 3: Commit**

```bash
git add mcp_server.py
git commit -m "fix(reliability): try/finally in lifespan + preserve_insertion_order optimization"
```

---

## Task 7: Consolidate `TOOL_SOURCES` to Single Source of Truth (P2)

**Files:**
- Modify: `constants.py` (add `TOOL_SOURCES`)
- Modify: `citation_middleware.py` (import from constants)
- Modify: `freshness_middleware.py` (import from constants, remove fallback)

**Context:** `TOOL_SOURCES` dict is duplicated between `citation_middleware.py` and `freshness_middleware.py` (with a try/except import fallback). When tools are added, both must be updated. Move to `constants.py`.

- [ ] **Step 1: Add `TOOL_SOURCES` to `constants.py`**

Copy the dict from `citation_middleware.py` into `constants.py`, after `MIDDLEWARE_SKIP_TOOLS`.

- [ ] **Step 2: Update `citation_middleware.py`**

Replace the local `TOOL_SOURCES` definition with:
```python
from constants import TOOL_SOURCES
```

Remove the entire dict (lines 15-33).

- [ ] **Step 3: Update `freshness_middleware.py`**

Replace the try/except import block (lines 13-34) with:
```python
from constants import TOOL_SOURCES
```

- [ ] **Step 4: Run existing middleware tests**

```bash
python -m pytest tests/test_middleware.py tests/test_percentile_middleware.py -v
```

Expected: ALL PASS (middleware behavior unchanged)

- [ ] **Step 5: Commit**

```bash
git add constants.py citation_middleware.py freshness_middleware.py
git commit -m "refactor: consolidate TOOL_SOURCES into constants.py (single source of truth)"
```

---

## Task 8: SQL Safety Tests (P0 — Verification)

**Files:**
- Create: `tests/test_sql_safety.py`

**Context:** `_validate_sql` is a critical security function but has no dedicated tests. Write tests covering: DDL blocking, stacked statements, filesystem function blocking, safe queries passing, and error message sanitization.

**Important:** These tests import the actual regex patterns from `mcp_server.py` to avoid drift. The module exposes `_UNSAFE_SQL`, `_SAFE_DDL`, `_UNSAFE_FUNCTIONS` at module level (lines 51-71). We also import `sanitize_error` (a new helper extracted in Task 4).

- [ ] **Step 1: Write tests**

```python
"""Tests for SQL validation and error sanitization in mcp_server."""

import sys
import os
import re
import pytest

# Add server dir to path so we can import from it
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sql_utils import validate_identifier, sanitize_error


# Import the actual regex patterns from mcp_server to test them directly.
# These are module-level compiled patterns, not internal functions.
from mcp_server import _UNSAFE_SQL, _SAFE_DDL, _UNSAFE_FUNCTIONS


class TestUnsafeSQLPattern:
    def test_blocks_drop_table(self):
        assert _UNSAFE_SQL.match("DROP TABLE users")

    def test_blocks_insert(self):
        assert _UNSAFE_SQL.match("INSERT INTO users VALUES (1)")

    def test_blocks_delete(self):
        assert _UNSAFE_SQL.match("DELETE FROM users")

    def test_blocks_create_table(self):
        assert _UNSAFE_SQL.match("CREATE TABLE evil (id INT)")

    def test_blocks_attach(self):
        assert _UNSAFE_SQL.match("ATTACH 'malicious.db'")

    def test_allows_select(self):
        assert not _UNSAFE_SQL.match("SELECT * FROM users")

    def test_allows_with(self):
        assert not _UNSAFE_SQL.match("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_allows_explain(self):
        assert not _UNSAFE_SQL.match("EXPLAIN SELECT * FROM users")

    def test_case_insensitive(self):
        assert _UNSAFE_SQL.match("drop table users")
        assert _UNSAFE_SQL.match("  DROP TABLE users")


class TestSafeDDLPattern:
    def test_allows_create_or_replace_view(self):
        assert _SAFE_DDL.match("CREATE OR REPLACE VIEW my_view AS SELECT 1")

    def test_blocks_create_table(self):
        assert not _SAFE_DDL.match("CREATE TABLE evil (id INT)")

    def test_blocks_create_view_without_replace(self):
        assert not _SAFE_DDL.match("CREATE VIEW my_view AS SELECT 1")


class TestUnsafeFunctions:
    def test_blocks_read_parquet(self):
        assert _UNSAFE_FUNCTIONS.search("SELECT * FROM read_parquet('/etc/passwd')")

    def test_blocks_read_csv(self):
        assert _UNSAFE_FUNCTIONS.search("SELECT read_csv('/tmp/data.csv')")

    def test_blocks_glob(self):
        assert _UNSAFE_FUNCTIONS.search("SELECT * FROM glob('/data/*')")

    def test_blocks_http_get(self):
        assert _UNSAFE_FUNCTIONS.search("SELECT http_get('http://evil.com')")

    def test_allows_normal_queries(self):
        assert not _UNSAFE_FUNCTIONS.search("SELECT * FROM lake.housing.hpd_violations")

    def test_allows_function_name_in_column(self):
        # Column named 'glob_pattern' should NOT trigger the block
        assert not _UNSAFE_FUNCTIONS.search("SELECT glob_pattern FROM config")


class TestErrorSanitization:
    def test_strips_file_paths(self):
        result = sanitize_error("Cannot open file /data/common-ground/lance/entity.lance")
        assert "/data" not in result
        assert "[path]" in result

    def test_strips_password(self):
        result = sanitize_error("Connection failed: password=SuperSecret123 host=postgres")
        assert "SuperSecret123" not in result
        assert "password=***" in result

    def test_preserves_useful_message(self):
        result = sanitize_error("Table 'violations' does not exist")
        assert "does not exist" in result
```

**Note:** `sanitize_error` is a new function extracted to `sql_utils.py` in Task 4 Step 3 (alongside `validate_identifier`). This avoids importing the entire `mcp_server.py` module just for error sanitization, and makes it independently testable.

- [ ] **Step 2: Run tests**

```bash
python -m pytest tests/test_sql_safety.py -v
```

Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_sql_safety.py
git commit -m "test: add SQL validation and error sanitization tests"
```

---

## Task 9: Final Integration Verification

**Files:** None (verification only)

- [ ] **Step 1: Run all tests**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/ -v --tb=short
```

Expected: ALL PASS

- [ ] **Step 2: Verify no import errors in mcp_server.py**

```bash
python -c "
import sys; sys.path.insert(0, '.')
from entity import phonetic_search_sql, fuzzy_name_sql
from spatial import h3_kring_sql, h3_aggregate_sql, h3_heatmap_sql
from cursor_pool import CursorPool
from constants import MIDDLEWARE_SKIP_TOOLS, TOOL_SOURCES
print('All imports OK')
sql, params = phonetic_search_sql('John', 'Smith')
assert isinstance(params, list), 'entity.py must return (sql, params) tuple'
sql, params = h3_kring_sql(40.7, -74.0)
assert isinstance(params, list), 'spatial.py must return (sql, params) tuple'
print('Parameterized queries verified')
"
```

- [ ] **Step 3: Check for remaining string interpolation in SQL builders**

```bash
grep -n "f'" entity.py spatial.py | grep -v "_validate_identifier\|_VALID" | head -20
```

Expected: Only f-strings for validated identifiers (table names, column names), never for user values.

- [ ] **Step 4: Final commit with all changes**

```bash
git add -A
git status
# If any unstaged files remain, review and add explicitly
git commit -m "feat: MCP server hardening v2 — security, reliability, performance

- Parameterized SQL in entity.py and spatial.py (prevent injection)
- CursorPool: timeout, health checks, close()
- Thread-safe reconnect lock in _execute
- Error message sanitization (no path/credential leaks)
- /api/catalog uses shared pool (was creating new connection per request)
- Lifespan try/finally for clean shutdown
- preserve_insertion_order=false for analytics perf
- TOOL_SOURCES consolidated to constants.py
- SQL safety test suite"
```

---

## Summary of Changes

| Finding | Fix | Task |
|---------|-----|------|
| Prereq: Shared identifier validation | Extract `validate_identifier` to `sql_utils.py` | Task 0 |
| P0: SQL injection in entity.py | Parameterized `?` placeholders | Task 1 |
| P0: SQL injection in spatial.py | Parameterized `?` placeholders | Task 2 |
| P0: Error detail leakage | `sanitize_error()` in `sql_utils.py` | Task 4 |
| P1: No cursor timeout | `timeout` param on semaphore.acquire | Task 3 |
| P1: No cursor health check | Replace-on-error in context manager | Task 3 |
| P1: Reconnect race condition | `threading.Lock` around reconnect | Task 4 |
| P1: `/api/catalog` per-request conn | Use shared `_shared_pool` | Task 5 |
| P2: No lifespan try/finally | Wrap yield in try/finally | Task 6 |
| P2: No preserve_insertion_order | SET at startup | Task 6 |
| P2: Duplicated TOOL_SOURCES | Moved to constants.py | Task 7 |
| Test: SQL validation untested | New test_sql_safety.py | Task 8 |
