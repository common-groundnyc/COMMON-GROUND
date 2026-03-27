# Cursor Pool for Concurrent MCP Queries — Design Spec

**Date:** 2026-03-27
**Scope:** Replace global `_db_lock` with a DuckDB cursor pool to enable concurrent query execution.
**Target:** 50 concurrent MCP tool calls with <5% error rate (currently 22.8% at 50).

---

## Problem

The MCP server uses a single `threading.Lock()` (`_db_lock`) that serializes all database queries. FastMCP dispatches sync tools to a threadpool (concurrent), but every tool blocks on this lock, making concurrency useless.

Stress test results (current):

| Concurrency | RPS | P50 | Error Rate |
|-------------|-----|-----|------------|
| 10 | 3.5 | 806ms | 0% |
| 20 | 6.7 | 383ms | 20% |
| 50 | 14.6 | 1.6s | 22.8% |

The errors at 20+ concurrent are caused by lock contention + request timeouts.

## DuckDB Concurrency Model

From the [DuckDB docs](https://duckdb.org/docs/stable/guides/python/multiple_threads):

- One parent connection, multiple cursors via `.cursor()`
- Each cursor is a lightweight sub-connection that can run one query at a time
- Multiple cursors from one connection can run queries in parallel
- MVCC handles read isolation automatically
- No external locking needed for concurrent reads
- DuckDB cursors are stateless between `.execute()` calls — a new execute discards any prior result

Pattern from DuckDB docs:
```python
duckdb_con = duckdb.connect('my_db.duckdb')

def query_from_thread(duckdb_con):
    local_con = duckdb_con.cursor()
    results = local_con.execute("SELECT ...").fetchall()
```

## FastMCP Threading Model

From [FastMCP docs](https://gofastmcp.com/servers/tools):

- Sync tools automatically run in a threadpool
- Multiple tool calls execute concurrently in separate threads
- The event loop is never blocked
- Each tool invocation runs in its own thread

This means FastMCP already provides the concurrency — the `_db_lock` is the only thing preventing it from working.

---

## Design

### New module: `cursor_pool.py`

A thread-safe pool of DuckDB cursors with context manager interface.

```python
class CursorPool:
    def __init__(self, conn, size=8):
        self._conn = conn
        self._semaphore = threading.Semaphore(size)
        self._cursors = [conn.cursor() for _ in range(size)]
        self._available = queue.Queue()
        for c in self._cursors:
            self._available.put(c)

    @contextmanager
    def cursor(self):
        self._semaphore.acquire()
        cur = self._available.get()
        try:
            yield cur
        finally:
            self._available.put(cur)
            self._semaphore.release()

    def execute(self, sql, params=None):
        """Convenience: acquire cursor, execute, return (cols, rows)."""
        with self.cursor() as cur:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = result.fetchmany(MAX_QUERY_ROWS)
            return cols, rows
```

No cursor "reset" needed — DuckDB cursors are stateless between `.execute()` calls.

### Pool size: 8

- Starting point, not inherently tied to `SET threads = 8`
- DuckDB's `threads = 8` controls intra-query parallelism; pool size controls inter-query concurrency — these are different dimensions
- Lightweight queries (e.g., `SELECT * FROM pctile_buildings WHERE bbl = ?`) use 1-2 DuckDB threads, so 8 such queries can run without contention
- Heavy queries (e.g., `entity_xray`) may try to use all 8 DuckDB threads, creating resource competition
- The semaphore ensures graceful queuing instead of errors when >8 concurrent
- Can be increased to 12-16 later if lightweight query patterns dominate

### Integration into `mcp_server.py`

**Lifespan:** Create pool after connection setup, before tools are served:
```python
pool = CursorPool(conn, size=8)
yield {"db": conn, "pool": pool, "catalog": catalog, ...}
```

**`_execute()` function:** Change from lock-based to pool-based:
```python
# Before:
def _execute(conn, sql, params=None):
    with _db_lock:
        cur = conn.execute(sql, params or [])
        ...

# After:
def _execute(pool, sql, params=None):
    with pool.cursor() as cur:
        result = cur.execute(sql, params or [])
        ...
```

**Tool functions:** Change `db = ctx.lifespan_context["db"]` calls to use pool:
```python
# Before:
db = ctx.lifespan_context["db"]
cols, rows = _execute(db, sql, params)

# After:
pool = ctx.lifespan_context["pool"]
cols, rows = _execute(pool, sql, params)
```

**Direct `_db_lock` usages in tools:** Some tools acquire the lock directly (not through `_execute`). These change to `with pool.cursor() as cur:`.

### `_db_lock` usage breakdown (19 lines)

**Startup-only (stay on parent `conn`, remove lock):**
- Line 559: `_build_explorations()` — runs before `yield`
- Line 2670: `build_percentile_tables()` — runs before `yield`
- Line 2677: `build_lake_percentile_tables()` — runs before `yield`

**Background embed thread (give dedicated cursor):**
- Lines 1124, 1166, 1197: Embedding functions run in a daemon thread *after* `yield`. Currently use `_db_lock` with parent `conn`. Since DuckDB MVCC handles concurrent reads safely, these can use a dedicated cursor (`conn.cursor()`) without any lock. The embed thread only reads lake tables (SELECTs) and writes to Lance (external), never to DuckDB tables.

**Runtime tool queries (migrate to pool) — 12 usages:**
- Line 605: `_execute()` function — the main one, all tools go through this
- Line 2869: `sql_admin` — DDL (CREATE VIEW). Works through cursors since views are metadata.
- Line 3186: `data_catalog` Lance search
- Line 3245: `export_data`
- Line 3375: `text_search` Lance search
- Line 3458: `semantic_search`
- Line 4119: `gentrification_tracker` — **temp table hazard, see Risks**
- Line 8035: `similar_buildings` Lance lookup
- Line 8065: `similar_buildings` KNN search
- Line 10557: `fuzzy_entity_search`
- Line 12085: `person_crossref` Lance fallback

**Remove entirely:**
- Line 68: `_db_lock = threading.Lock()` declaration
- Line 2741: Lifespan context `"lock": _db_lock` — replace with `"pool": pool`

### `percentile_middleware.py` + `percentiles.py` migration

The percentile middleware retrieves `lock` from lifespan context and passes it to `lookup_percentiles()` and `get_population_count()`:

```python
# Current (percentile_middleware.py line 106):
lock = lifespan.get("lock")
percentiles = lookup_percentiles(db, entity_type, key, lock=lock)

# After:
pool = lifespan.get("pool")
percentiles = lookup_percentiles(pool, entity_type, key)
```

`percentiles.py` functions `lookup_percentiles()` and `get_population_count()` change from `(db, ..., lock=None)` to `(pool, ...)`:
```python
# Before:
def lookup_percentiles(db, entity_type, key, lock=None):
    with lock:
        rows = db.execute(f"SELECT * FROM main.{table} WHERE {key_col} = ?", [key]).fetchall()

# After:
def lookup_percentiles(pool, entity_type, key):
    with pool.cursor() as cur:
        rows = cur.execute(f"SELECT * FROM main.{table} WHERE {key_col} = ?", [key]).fetchall()
```

The `build_percentile_tables()` and `build_lake_percentile_tables()` functions keep their current signature (they run at startup on the parent `conn`, not through the pool).

### What stays on the parent connection (no pool)

These run during startup, before any tools are served:
- Graph table builds (lines 1206-2412)
- Percentile table builds (`build_percentile_tables`, `build_lake_percentile_tables`)
- JSON view creation
- DuckLake configuration (SET, CALL, ALTER TABLE)
- Extension loading
- `_build_explorations()` (pre-computes highlights)

### Background embedding thread

The embed thread starts after `yield` (line 2718) and runs concurrently with tool queries. It currently uses `_db_lock` with the parent `conn` for SELECT queries.

**Migration:** Give the embed thread a dedicated cursor (`embed_cursor = conn.cursor()`). Remove `_db_lock` from embed functions. The embed thread only does:
- SELECT from lake tables (reads — safe via MVCC)
- COPY TO Lance files (external I/O — no DuckDB conflict)
- CREATE TEMP TABLE / DROP TABLE for staging (uses the cursor's own temp namespace)

### `_catalog_connect()` unchanged

The `/api/catalog` HTTP endpoint already creates its own connection per request. No pool needed.

---

## Changes Summary

| File | Change |
|------|--------|
| `cursor_pool.py` (new) | ~50 lines: CursorPool class with semaphore + queue |
| `mcp_server.py` | Migrate 12 runtime `_db_lock` usages to pool; remove 3 startup locks; give embed thread a cursor; update `_execute()` signature; create pool in lifespan; replace `"lock"` with `"pool"` in context |
| `percentiles.py` | Change `lookup_percentiles()` and `get_population_count()` from `(db, ..., lock)` to `(pool, ...)` |
| `percentile_middleware.py` | Get `pool` from lifespan context instead of `db` + `lock` |
| `tests/test_cursor_pool.py` (new) | Thread safety tests, pool exhaustion test, context manager test |
| `stress_test.py` | Re-run to validate improvement |

## Files NOT Changed

| File | Reason |
|------|--------|
| `entity.py`, `spatial.py`, `quality.py` | Return SQL strings — don't touch DB |
| `formatters.py`, `response_middleware.py` | No DB access |
| `Dockerfile`, `start.sh`, `deploy.sh` | No infra changes |
| `warmup.py`, `server.py` | Separate connections, not affected |
| `csv_export.py`, `xlsx_export.py`, `source_links.py` | No DB access |

## Success Criteria

Run `stress_test.py --phase ramp` after implementation:

| Concurrency | Target RPS | Target Error Rate |
|-------------|------------|-------------------|
| 10 | >5 | 0% |
| 20 | >10 | <2% |
| 50 | >20 | <5% |

## Risks

1. **Temp table collision in `gentrification_tracker` (line 4119).** This tool creates temp tables (`_gent_tmp`, `_gent_{signal}`) that are scoped to the connection, not the cursor. Two concurrent calls would race on the same temp table names. **Mitigation:** Append a UUID to temp table names per invocation: `_gent_tmp_{uuid.uuid4().hex[:8]}`.

2. **DuckLake compactor:** DuckLake may trigger background compaction on any query. If this happens on a cursor while another cursor is querying, it could cause conflicts. **Mitigation:** compaction is already disabled in the lifespan (`enable_compaction = false`).

3. **`sql_admin` is a runtime write operation.** It runs `CREATE OR REPLACE VIEW` through a pool cursor. DuckDB handles view DDL safely across cursors since views are catalog metadata, not data. No special handling needed, but noted here for awareness.

4. **Pool exhaustion under extreme load:** At >8 concurrent, queries queue on the semaphore. If queries are slow (e.g., `entity_xray` at 27s), the queue grows. **Mitigation:** FastMCP's own timeout handling will return errors before the queue becomes unbounded.
