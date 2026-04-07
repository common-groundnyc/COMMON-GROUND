# 0008 — CursorPool for thread-safe DuckDB access

**Status:** Accepted (2026-03-27, hardened commit `eb0ea71`)

## Context

DuckDB connections are not safe across threads, and the FastMCP server handles many concurrent requests. A naive "one connection per request" pattern broke at 50 concurrent under stress testing. A global lock serialized everything.

## Decision

Introduce `shared/db.py::CursorPool`:

- Fixed-size pool of cursors, each wrapping its own DuckDB connection
- `execute(pool, sql, params) → (cols, rows)` — synchronous, used inside `asyncio.to_thread`
- Per-cursor timeout; cursor replacement on error; proper `close()` on shutdown

Callers always wrap:
```python
cols, rows = await asyncio.to_thread(execute, pool, sql, params)
```

## Consequences

- **+** 0% error rate at 50 concurrent under stress (2026-03-27)
- **+** Clean separation: sync SQL, async HTTP
- **+** Deterministic timeouts and cursor replacement
- **−** Fixed pool size must be tuned per host
- **−** All callers must remember the `asyncio.to_thread` wrapper
