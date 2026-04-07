# MCP CursorPool contract

All DuckDB access inside `infra/duckdb-server/` goes through `shared/db.py::CursorPool`. Never open DuckDB connections directly in route handlers or tool implementations.

## The contract

```python
from shared.db import execute
import asyncio

cols, rows = await asyncio.to_thread(execute, pool, sql, params)
```

- **Signature:** `execute(pool, sql, params) → (cols, rows)` — always this tuple.
- **Always wrap in `asyncio.to_thread`.** DuckDB calls are blocking; running them on the event loop blocks all concurrent requests.
- `cols` is a list of column names from `cursor.description`.
- `rows` is a list of tuples (not dicts — serialization happens in the route).

## Why

- Thread-safe: each cursor has its own connection.
- Timeouts, cursor replacement on error, and `close()` are built in.
- Stress-tested to 0% errors at 50 concurrent (2026-03-27).

## See also

- `docs/adr/0008-cursor-pool.md`
- `infra/duckdb-server/shared/db.py`
