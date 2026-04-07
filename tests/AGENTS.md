# tests — Agent Notes

Two test trees exist in this repo, with different runners and dependencies.

## 1. Project-level `tests/`

Covers Dagster assets, sources, and resources. Run with:

```bash
uv run pytest tests/ -v
```

## 2. Server-level `infra/duckdb-server/tests/`

Covers FastMCP routes, middleware, SQL validators, and DuckDB integration. **Requires extra dependencies** via `uv run --with`:

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v
```

## Conventions

- **No mocking of DuckDB.** Use a real in-memory DB (`duckdb.connect(":memory:")`) for determinism. Mocks hide real bugs — prior incidents.
- **Fixtures** live next to the tests that use them; no global `conftest.py` sprawl.
- **One assertion concept per test.** Table-driven style when covering many inputs.
- **Never commit `.duckdb` files** produced during tests (`.claudeignore` excludes them).

## See also

- `@docs/adr/0008-cursor-pool.md` — test isolation via CursorPool
