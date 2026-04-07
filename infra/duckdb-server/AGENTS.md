# infra/duckdb-server — Agent Notes

This is the **serving tier**. A single FastMCP 3.x process serves:

- **MCP protocol** at `/mcp` — 15 super tools, exposed to AI clients (Claude, Cursor)
- **REST endpoints** at `/api/*` — via `@mcp.custom_route`
- **Mosaic data server** at `/mosaic/query` — sqlglot-validated SQL for the explore dashboard
- **Health** at `/health`

All reachable at `https://mcp.common-ground.nyc/` via Cloudflare Tunnel.

## Layout

```
infra/duckdb-server/
├── server.py                # DuckDB + DuckLake attach, extension loading
├── mcp_server.py            # FastMCP entrypoint; registers super tools and @mcp.custom_route REST handlers
├── Dockerfile
├── tools/                   # Super tool implementations (one file per tool)
│   (query, entity, building, network, semantic_search, address_report,
│    anomalies, civic, health, legal, neighborhood, safety, school,
│    services, transit, suggest, nl_query)
├── routes/                  # @mcp.custom_route REST handlers
│   ├── explore.py           # /api/neighborhood, /api/zips/search, /api/buildings/worst
│   └── mosaic_route.py      # /mosaic/query (sqlglot AST validator)
├── shared/
│   ├── db.py                # CursorPool, execute(pool, sql, params) → (cols, rows)
│   ├── explore_queries.py   # Pure SQL builders for REST endpoints
│   ├── graph.py             # DuckPGQ property graph cache
│   └── vector_search.py     # hnsw_acorn wrapper (was lance.py, renamed 2026-04-07)
├── middleware/              # citation, freshness, percentile, response
└── tests/                   # pytest suite (~50+ tests)
```

## Core contracts

### CursorPool

All DuckDB access goes through `shared/db.py::CursorPool`. **Never** open connections directly.

```python
from shared.db import execute
import asyncio

cols, rows = await asyncio.to_thread(execute, pool, sql, params)
```

- `execute(pool, sql, params) → (cols, rows)` — always this tuple shape.
- **Always wrap in `asyncio.to_thread`** — DuckDB calls are blocking.
- Timeout, cursor replacement, and close handling are built in (commit `eb0ea71`).

### DuckLake catalog convention

DuckLake is attached `AS lake`. Reference tables as `lake.<schema>.<table>`. The historic `METADATA_SCHEMA='lake'` was retired 2026-04-07 — postgres metadata now lives in the default `public` schema, but the ATTACH alias is still `lake`. See `@docs/adr/0002-ducklake-catalog-convention.md`.

### Adding a REST endpoint

1. Create a handler in `routes/my_route.py`:
   ```python
   from fastmcp import FastMCP
   from starlette.requests import Request
   from starlette.responses import JSONResponse

   def register(mcp: FastMCP, pool):
       @mcp.custom_route("/api/my-endpoint", methods=["GET"])
       async def handler(request: Request):
           cols, rows = await asyncio.to_thread(execute, pool, "SELECT 1", ())
           return JSONResponse({"cols": cols, "rows": rows})
   ```
2. Register in `mcp_server.py`:
   ```python
   from routes import my_route
   my_route.register(mcp, pool)
   ```
3. Add a test in `tests/test_my_route.py` (see `tests/test_explore_routes.py` for pattern).

### Adding a super tool

1. Create `tools/my_tool.py` exposing a function.
2. Register it in `tools/__init__.py` and `mcp_server.py` with `@mcp.tool()`.
3. Add a smoke test in `tests/`.

### Mosaic sqlglot validator

`/mosaic/query` parses incoming SQL via `sqlglot.parse(sql, dialect="duckdb")`, walks the AST, and rejects:

- Anything outside the schema allowlist
- Any non-`SELECT` / non-`UNION` statement
- Tables whose name starts with `_` (internal)

Real column names from `execute()`'s cols list are used in the response. See `@docs/adr/0007-sqlglot-mosaic-validator.md`.

## Deploy

See `@docs/runbooks/deploy-mcp-server.md`. Summary: `git push` → `ssh` → `git pull` on the server → `docker compose build duckdb-server && docker compose up -d duckdb-server`.

## See also

- `@docs/adr/0006-fastmcp-for-rest.md`
- `@docs/adr/0008-cursor-pool.md`
- `@docs/runbooks/known-issues.md`
