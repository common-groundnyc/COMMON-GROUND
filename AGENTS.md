# Common Ground Data Pipeline

Civic intelligence data platform for NYC. Ingests open data, resolves entities probabilistically, builds property graphs, and serves it all through a FastMCP server and public dashboard at common-ground.nyc/explore.

## Golden rules

1. **Dagster always runs in Docker** — never `dagster dev` locally. macOS forkserver OOMs at 60 GB.
2. **Never bypass Dagster** — all ingestion through `@asset` definitions, so runs appear in the UI.
3. **DuckLake is attached `AS lake`** — reference tables as `lake.<schema>.<table>`.
4. **MCP server is the interface** — clients hit it via Cloudflare Tunnel; do not expose Postgres or DuckDB directly.
5. **Git-based deploy** — `git push` locally, `git pull` on the server. No rsync.
6. **REST routes share the FastMCP server** — new HTTP endpoints live in `infra/duckdb-server/routes/*.py` and use `@mcp.custom_route`.
7. **Mosaic is sqlglot-validated** — `/mosaic/query` AST-walks and allowlists.
8. **Never read `*.secrets.toml`** — use SOPS-encrypted `.env.secrets.enc`. See `@docs/runbooks/secrets-sops.md`.

## Stack

- **Dagster 1.12.x** (Docker, Linux forkserver) — orchestration, sensors, schedules
- **DuckDB 1.5.x + DuckLake** — attached `AS lake`, postgres catalog in `public`
- **Postgres** — DuckLake metadata + future `cg` platform schema
- **Splink 4.0** — probabilistic entity resolution
- **hnsw_acorn** (DuckDB community extension) — ACORN-1 HNSW vector search
- **DuckPGQ** — property graph queries
- **FastMCP 3.x** — MCP protocol + REST + Mosaic (single process)
- **sqlglot** — Mosaic SQL AST validator

## Repo map

| Path | Agent notes |
|---|---|
| `src/dagster_pipeline/` | Ingestion tier — see `@src/dagster_pipeline/AGENTS.md` |
| `infra/duckdb-server/` | Serving tier (MCP + REST + Mosaic) — see `@infra/duckdb-server/AGENTS.md` |
| `infra/cg-platform/` | Postgres schema for subscriptions/notifications |
| `tests/` | Two test trees — see `@tests/AGENTS.md` |
| `scripts/` | Dedup, investigation, one-off helpers (no shared conventions) |
| `docs/ARCHITECTURE.md` | Codemap and dependency diagram |
| `docs/adr/` | 10 load-bearing decisions |
| `docs/runbooks/` | Deploy, secrets, known issues |
| `docs/superpowers/` | Roadmap, phases, audits, plans, specs |
| `memory/` | Repo-specific gotchas (committed) |

## Commands

```bash
# Start the full Dagster stack
docker compose up -d
docker compose logs -f dagster-daemon

# Materialize an asset from inside the container
docker compose exec dagster-code dagster asset materialize \
  --select 'health/restaurant_inspections' -m dagster_pipeline.definitions

# Run server tests (requires the --with deps)
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v

# Deploy — see @docs/runbooks/deploy-mcp-server.md
```

## Progressive context

Load on demand when the task requires them:

- `@docs/ARCHITECTURE.md` — codemap
- `@docs/runbooks/deploy-mcp-server.md` — MCP server deploy
- `@docs/runbooks/deploy-dagster.md` — Dagster deploy
- `@docs/runbooks/secrets-sops.md` — secrets handling
- `@docs/runbooks/known-issues.md` — current known issues
- `@docs/adr/` — 10 load-bearing decisions (Nygard format)

## Where to look

- **Current phase / state** — `docs/superpowers/STATE.md`, `docs/superpowers/ROADMAP.md`
- **Project vision** — `docs/superpowers/PROJECT.md`
- **In-flight plans** — `docs/superpowers/plans/`
- **Design specs** — `docs/superpowers/specs/`
- **Repo memory** — `memory/MEMORY.md`
