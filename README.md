# Common Ground Data Pipeline

Civic intelligence data platform for NYC. Ingests open data (Socrata, federal, custom scrapers), resolves entities with Splink, builds DuckPGQ property graphs, serves it all through a FastMCP server at `mcp.common-ground.nyc` and a public dashboard at [common-ground.nyc/explore](https://common-ground.nyc/explore).

**~352 tables · ~568M rows · 14 schemas · 15 MCP super tools · 8 property graphs**

## Quickstart

```bash
# Always run Dagster in Docker (macOS forkserver OOMs otherwise)
docker compose up -d
docker compose logs -f dagster-daemon
open http://localhost:3000      # Dagster UI
```

Server-side tests:

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v
```

## Repo map

| Path | What's there |
|---|---|
| `src/dagster_pipeline/` | Dagster assets, sources, definitions — the ingestion tier |
| `infra/duckdb-server/` | FastMCP server (MCP tools + REST + Mosaic data server) — the serving tier |
| `infra/cg-platform/` | Postgres schema for the future subscription/notification platform |
| `scripts/` | Dedup, ingestion, investigation helpers |
| `tests/` | Project-level pytest suite |
| `docs/superpowers/` | GSD roadmap, phase plans, audits, strategic plans |
| `docs/superpowers/` | Design specs and implementation plans |
| `docker-compose.yml` | dagster-code, dagster-webserver, dagster-daemon |

## Key docs

- **[AGENTS.md](./AGENTS.md)** — agent onboarding (stack, commands, operating principles, deploy, known issues). `CLAUDE.md` is a symlink to this.
- **[docs/superpowers/ROADMAP.md](./docs/superpowers/ROADMAP.md)** — phases and milestones
- **[docs/superpowers/STATE.md](./docs/superpowers/STATE.md)** — current phase
- **[docs/superpowers/plans/cg-platform-roadmap.md](./docs/superpowers/plans/cg-platform-roadmap.md)** — CG platform v0.2+ (notifications, Telegram bot, CLI)

## Deploy

Server code deploys via `git push` → `ssh` → `git pull` on Hetzner. See [AGENTS.md → Deploy](./AGENTS.md) for the full sequence. The website lives in a separate repo (`common-ground-website`) and deploys to Cloudflare Workers.

## Gotchas

- **Never run `dagster dev` locally** — always Docker. macOS forkserver OOMs at 60 GB.
- **Never bypass Dagster** with ad-hoc ingestion scripts.
- **DuckLake is attached `AS lake`** — reference tables as `lake.<schema>.<table>`.
- **`lake.housing.hpd_violations`** has some orphan parquet refs — filtered queries fail, counts work. See AGENTS.md.
- **Secrets** live in SOPS-encrypted `.env.secrets.enc`. Never read `*.secrets.toml` files.

## Stack

Dagster 1.12 · DuckDB 1.5 + DuckLake · Postgres · Splink 4 · hnsw_acorn · DuckPGQ · FastMCP 3 · sqlglot · Hetzner + Cloudflare Tunnel
