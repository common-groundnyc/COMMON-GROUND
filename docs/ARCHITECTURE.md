# Architecture

Three tiers, orchestrated by Dagster, served through a single FastMCP process.

## Data flow

```
┌─────────────────────────┐
│  TIER 1 — INGESTION     │   Dagster assets (Docker, Linux forkserver)
│                         │
│  Socrata (287 tables)   │   src/dagster_pipeline/sources/
│  Federal (22 tables)    │   src/dagster_pipeline/defs/*_assets.py
│  Custom scrapers        │
└───────────┬─────────────┘
            │ httpx → parquet
            ▼
┌─────────────────────────┐
│  DuckLake               │   Postgres catalog (public schema)
│  attached AS lake       │   + NVMe parquet on Hetzner
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  TIER 2 — RESOLUTION    │   Splink 4.0, hnsw_acorn, DuckPGQ
│                         │
│  resolved_entities      │   55M records → canonical UUIDs
│  entity_master          │
│  9 materialized views   │
│  8 property graphs      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  TIER 3 — SERVING       │   FastMCP 3.x (infra/duckdb-server/)
│                         │
│  /mcp          (15 super tools, MCP protocol)
│  /api/*        (REST endpoints via @mcp.custom_route)
│  /mosaic/query (sqlglot-validated Mosaic data server)
│  /health
└───────────┬─────────────┘
            │ Cloudflare Tunnel
            ▼
   ┌────────────────────┐
   │ MCP clients         │  Claude, Cursor
   │ common-ground.nyc/   │  Next.js + Mosaic + MapLibre
   │   explore            │
   │ cg-cli (planned)     │
   └────────────────────┘
```

## Top-level directories

### `src/dagster_pipeline/`
Ingestion tier. Pure API clients in `sources/`, Dagster asset wrappers in `defs/`, sensors for inline-flush and freshness. `definitions.py` is the Dagster entry point. 309 Socrata + federal assets, ~40 computed assets, 9 MVs. See `src/dagster_pipeline/AGENTS.md`.

### `infra/duckdb-server/`
Serving tier. Single FastMCP 3.x process registers 15 super tools, REST routes (via `@mcp.custom_route`), and the Mosaic data server. DuckDB access is pooled through `shared/db.py::CursorPool`. Middleware stack: citation, freshness, percentile, response. See `infra/duckdb-server/AGENTS.md`.

### `infra/cg-platform/`
Postgres schema for the future subscription / notification platform (`schema.sql`). Not yet live. See `docs/superpowers/plans/cg-platform-roadmap.md`.

### `tests/`
Project-level pytest suite (Dagster assets, sources). The server-level suite lives at `infra/duckdb-server/tests/` and has different runner deps. See `tests/AGENTS.md`.

### `scripts/`
Ad-hoc helpers: dedup (`dedup_*.py`), investigation (`investigate_*.py`), ingestion one-offs, comments generator. No shared convention.

### `docs/`
- `ARCHITECTURE.md` — this file
- `adr/` — 10 load-bearing decisions
- `runbooks/` — deploy, secrets, known issues
- `superpowers/` — roadmap, phases, audits, plans, specs (the unified planning tree)

### `memory/`
Repo-specific auto-memory, committed to git. Index at `memory/MEMORY.md`.

## Machine-discovered graph (second opinion)

`graphify-out/` (gitignored) holds an AST + semantic knowledge graph of the whole repo: **1614 nodes, 2046 edges, 115 communities** across 320 files. Regenerate with:

```bash
graphify .
open graphify-out/graph.html
```

Top connected abstractions per the graph — these are the actual centers of gravity and match what the ADRs describe:

| Rank | Node | Edges | Referenced ADR |
|---|---|---|---|
| 1 | `CursorPool` | 31 | [0008](adr/0008-cursor-pool.md) |
| 2 | `PercentileMiddleware` | 30 | [0006](adr/0006-fastmcp-for-rest.md) |
| 3 | `OutputFormatterMiddleware` | 29 | [0006](adr/0006-fastmcp-for-rest.md) |
| 4 | `DuckLakeResource` | 23 | [0002](adr/0002-ducklake-catalog-convention.md) |

Read `graphify-out/GRAPH_REPORT.md` for community clustering and surprising connections.

## External dependencies

- **Hetzner** — bare metal in Nuremberg at `178.156.228.119`
- **Cloudflare Tunnel** — all public services at `*.common-ground.nyc`
- **GitHub** — deploy mechanism (`git pull` on the server)

## Production layout on Hetzner

```
/opt/dagster-pipeline/              # this repo, git-tracked
├── docker-compose.yml              # dagster-code, dagster-daemon, dagster-webserver
└── infra/duckdb-server/            # source of truth for the MCP server

/opt/common-ground/                 # legacy directory, runs the production stack
├── docker-compose.yml              # postgres, duckdb-server, duck-ui, cloudflared, xyops
└── duckdb-server → /opt/dagster-pipeline/infra/duckdb-server  # SYMLINK
```

The two compose projects bridge via the `common-ground-shared` external Docker network.
