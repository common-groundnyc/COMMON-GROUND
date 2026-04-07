# Common Ground Data Pipeline

Civic intelligence data platform: ingests NYC + NYS + federal open data, resolves entities probabilistically, builds property graphs, and serves it all through an MCP server, REST endpoints, a Mosaic data server, and a public dashboard at common-ground.nyc/explore.

## Three-Tier Architecture

```
TIER 1 — INGESTION                   TIER 2 — RESOLUTION              TIER 3 — SERVING
─────────────────                    ──────────────────                ────────────────
Socrata (287 tables)                 Splink ER (55M records)           FastMCP server
Federal sources (22 tables)   ───►   HNSW ACORN-1 embeddings    ───►   ├─ /mcp           (15 super tools, MCP protocol)
Custom scrapers (NYPD/OCA/...)       Entity master (UUIDs)             ├─ /api/*         (REST endpoints)
                ↓                                ↓                      ├─ /mosaic/query  (Mosaic data server, sqlglot-validated)
        DuckLake (Postgres catalog                                     └─ /health
        + local NVMe parquet on Hetzner)                                       │
                                                                               ▼
                                                                Cloudflare Tunnel ──► users
                                                                                       ├─ MCP clients (Claude, Cursor)
                                                                                       ├─ common-ground.nyc/explore (Next.js + Mosaic)
                                                                                       └─ cg-cli (planned)
```

All orchestrated by **Dagster** (Docker), running on a Hetzner server in Nuremberg.

## Stack

- **Dagster 1.12.x** (Docker, Linux forkserver) — orchestration, sensors, scheduling, UI
- **Direct DuckDB ingestion** — sources fetch via httpx and write straight to DuckLake (dlt removed 2026-03-26)
- **DuckDB 1.5.x + DuckLake** — catalog attached AS `lake` with metadata stored in postgres `public` schema (the historic METADATA_SCHEMA='lake' was dropped 2026-04-07; tables are still accessed as `lake.<schema>.<table>` because that's the ATTACH alias)
- **Postgres** — DuckLake metadata catalog (single instance `common-ground-postgres-1`, also hosts the future `cg` database for subscriptions/notifications via `infra/cg-platform/schema.sql`)
- **Splink 4.0** — probabilistic entity resolution
- **`hnsw_acorn`** (DuckDB community extension) — ACORN-1 filtered HNSW vector index for semantic search; embeddings in `main.*` tables, MiniLM-L6-v2 (384-dim, INT8) via ONNX Runtime; Parquet cache pattern (no reliance on experimental HNSW persistence)
- **DuckPGQ** — property graph queries on DuckDB
- **FastMCP 3.x** — MCP server, exposing 15 super tools to AI clients AND custom REST routes via `@mcp.custom_route`
- **sqlglot** — AST-based SQL allowlist validator for the `/mosaic/query` endpoint

## Data Coverage (~352 tables, ~568M rows, 14 schemas)

**Socrata: 287 tables / 11 schemas**
business · city_government · education · environment · financial · health · housing · public_safety · recreation · social_services · transportation

**Federal / state / custom: ~22 tables**
BLS · Census ACS + ZCTA · HUD · FEC · EPA ECHO · CourtListener · LittleSis · USASpending · NREL · NYPD misconduct · NYS BOE · NYS death index · marriage index · OCA housing court · Urban Institute · College Scorecard

**Computed (~40 tables/MVs in `foundation` + `main`)**
name_index · resolved_entities · entity_master · h3_index · phonetic_index · row_fingerprints · address_lookup · 9 materialized views (building_hub, acris_deeds, zip_stats, crime_precinct, corp_network, entity_acris, city_averages, pctile_violations, pctile_311) · spatial_views · **geo_zip_boundaries** (NYC MODZCTA polygons, 178 ZIPs)

## Project Structure

```
dagster-pipeline/
├── docs/superpowers/                      # Project management — see README below
│   ├── PROJECT.md                  # Vision, requirements, current state audit
│   ├── ROADMAP.md                  # v1.0/v2.0/v3.0 phase breakdown (21 phases)
│   ├── STATE.md                    # Current phase + progress
│   ├── phases/                     # 30 phase execution plans
│   ├── audits/                     # Cross-functional audits
│   └── plans/                      # Strategic initiative plans (incl. CG platform expansion)
├── docs/superpowers/
│   ├── specs/                      # Design specs (e.g. /explore dashboard)
│   └── plans/                      # Implementation plans for executing-plans/subagent-driven-development
├── src/dagster_pipeline/
│   ├── definitions.py              # Dagster Definitions, jobs, schedules, sensors
│   ├── sources/                    # API clients & extractors (socrata, bls, census, hud, fec, ...)
│   └── defs/                       # Dagster assets:
│       ├── socrata_direct_assets.py    # 287 Socrata assets
│       ├── federal_direct_assets.py    # 22 federal assets
│       ├── name_index_asset.py         # ER preprocessing
│       ├── resolved_entities_asset.py  # Splink clustering
│       ├── entity_master_asset.py      # Canonical entity table
│       ├── foundation_assets.py        # h3, phonetic, fingerprints
│       ├── materialized_view_assets.py # 9 MVs
│       ├── spatial_views_asset.py
│       ├── address_lookup_asset.py
│       ├── geo_zip_boundaries_asset.py # NYC MODZCTA polygons (annual refresh)
│       ├── quality_assets.py
│       ├── flush_sensor.py             # Inline → parquet flush
│       └── freshness_sensor.py         # Hourly source-vs-lake freshness check
├── infra/
│   ├── deploy.sh                       # rsync helper (LEGACY — github deploy is now primary)
│   ├── hetzner-docker-compose.yml      # full production stack reference
│   ├── init-db.sql
│   ├── cg-platform/
│   │   └── schema.sql                  # Postgres schema for the future subscription/notification platform
│   └── duckdb-server/                  # MCP + HTTP server (deployed to Hetzner)
│       ├── server.py                   # DuckDB + DuckLake attach
│       ├── mcp_server.py               # FastMCP entrypoint, 15 super tools, 8 property graphs, custom_route block
│       ├── Dockerfile
│       ├── tools/                      # Super tool implementations
│       │   (query, entity, building, network, semantic_search, address_report,
│       │    anomalies, civic, health, legal, neighborhood, safety, school,
│       │    services, transit, suggest, nl_query)
│       ├── routes/                     # @mcp.custom_route handlers (REST endpoints)
│       │   ├── explore.py              # /api/neighborhood, /api/zips/search, /api/buildings/worst
│       │   └── mosaic_route.py         # /mosaic/query (sqlglot AST validator + schema allowlist)
│       ├── shared/
│       │   ├── db.py                   # CursorPool, execute(pool, sql, params) → (cols, rows)
│       │   ├── explore_queries.py      # Pure SQL builders for the explore REST endpoints
│       │   ├── graph.py                # Property graph cache
│       │   └── ...
│       ├── middleware/                 # citation, freshness, percentile, response
│       └── tests/                      # pytest suite (~28 explore + existing tests)
│           (test_explore_queries, test_explore_routes, test_mosaic_route,
│            test_anomalies, test_csv_export, test_middleware, ...)
├── scripts/                            # Dedup, ingestion, investigation, comments
├── docker-compose.yml                  # dagster-code, dagster-webserver, dagster-daemon (joins common-ground-shared external network)
└── pyproject.toml
```

## Property Graphs (DuckPGQ, built at MCP startup)

| Graph | Domain |
|---|---|
| housing | landlord ownership, complaints, violations |
| building_network | co-owners, shared addresses |
| transaction_network | ACRIS buyers/sellers, deed chains |
| corporate_web | NYS corps, officers, addresses |
| influence_network | campaign donors, lobbyists, recipients |
| contractor_network | permit holders, violations |
| officer_network | corporate officers, board positions |
| tradewaste_network | waste haulers, violations, licensees |

## Project State (see `docs/superpowers/`)

- ✅ **v1.0 Entity Resolution** (8 phases, shipped 2026-03-17) — Splink clustering of 55.5M records
- ✅ **v2.0 DuckPGQ Graph Rebuild** (10 phases, shipped 2026-03-26) — 8 property graphs rebuilt
- ✅ **CG Platform v0.1 — /explore dashboard** (shipped 2026-04-07) — 20-task TDD plan, server REST endpoints + Mosaic data server + Next.js dashboard at `common-ground.nyc/explore`. See `docs/superpowers/specs/2026-04-07-cg-explore-dashboard-design.md` and `docs/superpowers/plans/cg-platform-roadmap.md`.
- 🚧 **v3.0 UX Innovation & Intelligence** (11 phases planned, ~50% milestone progress) — currently Phase 11: Entity Master
- ⏳ **CG Platform v0.2 — notification engine + Telegram bot + CLI** (planned) — see `docs/superpowers/plans/cg-platform-roadmap.md` and the 5 sub-plans (`cg-notification-engine-design.md`, `cg-telegram-bot-architecture.md`, etc.)

## Hetzner Server (178.156.228.119)

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
```

### Public services (via Cloudflare Tunnel)

| URL | Backend | Notes |
|---|---|---|
| `https://mcp.common-ground.nyc/mcp` | FastMCP server, port 4213 | MCP protocol — 15 super tools |
| `https://mcp.common-ground.nyc/health` | duckdb-server | Liveness probe |
| `https://mcp.common-ground.nyc/api/catalog` | duckdb-server | Lake catalog (schemas, tables, row counts) |
| `https://mcp.common-ground.nyc/api/table-meta?table=schema.name` | duckdb-server | Single-table schema + column metadata |
| `https://mcp.common-ground.nyc/api/query` | duckdb-server | Ad-hoc SQL query (POST) |
| `https://mcp.common-ground.nyc/api/anomalies` | duckdb-server | Anomaly detector |
| `https://mcp.common-ground.nyc/api/neighborhood/{zip}` | duckdb-server (routes/explore.py) | Neighborhood stat-card payload (HPD/311/NYPD/DOHMH) |
| `https://mcp.common-ground.nyc/api/zips/search?q=...` | duckdb-server (routes/explore.py) | ZIP autocomplete from `lake.foundation.geo_zip_boundaries` |
| `https://mcp.common-ground.nyc/api/buildings/worst?zip=...` | duckdb-server (routes/explore.py) | Top-N worst-buildings ranking |
| `https://mcp.common-ground.nyc/mosaic/query` | duckdb-server (routes/mosaic_route.py) | Mosaic data server, JSON SQL POST, sqlglot AST validation |
| `https://duckdb.common-ground.nyc` | duck-ui container | Browser SQL UI |
| `https://dagster.common-ground.nyc` | dagster-webserver | Dagster UI (Cloudflare Access gated) |
| `https://common-ground.nyc/` | common-ground-website (Cloudflare Workers) | Marketing/manifesto site |
| `https://common-ground.nyc/explore` | common-ground-website | The /explore dashboard (Mosaic + MapLibre + REST) |
| `https://common-ground.nyc/explore/table` | common-ground-website | Legacy data table browser (preserved) |

### Container layout (`/opt/`)

```
/opt/dagster-pipeline/             # this repo, git-tracked, deployed via `git pull`
├── docker-compose.yml             # dagster-code, dagster-daemon, dagster-webserver
└── infra/duckdb-server/           # MCP server source (the canonical source of truth)

/opt/common-ground/                # legacy directory, hosts the rest of the production stack
├── docker-compose.yml             # postgres, duckdb-server, duck-ui, pg-backup, xyops, cloudflared
└── duckdb-server -> /opt/dagster-pipeline/infra/duckdb-server   # SYMLINK to the git tree
```

The duckdb-server container is built from the symlink, which resolves to the git tree. **`git pull` is now the deploy mechanism for the MCP server source.** Networks: the dagster containers (in `/opt/dagster-pipeline/`) and the production stack (in `/opt/common-ground/`) bridge via the `common-ground-shared` external docker network.

## Credentials

SOPS + age. Encrypted file `.env.secrets.enc` is committed; `.env.secrets` is gitignored.

```bash
sops --input-type dotenv --output-type dotenv .env.secrets.enc           # edit
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets   # decrypt
```

## How to Run

**ALWAYS run Dagster in Docker** (Linux forkserver — avoids macOS 60GB OOM crashes):

```bash
cd ~/Desktop/dagster-pipeline
docker compose up -d                  # dagster-code, webserver, daemon
docker compose logs -f dagster-daemon
```

Materialize from CLI inside the container:

```bash
docker compose exec dagster-code dagster asset materialize \
  --select 'health/restaurant_inspections' -m dagster_pipeline.definitions
```

### Deploy

**Server source (MCP + REST + Mosaic):**

```bash
# 1. Commit + push locally
git add infra/duckdb-server/...
git commit -m "..."
git push origin main

# 2. Pull on the server (the symlink picks it up)
ssh fattie@178.156.228.119
cd /opt/dagster-pipeline && git pull
cd /opt/common-ground && sudo docker compose build duckdb-server && sudo docker compose up -d duckdb-server
```

**Dagster code containers** (in `/opt/dagster-pipeline/`):

```bash
ssh fattie@178.156.228.119
cd /opt/dagster-pipeline && git pull
docker compose up -d --build dagster-code
```

**Website** (separate repo `common-ground-website`): `npm run deploy` (opennextjs-cloudflare). The website lives in its own GitHub repo and deploys directly to Cloudflare Workers.

The legacy `infra/deploy.sh` (rsync) still works but is no longer the primary path.

## Tests

Server-side pytest (~28 explore-related + existing suite, ~50+ total):

```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v
```

The website repo runs vitest (component unit tests) + Playwright e2e (`/explore` happy path) — see that repo's package.json.

## Key Operating Principles

1. **Dagster always** — never bypass with ad-hoc scripts. All ingestion through Dagster assets so runs are tracked.
2. **Dagster in Docker always** — never run `dagster dev` locally on macOS.
3. **Catalog convention** — DuckLake is attached `AS lake`; reference tables as `lake.<schema>.<table>`. Postgres metadata lives in the default `public` schema (the historic `METADATA_SCHEMA='lake'` was retired 2026-04-07).
4. **MCP server is the user interface** — clients hit it via Cloudflare Tunnel; do not expose Postgres or DuckDB directly. The same `infra/duckdb-server/` container serves the MCP protocol AND the REST endpoints AND the Mosaic data server (all via `@mcp.custom_route` on the existing FastMCP instance).
5. **GitHub-based deploy** — server code lives in this repo, deploys via `git push` → `git pull` on Hetzner. The `/opt/common-ground/duckdb-server` directory is a symlink to `/opt/dagster-pipeline/infra/duckdb-server/`.
6. **REST routes share the FastMCP server** — new HTTP endpoints live in `infra/duckdb-server/routes/*.py` and are registered in `mcp_server.py` via `@mcp.custom_route(...)`. They reuse `shared/db.py`'s `CursorPool` (call `await asyncio.to_thread(execute, pool, sql, params)` and unpack `(cols, rows)`).
7. **Mosaic data server is sqlglot-validated** — `/mosaic/query` parses incoming SQL via `sqlglot.parse(sql, dialect="duckdb")`, walks the AST, and rejects anything outside the schema allowlist or any non-SELECT/UNION statement, plus rejects internal `_` tables. Real column names from `execute()`'s cols list are used for the Mosaic JSON response.

## Known Issues

- **`lake.housing.hpd_violations` filtered queries fail** with `IO Error: Cannot open file ducklake-...parquet: No such file` — DuckLake catalog references parquet files that no longer exist on disk for some snapshots. `SELECT COUNT(*)` works, `WHERE zip = '...'` doesn't. Affects `/api/neighborhood` and `/api/buildings/worst` only. The dashboard handles this gracefully (em-dash placeholders). Fix is to compact the lake or re-materialize the table from Socrata.

## Related Memory

User-level memories (`~/.claude/projects/-Users-fattie2020/memory/`) cover:
- `project_commonground_stack.md` — full stack history & benchmarks
- `project_dagster_docker_memory_fix.md` — why Docker is mandatory
- `project_splink_entity_resolution.md`
- `project_mcp_server_*.md`
- `feedback_dagster_docker.md`, `feedback_dagster_always.md`
