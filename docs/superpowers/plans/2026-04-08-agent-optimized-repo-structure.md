# Agent-Optimized Repo Structure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure `dagster-pipeline` for optimal fresh-agent onboarding per April 2026 best practices: AGENTS.md standard, nested scoped files, progressive disclosure, codemap, ADRs, unified plan tree.

**Architecture:** Documentation/structure only — no code changes. Root `AGENTS.md` <150 lines with `@import`s; nested `AGENTS.md` in `src/`, `infra/duckdb-server/`, `tests/`; `docs/ARCHITECTURE.md` codemap; `docs/adr/` with 10 backfilled ADRs; `docs/runbooks/` for operational procedures; `docs/superpowers/` fully migrated to `docs/superpowers/`; project-level `memory/` mirroring global pattern.

**Tech Stack:** Markdown, git, shell. No build/test changes.

**Spec:** `docs/superpowers/specs/2026-04-07-agent-optimized-repo-structure-design.md`

**Working directory:** `/Users/fattie2020/Desktop/dagster-pipeline` (all paths below are relative to this)

---

## Phase 1: Plan Tree Migration

### Task 1: Audit `docs/superpowers/` and prepare migration

**Files:**
- Read: `docs/superpowers/` (full tree)

- [ ] **Step 1: List everything in `docs/superpowers/`**

Run: `find .planning -type f | sort`
Expected: list of ~60 files across `PROJECT.md`, `ROADMAP.md`, `STATE.md`, `config.json`, `agent-history.json`, `dlt-upgrade-plan.md`, `phases/`, `audits/`, `plans/`.

- [ ] **Step 2: Verify no filename collisions with `docs/superpowers/plans/`**

Run:
```bash
comm -12 <(ls docs/superpowers/plans/ | sort) <(ls docs/superpowers/plans/ | sort)
```
Expected: empty output (no collisions). If any output, append `strategic-` prefix to the `docs/superpowers/plans/` file before the `git mv` in Task 3.

- [ ] **Step 3: Grep for cross-references to `docs/superpowers/`**

Run:
```bash
grep -rl "\docs/superpowers/" --exclude-dir=.git --exclude-dir=.planning . > /tmp/planning-refs.txt
wc -l /tmp/planning-refs.txt
cat /tmp/planning-refs.txt
```
Save the list for Task 4.

### Task 2: Delete GSD-only artifacts

**Files:**
- Delete: `docs/superpowers/config.json`
- Delete: `docs/superpowers/agent-history.json`

- [ ] **Step 1: Delete GSD artifacts**

Run:
```bash
git rm docs/superpowers/config.json docs/superpowers/agent-history.json
```
Expected: both files staged for deletion.

- [ ] **Step 2: Commit**

Run:
```bash
git commit -m "chore: remove obsolete GSD state files"
```

### Task 3: Move `docs/superpowers/` contents into `docs/superpowers/`

**Files:**
- Move: `docs/superpowers/PROJECT.md` → `docs/superpowers/PROJECT.md`
- Move: `docs/superpowers/ROADMAP.md` → `docs/superpowers/ROADMAP.md`
- Move: `docs/superpowers/STATE.md` → `docs/superpowers/STATE.md`
- Move: `docs/superpowers/dlt-upgrade-plan.md` → `docs/superpowers/plans/dlt-upgrade-plan.md`
- Move: `docs/superpowers/phases/` → `docs/superpowers/phases/`
- Move: `docs/superpowers/audits/` → `docs/superpowers/audits/`
- Move: `docs/superpowers/plans/*` → `docs/superpowers/plans/`

- [ ] **Step 1: Move top-level files**

Run:
```bash
git mv docs/superpowers/PROJECT.md docs/superpowers/PROJECT.md
git mv docs/superpowers/ROADMAP.md docs/superpowers/ROADMAP.md
git mv docs/superpowers/STATE.md docs/superpowers/STATE.md
git mv docs/superpowers/dlt-upgrade-plan.md docs/superpowers/plans/dlt-upgrade-plan.md
```

- [ ] **Step 2: Move directories**

Run:
```bash
git mv docs/superpowers/phases docs/superpowers/phases
git mv docs/superpowers/audits docs/superpowers/audits
for f in docs/superpowers/plans/*; do git mv "$f" "docs/superpowers/plans/$(basename $f)"; done
```

- [ ] **Step 3: Verify `docs/superpowers/` is empty**

Run: `find .planning -type f`
Expected: empty output.

- [ ] **Step 4: Remove empty directory**

Run:
```bash
rmdir docs/superpowers/plans .planning 2>/dev/null || true
ls -la | grep planning
```
Expected: no `.planning` entry.

- [ ] **Step 5: Commit**

Run:
```bash
git commit -m "refactor: migrate docs/superpowers/ to docs/superpowers/"
```

### Task 4: Update cross-references

**Files:**
- Modify: every file in `/tmp/planning-refs.txt` from Task 1

- [ ] **Step 1: Replace `docs/superpowers/` with `docs/superpowers/` in all referencing files**

Run:
```bash
while IFS= read -r f; do
  [ -f "$f" ] || continue
  sed -i '' 's|\docs/superpowers/|docs/superpowers/|g' "$f"
done < /tmp/planning-refs.txt
```

- [ ] **Step 2: Verify no `docs/superpowers/` references remain outside ADRs and git history**

Run:
```bash
grep -rl "\docs/superpowers/" --exclude-dir=.git . || echo "CLEAN"
```
Expected: `CLEAN` (or only matches inside `docs/superpowers/audits/*` historical references — those are fine to leave).

- [ ] **Step 3: Commit**

Run:
```bash
git add -A
git commit -m "refactor: update docs/superpowers/ references to docs/superpowers/"
```

---

## Phase 2: Runbooks and Nested AGENTS.md

### Task 5: Create `docs/runbooks/` directory and deploy-mcp-server runbook

**Files:**
- Create: `docs/runbooks/deploy-mcp-server.md`

- [ ] **Step 1: Create directory**

Run: `mkdir -p docs/runbooks`

- [ ] **Step 2: Write `docs/runbooks/deploy-mcp-server.md`**

Content:
```markdown
# Deploy MCP Server (duckdb-server)

The server source lives in `infra/duckdb-server/`. On the Hetzner host, `/opt/common-ground/duckdb-server` is a symlink to `/opt/dagster-pipeline/infra/duckdb-server/`, so `git pull` is the deploy mechanism.

## Procedure

```bash
# 1. Commit and push locally
git add infra/duckdb-server/...
git commit -m "feat: ..."
git push origin main

# 2. Pull on the server
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
cd /opt/dagster-pipeline && git pull

# 3. Rebuild the container (the compose file is in /opt/common-ground/)
cd /opt/common-ground
sudo docker compose build duckdb-server
sudo docker compose up -d duckdb-server
```

## Verify

```bash
curl https://mcp.common-ground.nyc/health
curl https://mcp.common-ground.nyc/api/catalog | jq '.schemas | length'
```

Expected: `"ok"` and `14`.
```

- [ ] **Step 3: Commit**

```bash
git add docs/runbooks/deploy-mcp-server.md
git commit -m "docs(runbooks): deploy-mcp-server"
```

### Task 6: deploy-dagster runbook

**Files:**
- Create: `docs/runbooks/deploy-dagster.md`

- [ ] **Step 1: Write `docs/runbooks/deploy-dagster.md`**

Content:
```markdown
# Deploy Dagster Code Containers

Dagster runs in Docker on the Hetzner host, with the repo checked out at `/opt/dagster-pipeline/`. Local dagster-dev is forbidden (macOS forkserver OOMs at 60 GB).

## Procedure

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119
cd /opt/dagster-pipeline
git pull
docker compose up -d --build dagster-code
docker compose logs -f dagster-daemon
```

## Verify

Open https://dagster.common-ground.nyc (Cloudflare Access gated). Check the asset count is ~376 and no dagster-code import errors in the logs.
```

- [ ] **Step 2: Commit**

```bash
git add docs/runbooks/deploy-dagster.md
git commit -m "docs(runbooks): deploy-dagster"
```

### Task 7: secrets-sops runbook

**Files:**
- Create: `docs/runbooks/secrets-sops.md`

- [ ] **Step 1: Write `docs/runbooks/secrets-sops.md`**

Content:
```markdown
# Secrets (SOPS + age)

All secrets live in `.env.secrets.enc` (committed, encrypted with age). `.env.secrets` is gitignored.

## Edit

```bash
sops --input-type dotenv --output-type dotenv .env.secrets.enc
```

## Decrypt to local `.env.secrets`

```bash
sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc > .env.secrets
```

## Rules

- **Never** read `*.secrets.toml` files directly from code or agents.
- **Never** echo secrets to the terminal (`env`, `printenv`, `cat`).
- **Never** commit `.env.secrets` (decrypted).
- Use `dlt.secrets["key"]` in Python when applicable.
```

- [ ] **Step 2: Commit**

```bash
git add docs/runbooks/secrets-sops.md
git commit -m "docs(runbooks): secrets-sops"
```

### Task 8: known-issues runbook

**Files:**
- Create: `docs/runbooks/known-issues.md`

- [ ] **Step 1: Write `docs/runbooks/known-issues.md`**

Content:
```markdown
# Known Issues

## `lake.housing.hpd_violations` — orphan parquet references

**Symptom:** Filtered queries against `lake.housing.hpd_violations` fail with:
```
IO Error: Cannot open file ducklake-...parquet: No such file
```
`SELECT COUNT(*)` works; `WHERE zip = '...'` does not.

**Affected endpoints:** `/api/neighborhood`, `/api/buildings/worst`. The dashboard handles this gracefully with em-dash placeholders.

**Root cause:** DuckLake catalog references parquet files that no longer exist on disk for some snapshots.

**Fix:** Compact the lake or re-materialize the table from Socrata.

```bash
# Re-materialize
docker compose exec dagster-code dagster asset materialize \
  --select 'housing/hpd_violations' -m dagster_pipeline.definitions
```
```

- [ ] **Step 2: Commit**

```bash
git add docs/runbooks/known-issues.md
git commit -m "docs(runbooks): known-issues"
```

### Task 9: `src/dagster_pipeline/AGENTS.md`

**Files:**
- Create: `src/dagster_pipeline/AGENTS.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# src/dagster_pipeline — Agent Notes

This is the **ingestion tier**. Dagster assets fetch from Socrata, federal sources, and custom scrapers, then write directly to DuckLake. No dlt — removed 2026-03-26.

## Layout

```
src/dagster_pipeline/
├── definitions.py              # Dagster Definitions, jobs, schedules, sensors
├── sources/                    # API clients & extractors (pure functions, no Dagster)
│   ├── socrata_direct.py
│   ├── bls.py, census.py, hud.py, fec.py
│   ├── courtlistener.py, littlesis.py, usaspending.py
│   └── ...
├── defs/                       # Dagster @asset definitions
│   ├── socrata_direct_assets.py    # 287 Socrata assets
│   ├── federal_direct_assets.py    # 22 federal assets
│   ├── name_index_asset.py         # ER preprocessing
│   ├── resolved_entities_asset.py  # Splink clustering
│   ├── entity_master_asset.py      # Canonical entities
│   ├── foundation_assets.py        # h3, phonetic, fingerprints
│   ├── materialized_view_assets.py # 9 MVs
│   ├── spatial_views_asset.py
│   ├── address_lookup_asset.py
│   ├── geo_zip_boundaries_asset.py # NYC MODZCTA polygons
│   ├── quality_assets.py
│   ├── flush_sensor.py             # Inline → parquet flush
│   └── freshness_sensor.py         # Hourly source-vs-lake check
└── resources/                  # Dagster resources (duckdb, http clients)
```

## Conventions

- **One asset per table.** Asset key = `schema/table` (e.g. `housing/hpd_violations`).
- **Sources are pure.** `sources/*.py` exposes plain functions; only `defs/*.py` wraps them as `@asset`.
- **Never bypass Dagster.** No ad-hoc ingestion scripts. All runs must appear in the Dagster UI.
- **Register new assets** by importing them into `definitions.py` and adding to `Definitions(assets=[...])`.
- **DuckLake catalog is attached `AS lake`.** Write tables as `lake.<schema>.<table>`.

## Adding a new Socrata asset

1. Add dataset ID + schema to `src/dagster_pipeline/sources/datasets.py`.
2. Run the generator: the `socrata_direct_assets.py` picks it up automatically.
3. Materialize once locally via Docker to verify:
   ```bash
   docker compose exec dagster-code dagster asset materialize \
     --select 'schema/table_name' -m dagster_pipeline.definitions
   ```

## Run rules

- **Dagster always runs in Docker** — never `dagster dev` locally. See `@docs/runbooks/deploy-dagster.md`.
- **Materialize from inside the container:**
  ```bash
  docker compose exec dagster-code dagster asset materialize \
    --select 'housing/hpd_violations' -m dagster_pipeline.definitions
  ```

## See also

- `@docs/adr/0001-dagster-in-docker.md`
- `@docs/adr/0003-dlt-removal.md`
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/AGENTS.md
git commit -m "docs(agents): nested AGENTS.md for src/dagster_pipeline"
```

### Task 10: `infra/duckdb-server/AGENTS.md`

**Files:**
- Create: `infra/duckdb-server/AGENTS.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/AGENTS.md
git commit -m "docs(agents): nested AGENTS.md for infra/duckdb-server"
```

### Task 11: `tests/AGENTS.md`

**Files:**
- Create: `tests/AGENTS.md`

- [ ] **Step 1: Ensure `tests/` exists**

Run: `ls tests/ 2>/dev/null || mkdir tests`

- [ ] **Step 2: Write the file**

Content:
```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add tests/AGENTS.md
git commit -m "docs(agents): nested AGENTS.md for tests"
```

---

## Phase 3: Root AGENTS.md, ARCHITECTURE.md, ADRs

### Task 12: Rewrite root `AGENTS.md` to <150 lines

**Files:**
- Modify: `AGENTS.md` (currently 257 lines, target <150)

- [ ] **Step 1: Replace entire file contents**

Use `Write` tool to overwrite `AGENTS.md` with:

```markdown
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
```

- [ ] **Step 2: Verify line count**

Run: `wc -l AGENTS.md`
Expected: < 150.

- [ ] **Step 3: Verify symlink still resolves**

Run: `readlink CLAUDE.md && cat CLAUDE.md | head -3`
Expected: `AGENTS.md` and the first lines of the new content.

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md
git commit -m "docs(agents): slim root AGENTS.md to <150 lines with @imports"
```

### Task 13: `docs/ARCHITECTURE.md` codemap

**Files:**
- Create: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs: add ARCHITECTURE.md codemap"
```

### Task 14: Create `docs/adr/` and ADR 0001 — Dagster in Docker

**Files:**
- Create: `docs/adr/0001-dagster-in-docker.md`

- [ ] **Step 1: Create directory**

Run: `mkdir -p docs/adr`

- [ ] **Step 2: Write ADR 0001**

Content:
```markdown
# 0001 — Dagster runs in Docker always

**Status:** Accepted (2026-03-xx)

## Context

Running `dagster dev` directly on macOS triggered repeated OOM crashes at ~60 GB RAM. Root cause: Python's default multiprocessing start method on macOS is `spawn`, which duplicates parent memory per worker. The asset graph has 300+ definitions and Dagster holds them all in each subprocess.

## Decision

**Dagster always runs inside Docker**, which uses Linux's `forkserver` start method. Forked workers share memory via copy-on-write, cutting peak usage by ~75%.

Concretely:
- Local dev: `docker compose up -d` starts `dagster-code`, `dagster-webserver`, `dagster-daemon`
- CLI: `docker compose exec dagster-code dagster asset materialize ...`
- Never run `dagster dev` on the host

## Consequences

- **+** No more OOM crashes
- **+** Local env matches production exactly
- **−** Slightly slower iteration (rebuild on code changes)
- **−** Agents must remember to `docker compose exec` instead of running things directly
```

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0001-dagster-in-docker.md
git commit -m "docs(adr): 0001 dagster in docker"
```

### Task 15: ADR 0002 — DuckLake catalog convention

**Files:**
- Create: `docs/adr/0002-ducklake-catalog-convention.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0002 — DuckLake attached `AS lake`, metadata in postgres `public`

**Status:** Accepted (2026-04-07, supersedes 2026-03 convention)

## Context

DuckLake was originally attached with `METADATA_SCHEMA='lake'`, creating a dedicated `lake` schema in postgres for catalog metadata. This caused confusion: "lake" referred to both the ATTACH alias and the postgres schema. Commit `0dd2e09` claimed convergence was complete, but the actual code still had the dual meaning.

## Decision

- DuckLake is attached with **`AS lake`** (the alias stays).
- Postgres metadata lives in the **default `public` schema** — `METADATA_SCHEMA='lake'` is retired.
- Tables are still referenced as **`lake.<schema>.<table>`** because that's the ATTACH alias — this is a query-time convention, not a postgres schema.

## Consequences

- **+** Single meaning for "lake": the ATTACH alias
- **+** Simpler postgres: one schema, not two
- **−** Historical snapshots with `METADATA_SCHEMA='lake'` are incompatible and were retired
- **−** Agents must understand the two meanings of "lake" before 2026-04-07
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0002-ducklake-catalog-convention.md
git commit -m "docs(adr): 0002 ducklake catalog convention"
```

### Task 16: ADR 0003 — dlt removal

**Files:**
- Create: `docs/adr/0003-dlt-removal.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0003 — dlt removed in favor of direct DuckDB ingestion

**Status:** Accepted (2026-03-26)

## Context

The pipeline originally used `dlt` (data load tool) for ingestion: REST API sources, schema inference, staging, and destination loaders. Benchmarks showed:

- Meltano: ~2k rows/sec (rejected earlier)
- dlt: ~8k rows/sec
- Direct httpx → parquet → DuckLake: ~20k rows/sec (2.5× dlt)

dlt's abstractions (`@dlt.resource`, `@dlt.source`, load packages, staging destinations) added operational complexity, state management in `.dlt/`, and debugging surface area for marginal benefit once the sources stabilized.

## Decision

Remove `dlt` entirely. Sources fetch via `httpx` and write straight to DuckLake as parquet. Dagster asset definitions wrap the raw source functions.

## Consequences

- **+** 2.5× throughput on large Socrata tables
- **+** No more dlt state-management infrastructure (no `.dlt/` volume, no staging destination)
- **+** Simpler debugging — one Python function per source
- **−** Lost dlt's automatic schema evolution (handled manually now)
- **−** One-time migration cost (commit `5dc5f01` era)
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0003-dlt-removal.md
git commit -m "docs(adr): 0003 dlt removal"
```

### Task 17: ADR 0004 — hnsw_acorn over Lance

**Files:**
- Create: `docs/adr/0004-hnsw-acorn-over-lance.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0004 — hnsw_acorn over Lance for vector search

**Status:** Accepted (2026-04-07)

## Context

Semantic search needed a filtered vector index. Two candidates:

- **Lance** — Rust-based columnar format with ANN support, used via `lancedb`. Required a second storage layer alongside DuckLake.
- **hnsw_acorn** — DuckDB community extension implementing ACORN-1 (filtered HNSW). Keeps everything inside DuckDB.

Experimental HNSW persistence in DuckDB is unreliable, so the index must be rebuilt from a parquet cache.

## Decision

Use **`hnsw_acorn`** with a parquet-cache rebuild pattern. Embeddings (`MiniLM-L6-v2`, 384-dim, INT8) are materialized to `main.*` tables via ONNX Runtime and indexed at server startup.

`infra/duckdb-server/shared/lance.py` was renamed to `shared/vector_search.py` on 2026-04-07 to remove the misleading name.

## Consequences

- **+** One storage layer (DuckLake), no Lance sidecar
- **+** Filtered HNSW via ACORN-1 (no Lance equivalent at the time)
- **+** Index rebuild from parquet cache is deterministic
- **−** Index is rebuilt on every server start (startup latency)
- **−** Relies on a community extension
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0004-hnsw-acorn-over-lance.md
git commit -m "docs(adr): 0004 hnsw_acorn over lance"
```

### Task 18: ADR 0005 — Splink for entity resolution

**Files:**
- Create: `docs/adr/0005-splink-entity-resolution.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0005 — Splink 4.0 for probabilistic entity resolution

**Status:** Accepted (2026-03-17, v1.0 shipped)

## Context

55M+ records across NYC open data contain the same people and companies under different spellings, addresses, and abbreviations. Deterministic join keys fail. Options considered:

- Hand-rolled fuzzy matching (rapidfuzz + custom blocking) — brittle, no probabilistic framework
- Dedupe.io — Python-native but slow on 55M records
- **Splink 4.0** — probabilistic ER with a DuckDB backend, scales to 100M+ via blocking and EM training

## Decision

Use **Splink 4.0** with the DuckDB backend. Training model persisted at `models/splink_model_v2.json`. Clustering runs as a Dagster asset (`resolved_entities_asset.py`) and feeds `entity_master_asset.py`.

## Consequences

- **+** Probabilistic scores + explainable match weights
- **+** Runs entirely in DuckDB, no extra infra
- **+** Scales — v1.0 shipped 55M records in 8 phases
- **−** EM training is slow; model is retrained infrequently
- **−** Requires careful blocking rules to avoid O(n²) comparisons
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0005-splink-entity-resolution.md
git commit -m "docs(adr): 0005 splink entity resolution"
```

### Task 19: ADR 0006 — FastMCP serves REST + Mosaic

**Files:**
- Create: `docs/adr/0006-fastmcp-for-rest.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0006 — FastMCP serves REST and Mosaic alongside MCP protocol

**Status:** Accepted (2026-03-26)

## Context

The explore dashboard needed REST endpoints (`/api/neighborhood`, `/api/zips/search`, `/api/buildings/worst`) and a Mosaic data server (`/mosaic/query`). Options:

- **Separate FastAPI service** — duplicate DuckDB connection pools, separate deploy, separate container
- **Sidecar process** — same host, separate port, coordination overhead
- **Extend FastMCP with `@mcp.custom_route`** — reuse the existing FastMCP 3.x process and its DuckDB pool

## Decision

Extend the existing FastMCP server. Register REST handlers via `@mcp.custom_route` in `routes/*.py`, all sharing the same `CursorPool`.

## Consequences

- **+** One process, one DuckDB pool, one deploy
- **+** REST endpoints reuse middleware (citation, freshness, percentile)
- **+** Mosaic data server lives next to the MCP tools it mirrors
- **−** Couples REST availability to MCP server availability
- **−** FastMCP's `@mcp.custom_route` is newer and less documented than FastAPI
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0006-fastmcp-for-rest.md
git commit -m "docs(adr): 0006 fastmcp for rest"
```

### Task 20: ADR 0007 — sqlglot Mosaic validator

**Files:**
- Create: `docs/adr/0007-sqlglot-mosaic-validator.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0007 — sqlglot AST validator for `/mosaic/query`

**Status:** Accepted (2026-04-07)

## Context

The Mosaic data server at `/mosaic/query` accepts arbitrary SQL from the browser and returns JSON. Accepting raw SQL is a SQL injection and data-exfiltration footgun. Options:

- **Regex/string filters** — trivially bypassed
- **Parse + walk + allowlist** — robust but requires a real SQL parser
- **DuckDB's own parser** — no stable AST walker API

## Decision

Use **`sqlglot`** to parse with `dialect="duckdb"`, walk the AST, and enforce:

- Only `SELECT` and `UNION` statements allowed
- All referenced tables must be in the schema allowlist
- Table names starting with `_` are rejected (internal tables)
- Column names in the response come from the real `execute()` result, not client input

## Consequences

- **+** Provable rejection of non-SELECT / non-allowlist queries
- **+** AST walk catches nested subqueries, CTEs, unions
- **+** sqlglot is battle-tested and maintained
- **−** Dialect parity is not perfect — some DuckDB-specific syntax may parse oddly
- **−** Adds sqlglot to the server dependency tree
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0007-sqlglot-mosaic-validator.md
git commit -m "docs(adr): 0007 sqlglot mosaic validator"
```

### Task 21: ADR 0008 — CursorPool

**Files:**
- Create: `docs/adr/0008-cursor-pool.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0008-cursor-pool.md
git commit -m "docs(adr): 0008 cursor pool"
```

### Task 22: ADR 0009 — Git deploy over rsync

**Files:**
- Create: `docs/adr/0009-git-deploy-over-rsync.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0009 — Git-based deploy replaces rsync

**Status:** Accepted (2026-04-07)

## Context

The original deploy was `infra/deploy.sh` — a bash script that rsynced the local tree to the Hetzner host. Problems:

- Diverging server state (edits on server not reflected in repo)
- No audit trail for what was deployed
- Had to re-rsync to redeploy even unchanged files
- The server's `/opt/common-ground/duckdb-server/` directory had diverged from `/opt/dagster-pipeline/infra/duckdb-server/`

## Decision

- Server checks out the repo at `/opt/dagster-pipeline/`
- `/opt/common-ground/duckdb-server` is a **symlink** to `/opt/dagster-pipeline/infra/duckdb-server/`
- Deploy = `git push` locally → `ssh` → `git pull` on server → `docker compose build duckdb-server && docker compose up -d duckdb-server`
- `infra/deploy.sh` is kept as legacy but is no longer primary

## Consequences

- **+** One source of truth (the git tree)
- **+** Full audit trail via git history
- **+** Redeploy is cheap (only fetches new commits)
- **+** The symlink makes `/opt/common-ground/docker-compose.yml` see the latest code automatically
- **−** Requires server to have git + GitHub auth configured
- **−** Two compose projects now bridge via `common-ground-shared` external network
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0009-git-deploy-over-rsync.md
git commit -m "docs(adr): 0009 git deploy over rsync"
```

### Task 23: ADR 0010 — MinIO removal

**Files:**
- Create: `docs/adr/0010-minio-removal.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# 0010 — MinIO / Hetzner S3 removed from pipeline

**Status:** Accepted (2026-04-07)

## Context

Early iterations used MinIO and Hetzner S3 as an intermediate staging layer between ingestion and DuckLake. This was a vestige of the Meltano era. Once ingestion wrote directly to local NVMe parquet (see ADR 0003 — dlt removal), the S3 layer became dead weight:

- Extra network hops for every row
- Extra credentials to rotate (`MINIO_*`, `HETZNER_S3_*`)
- Extra container (`minio`) in `docker-compose.yml`
- Extra failure mode (bucket full / auth drift)

## Decision

Remove MinIO and Hetzner S3 entirely. Ingestion writes directly to NVMe-backed parquet under DuckLake's managed path.

Cleanup (session 2026-04-07):
- Dropped MinIO container from `docker-compose.yml`
- Removed `.dlt/` volume mount (orphan from dlt era)
- Purged dlt artifacts from `/opt/common-ground/`
- Removed `MINIO_*` / S3 env vars from `.env.secrets.enc`

## Consequences

- **+** Simpler stack: one storage layer (NVMe + DuckLake)
- **+** Fewer secrets to manage
- **+** Lower latency on writes
- **−** No offsite durability at the storage layer — backups now happen at the postgres/parquet level (see `docs/runbooks/` future entry)
- **−** Single-host failure mode: if the NVMe dies, data is gone until restored from backup
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0010-minio-removal.md
git commit -m "docs(adr): 0010 minio removal"
```

---

## Phase 4: Project Memory Seed

### Task 24: Create `memory/` and `memory/MEMORY.md` index

**Files:**
- Create: `memory/MEMORY.md`

- [ ] **Step 1: Create directory**

Run: `mkdir -p memory`

- [ ] **Step 2: Write `memory/MEMORY.md`**

Content:
```markdown
# Common Ground Pipeline — Project Memory

This is the **repo-scoped** auto-memory, committed to git. Contrast with `~/.claude/projects/*/memory/` which is private/user-scoped.

Each entry is a pointer to a file with the full detail. Keep this index under 200 lines.

## Gotchas
- [Embedder OOM during startup](embedder_oom.md) — force-CPU env var required
- [DuckLake catalog convention](ducklake_catalog_convention.md) — `AS lake`, postgres `public` schema
- [hpd_violations parquet orphans](hpd_violations_parquet_orphans.md) — filtered queries fail, COUNT works
- [Symlink deploy flow](symlink_deploy_flow.md) — `/opt/common-ground/duckdb-server` → git tree
- [MCP CursorPool contract](mcp_cursor_pool.md) — `execute(pool, sql, params) → (cols, rows)`, always `asyncio.to_thread`
```

- [ ] **Step 3: Commit**

```bash
git add memory/MEMORY.md
git commit -m "docs(memory): init project-level memory index"
```

### Task 25: Memory file — embedder OOM

**Files:**
- Create: `memory/embedder_oom.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# Embedder OOM during startup

**Symptom:** `duckdb-server` container enters a crash loop at startup. Logs show the MiniLM ONNX model trying to use MPS (Apple Metal) or CUDA and failing.

**Root cause:** ONNX Runtime picks up whatever provider is available. On Hetzner (CPU-only) and on macOS dev machines, this can trigger SIGBUS or OOM on model load.

**Fix:** Force CPU execution via env var in `docker-compose.yml` (and `.env`):

```
HINDSIGHT_API_EMBEDDINGS_LOCAL_FORCE_CPU=1
```

This is a real requirement — **not** a placebo. Removing it triggers the crash loop.

**Related:** hindsight memory daemon uses the same pattern. See global memory `project_mcp_server_march26.md` for the original incident.
```

- [ ] **Step 2: Commit**

```bash
git add memory/embedder_oom.md
git commit -m "docs(memory): embedder OOM + force-CPU env var"
```

### Task 26: Memory file — DuckLake catalog convention

**Files:**
- Create: `memory/ducklake_catalog_convention.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# DuckLake catalog convention (post 2026-04-07)

DuckLake is **attached** with `AS lake` — this is the query-time alias.

Postgres metadata lives in the **default `public` schema**. The historic `METADATA_SCHEMA='lake'` was retired 2026-04-07 (see `docs/adr/0002-ducklake-catalog-convention.md`).

## Rules

- Reference tables as `lake.<schema>.<table>` — always three parts.
- Do **not** create a `lake` schema in postgres. Everything goes in `public`.
- If you see `METADATA_SCHEMA='lake'` in code, it's legacy and should be removed.

## Why agents get confused

"lake" has two historical meanings: the ATTACH alias (still current) and a postgres schema (retired). The ATTACH alias is the only one that matters now.
```

- [ ] **Step 2: Commit**

```bash
git add memory/ducklake_catalog_convention.md
git commit -m "docs(memory): ducklake catalog convention"
```

### Task 27: Memory file — hpd_violations parquet orphans

**Files:**
- Create: `memory/hpd_violations_parquet_orphans.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# `lake.housing.hpd_violations` parquet orphans

**Symptom:**
```
IO Error: Cannot open file ducklake-...parquet: No such file
```

**Scope:** `SELECT COUNT(*)` works. `SELECT ... WHERE zip = '...'` fails. Only affects `hpd_violations`.

**Affected endpoints:** `/api/neighborhood`, `/api/buildings/worst`. The dashboard handles it gracefully with em-dash placeholders.

**Root cause:** DuckLake catalog still references parquet files that have been deleted from disk for some snapshots.

**Fix:** Either compact the lake or re-materialize the table:

```bash
docker compose exec dagster-code dagster asset materialize \
  --select 'housing/hpd_violations' -m dagster_pipeline.definitions
```

**See also:** `docs/runbooks/known-issues.md`.
```

- [ ] **Step 2: Commit**

```bash
git add memory/hpd_violations_parquet_orphans.md
git commit -m "docs(memory): hpd_violations parquet orphans"
```

### Task 28: Memory file — symlink deploy flow

**Files:**
- Create: `memory/symlink_deploy_flow.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
# Symlink deploy flow

The Hetzner host has **two compose projects** that share the same duckdb-server source via a symlink:

```
/opt/dagster-pipeline/              # git-tracked repo
├── docker-compose.yml              # dagster-code, dagster-daemon, dagster-webserver
└── infra/duckdb-server/            # source of truth for MCP server

/opt/common-ground/                 # legacy directory
├── docker-compose.yml              # postgres, duckdb-server, duck-ui, cloudflared, xyops
└── duckdb-server → /opt/dagster-pipeline/infra/duckdb-server   # SYMLINK
```

## Implications

- **Deploy the MCP server with `git pull`**, then rebuild from `/opt/common-ground/`:
  ```bash
  cd /opt/dagster-pipeline && git pull
  cd /opt/common-ground && sudo docker compose build duckdb-server && sudo docker compose up -d duckdb-server
  ```
- The two compose projects **bridge via the `common-ground-shared` external Docker network**.
- Editing files under `/opt/common-ground/duckdb-server/` edits the git tree directly. Don't.

**See also:** `docs/adr/0009-git-deploy-over-rsync.md`, `docs/runbooks/deploy-mcp-server.md`.
```

- [ ] **Step 2: Commit**

```bash
git add memory/symlink_deploy_flow.md
git commit -m "docs(memory): symlink deploy flow"
```

### Task 29: Memory file — MCP CursorPool contract

**Files:**
- Create: `memory/mcp_cursor_pool.md`

- [ ] **Step 1: Write the file**

Content:
```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add memory/mcp_cursor_pool.md
git commit -m "docs(memory): MCP cursor pool contract"
```

---

## Phase 5: Final verification

### Task 30: End-to-end smoke check

**Files:** (read-only verification)

- [ ] **Step 1: Verify root AGENTS.md line count**

Run: `wc -l AGENTS.md`
Expected: < 150.

- [ ] **Step 2: Verify CLAUDE.md still resolves**

Run: `readlink CLAUDE.md && head -3 CLAUDE.md`
Expected: `AGENTS.md`, then the title line "# Common Ground Data Pipeline".

- [ ] **Step 3: Verify no `docs/superpowers/` references remain**

Run:
```bash
grep -r "\docs/superpowers/" --exclude-dir=.git --exclude-dir=node_modules . 2>/dev/null | grep -v "docs/superpowers/audits" | grep -v "docs/adr" || echo "CLEAN"
```
Expected: `CLEAN` (or only matches inside historical audits / ADRs, which are fine).

- [ ] **Step 4: Verify `docs/superpowers/` is deleted**

Run: `ls -la | grep planning || echo "GONE"`
Expected: `GONE`.

- [ ] **Step 5: Verify all nested AGENTS.md exist**

Run:
```bash
ls src/dagster_pipeline/AGENTS.md infra/duckdb-server/AGENTS.md tests/AGENTS.md
```
Expected: all three listed, no errors.

- [ ] **Step 6: Verify ARCHITECTURE.md and ADRs exist**

Run:
```bash
ls docs/ARCHITECTURE.md
ls docs/adr/ | wc -l
```
Expected: file exists, and ADR count is 10.

- [ ] **Step 7: Verify runbooks exist**

Run: `ls docs/runbooks/`
Expected: `deploy-dagster.md`, `deploy-mcp-server.md`, `known-issues.md`, `secrets-sops.md`.

- [ ] **Step 8: Verify memory seed**

Run: `ls memory/`
Expected: `MEMORY.md` + 5 files (embedder_oom, ducklake_catalog_convention, hpd_violations_parquet_orphans, symlink_deploy_flow, mcp_cursor_pool).

- [ ] **Step 9: Verify @import targets exist**

Run:
```bash
for f in docs/ARCHITECTURE.md docs/runbooks/deploy-mcp-server.md docs/runbooks/deploy-dagster.md docs/runbooks/secrets-sops.md docs/runbooks/known-issues.md; do
  [ -f "$f" ] && echo "OK $f" || echo "MISSING $f"
done
```
Expected: all `OK`.

- [ ] **Step 10: Verify tests still pass**

Run:
```bash
cd infra/duckdb-server
uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v 2>&1 | tail -10
```
Expected: no regressions (same pass count as before the restructure — docs-only changes should not affect tests).

- [ ] **Step 11: Final commit if anything drifted**

Run:
```bash
git status
```
Expected: clean tree. If not, commit any incidental fixes with `chore: post-restructure cleanup`.

---

## Self-review notes

- **Spec coverage:** Every section of the spec (§3–10) maps to a task or phase.
- **Placeholder scan:** No TBDs. All code/content is inline.
- **Type consistency:** All file paths and ADR filenames match between spec and plan. `CursorPool` signature is consistent across Task 10 (infra AGENTS.md), Task 21 (ADR 0008), and Task 29 (memory file).
- **Phase independence:** Phase 1 (plan migration) is isolated from Phase 2–4 (new files). Phase 5 verifies all of them.
- **Frequent commits:** Each task commits once. 30 tasks = 30 commits. Phase boundaries are natural review points.
