# Agent-Optimized Repo Structure — Design Spec

**Date:** 2026-04-07
**Status:** Approved, ready for implementation plan
**Scope:** `~/Desktop/dagster-pipeline`

## 1. Goals

Restructure the repo so any fresh agent session (Claude Code, Codex, Cursor, Gemini CLI, Copilot) orients in minimum tokens and loads only context relevant to the current task. Apply April 2026 best practices:

- **AGENTS.md** as the Linux-Foundation-standard source of truth (CLAUDE.md → symlink)
- **Nested `AGENTS.md`** files with deeper-file-wins precedence for scoped conventions
- **Progressive disclosure** via `@import` so the root file stays <150 lines
- **Hand-curated only** — ETH Zurich research (InfoQ March 2026) shows auto-generated or bloated AGENTS.md files hurt task success by ~2% and raise inference cost 20–23%
- **Codemap** at `docs/ARCHITECTURE.md` so agents can orient without spelunking 200+ files
- **ADRs** capturing the *why* behind load-bearing decisions that are otherwise lost to git archaeology
- **Single plan tree** under `docs/superpowers/` — `docs/superpowers/` (GSD) is no longer used
- **Project-level memory** mirroring the global auto-memory pattern, seeded with repo-specific gotchas

## 2. Non-goals

- No LLM-auto-generated context files (explicitly avoided per 2026 research)
- No `llms.txt` (that's for public doc sites, not private repos)
- No architecture overviews in the root AGENTS.md (research shows they don't reduce navigation time)
- No refactoring of code itself — documentation/structure only
- No changes to the website repo (`common-ground-website`) — out of scope

## 3. Target layout

```
dagster-pipeline/
├── README.md                          # human landing page (DONE)
├── AGENTS.md                          # <150 lines, root agent onboarding
├── CLAUDE.md                          # → AGENTS.md (symlink, DONE)
├── .claudeignore                      # (DONE)
│
├── src/dagster_pipeline/
│   └── AGENTS.md                      # NEW — asset/source conventions (~60 lines)
│
├── infra/duckdb-server/
│   └── AGENTS.md                      # NEW — FastMCP/DuckDB conventions (~80 lines)
│
├── tests/
│   └── AGENTS.md                      # NEW — pytest invocation + fixtures (~30 lines)
│
├── docs/
│   ├── ARCHITECTURE.md                # NEW — codemap, one para per top dir (~150 lines)
│   ├── adr/                           # NEW — 10 ADRs (Michael Nygard format)
│   │   ├── 0001-dagster-in-docker.md
│   │   ├── 0002-ducklake-catalog-convention.md
│   │   ├── 0003-dlt-removal.md
│   │   ├── 0004-hnsw-acorn-over-lance.md
│   │   ├── 0005-splink-entity-resolution.md
│   │   ├── 0006-fastmcp-for-rest.md
│   │   ├── 0007-sqlglot-mosaic-validator.md
│   │   ├── 0008-cursor-pool.md
│   │   ├── 0009-git-deploy-over-rsync.md
│   │   └── 0010-minio-removal.md
│   ├── runbooks/                      # NEW — operational procedures
│   │   ├── deploy-mcp-server.md
│   │   ├── deploy-dagster.md
│   │   ├── secrets-sops.md
│   │   └── known-issues.md
│   └── superpowers/                   # UNIFIED plan tree (was also docs/superpowers/)
│       ├── PROJECT.md                 # moved from docs/superpowers/
│       ├── ROADMAP.md                 # moved from docs/superpowers/
│       ├── STATE.md                   # moved from docs/superpowers/
│       ├── phases/                    # moved from docs/superpowers/phases/
│       ├── audits/                    # moved from docs/superpowers/audits/
│       ├── plans/                     # merged: existing + docs/superpowers/plans/
│       └── specs/                     # existing (this file lives here)
│
└── memory/                            # NEW — project-level auto-memory
    ├── MEMORY.md                      # index (mirrors global pattern)
    ├── embedder_oom.md
    ├── ducklake_catalog_convention.md
    ├── hpd_violations_parquet_orphans.md
    ├── symlink_deploy_flow.md
    └── mcp_cursor_pool.md
```

**Deleted:** `docs/superpowers/` (fully migrated to `docs/superpowers/`).

## 4. Root `AGENTS.md` (<150 lines)

Hard line budget per section:

| Section | Budget | Content |
|---|---|---|
| One-liner | 2 | What this repo is |
| Golden rules | 8 | Dagster-in-Docker, never bypass Dagster, DuckLake `AS lake`, MCP is the interface, git-deploy, REST shares FastMCP, sqlglot Mosaic |
| Stack | 10 | Pinned versions only (Dagster 1.12.x, DuckDB 1.5.x, Splink 4.0, FastMCP 3.x, etc.) |
| Repo map | 15 | One line per top dir, each pointing at its nested `AGENTS.md` |
| Commands | 20 | Exact invocations: `docker compose up -d`, pytest command, materialize command, deploy |
| @imports | 10 | `@docs/ARCHITECTURE.md`, `@docs/runbooks/deploy-mcp-server.md`, `@docs/runbooks/known-issues.md`, `@docs/adr/` |
| Where to look | 15 | Pointers to `docs/superpowers/`, `memory/`, nested AGENTS.md |

**Content target: ~80 lines + headers/blanks → <150 total.** Everything currently in the 257-line CLAUDE.md that doesn't fit: split into nested AGENTS.md, runbooks, or ADRs.

## 5. Nested `AGENTS.md` files

### 5.1 `src/dagster_pipeline/AGENTS.md` (~60 lines)

- Asset/source/resource directory layout
- How to add a new Socrata, federal, or custom source
- `definitions.py` registration pattern
- Sensor patterns: flush_sensor (inline → parquet), freshness_sensor (hourly source-vs-lake)
- Materialize invocation: `docker compose exec dagster-code dagster asset materialize --select ... -m dagster_pipeline.definitions`
- Never run `dagster dev` locally — always Docker (forkserver OOM)

### 5.2 `infra/duckdb-server/AGENTS.md` (~80 lines)

- FastMCP super-tool registration pattern (`tools/*.py` → `mcp_server.py`)
- `@mcp.custom_route` for REST endpoints (`routes/*.py`)
- `CursorPool` contract: `execute(pool, sql, params) → (cols, rows)`, always `await asyncio.to_thread(execute, pool, sql, params)`
- Middleware stack: citation, freshness, percentile, response
- Mosaic sqlglot validator: `/mosaic/query` parses with `sqlglot.parse(sql, dialect="duckdb")`, AST walk, schema allowlist, rejects non-SELECT/UNION and `_`-prefixed internal tables
- Property graph cache (DuckPGQ, built at startup)
- `shared/` layout: `db.py`, `explore_queries.py`, `graph.py`, `vector_search.py`
- Deploy: `git pull` on server + `docker compose build duckdb-server && docker compose up -d duckdb-server`

### 5.3 `tests/AGENTS.md` (~30 lines)

- Two test trees exist:
  - Project-level `tests/` (Dagster assets, sources)
  - Server-level `infra/duckdb-server/tests/` (FastMCP routes, middleware, SQL validators)
- Server test invocation:
  ```bash
  cd infra/duckdb-server
  uv run --with fastmcp --with starlette --with httpx --with sqlglot pytest tests/ -v
  ```
- Fixture conventions
- Never mock DuckDB — use real in-memory DB for determinism

## 6. `docs/ARCHITECTURE.md` (codemap)

One paragraph per top-level directory (`src/`, `infra/`, `scripts/`, `tests/`, `docs/`, `memory/`). ASCII dependency diagram showing:

```
Dagster assets → DuckLake (postgres catalog + NVMe parquet) → FastMCP server → Cloudflare Tunnel → clients
                                    ↑                               ↑
                                Splink ER                      sqlglot validator
                                hnsw_acorn                     15 super tools
                                DuckPGQ                        REST routes
                                                               Mosaic server
```

Regenerable (the `doc-updater` agent or manual refresh). ~150 lines. **Not** duplicated in root AGENTS.md — `@import`-ed on demand.

## 7. ADRs (Michael Nygard format)

Each ADR: Context / Decision / Status / Consequences. One page each. Backfilled from existing audits, commit history, and global memory notes.

| # | Title | Source |
|---|---|---|
| 0001 | Dagster runs in Docker always | global memory `feedback_dagster_docker.md` |
| 0002 | DuckLake catalog `AS lake`, postgres public schema | CLAUDE.md §3 + commit `0dd2e09` |
| 0003 | dlt removed in favor of direct DuckDB ingestion | commit history 2026-03-26 |
| 0004 | hnsw_acorn over Lance for vector search | global memory `project_cg_vector_search.md` |
| 0005 | Splink 4.0 for probabilistic entity resolution | global memory `project_splink_entity_resolution.md` |
| 0006 | FastMCP serves REST + Mosaic alongside MCP protocol | `docs/superpowers/audits/ai-integration.md` |
| 0007 | sqlglot AST validator for Mosaic `/mosaic/query` | `docs/superpowers/plans/cg-explore-dashboard-design.md` |
| 0008 | CursorPool for thread-safe DuckDB access | global memory `project_mcp_server_hardening.md` |
| 0009 | Git-based deploy replaces rsync `deploy.sh` | session work 2026-04-07 |
| 0010 | MinIO / Hetzner S3 removed from pipeline | session work 2026-04-07 |

## 8. Runbooks

Extracted verbatim from current root CLAUDE.md deploy/secrets/known-issues sections:

- **`deploy-mcp-server.md`** — git push → ssh → `cd /opt/dagster-pipeline && git pull` → `cd /opt/common-ground && docker compose build duckdb-server && docker compose up -d duckdb-server`
- **`deploy-dagster.md`** — dagster-code container rebuild: `cd /opt/dagster-pipeline && git pull && docker compose up -d --build dagster-code`
- **`secrets-sops.md`** — edit/decrypt `.env.secrets.enc` via sops, never read `*.secrets.toml` directly
- **`known-issues.md`** — `lake.housing.hpd_violations` parquet orphans (filtered queries fail, count works); fix = compact lake or re-materialize

## 9. Plan tree unification

All of `docs/superpowers/*` → `docs/superpowers/*`. **GSD is no longer in use**; state files move alongside plans.

Migration map:

| From | To |
|---|---|
| `docs/superpowers/PROJECT.md` | `docs/superpowers/PROJECT.md` |
| `docs/superpowers/ROADMAP.md` | `docs/superpowers/ROADMAP.md` |
| `docs/superpowers/STATE.md` | `docs/superpowers/STATE.md` |
| `docs/superpowers/phases/**` | `docs/superpowers/phases/**` |
| `docs/superpowers/audits/**` | `docs/superpowers/audits/**` |
| `docs/superpowers/plans/**` | `docs/superpowers/plans/**` (merge with existing) |
| `docs/superpowers/config.json` | **delete** (GSD artifact) |
| `docs/superpowers/agent-history.json` | **delete** (GSD artifact) |
| `docs/superpowers/dlt-upgrade-plan.md` | `docs/superpowers/plans/dlt-upgrade-plan.md` |

Use `git mv` so history is preserved. After verification, `docs/superpowers/` is deleted. Cross-references in other markdown files are updated via `grep -rl "docs/superpowers/" | xargs sed -i ''` (verified by grep afterward).

**Naming-collision check:** if `docs/superpowers/plans/foo.md` and `docs/superpowers/plans/foo.md` both exist, the `docs/superpowers/` version is prefixed `strategic-` before merge. (Assumption to verify at execution time — likely zero collisions since the two trees have been used for different purposes.)

## 10. Project-level memory

`memory/` at repo root. **Committed to git** (unlike `~/.claude/projects/*/memory/` which is private user-level).

**`memory/MEMORY.md`** — index only, mirrors the global pattern:

```markdown
# Common Ground Pipeline — Project Memory

## Gotchas
- [Embedder OOM](embedder_oom.md) — force-CPU env var required
- [DuckLake catalog convention](ducklake_catalog_convention.md) — AS lake, postgres public schema
- [hpd_violations parquet orphans](hpd_violations_parquet_orphans.md) — filtered queries fail
- [Symlink deploy flow](symlink_deploy_flow.md) — /opt/common-ground/duckdb-server → git tree
- [MCP CursorPool contract](mcp_cursor_pool.md) — execute → (cols, rows), use asyncio.to_thread
```

Seed files are extracted from existing global memory entries (marked so they're not duplicated — global becomes a pointer, repo becomes the source of truth for this project).

## 11. Implementation phases

Each phase independently verifiable (grep for broken links / run existing tests).

**Phase 1 — Plan tree migration** *(low risk, sets up the tree)*
- `git mv docs/superpowers/PROJECT.md docs/superpowers/` etc.
- Merge `docs/superpowers/plans/` into `docs/superpowers/plans/`
- Delete `docs/superpowers/config.json`, `agent-history.json`
- Grep + sed all `docs/superpowers/` refs across the repo
- Delete `docs/superpowers/`
- Verification: `grep -r "docs/superpowers/" .` returns only historical matches in ADRs/commit messages

**Phase 2 — Root AGENTS.md slim + nested files**
- Extract deploy → `docs/runbooks/deploy-*.md`
- Extract known issues → `docs/runbooks/known-issues.md`
- Extract secrets → `docs/runbooks/secrets-sops.md`
- Create `src/dagster_pipeline/AGENTS.md`, `infra/duckdb-server/AGENTS.md`, `tests/AGENTS.md`
- Rewrite root `AGENTS.md` to <150 lines with `@import`s
- Verification: `wc -l AGENTS.md` < 150; all `@import`-ed files exist

**Phase 3 — ARCHITECTURE.md + ADRs**
- Write `docs/ARCHITECTURE.md` codemap
- Backfill 10 ADRs from audits, memory, commit history
- Link from root AGENTS.md `@imports`
- Verification: all 10 ADR files present; `docs/ARCHITECTURE.md` exists

**Phase 4 — Project memory seed**
- Create `memory/` directory
- Write `MEMORY.md` index + 5 seed files
- Commit
- Verification: `memory/MEMORY.md` present and indexes 5 files

## 12. Success criteria

- `wc -l AGENTS.md` < 150
- Root AGENTS.md contains only `@import`-ed paths that exist
- `grep -r "\docs/superpowers/" .` returns zero matches outside ADRs and git history
- All existing tests still pass (no code touched, so this is a smoke check)
- `tree -L 2 docs/` shows the target layout
- A fresh agent opening the repo can identify the stack, find the deploy procedure, and locate the server routes in <5 tool calls

## 13. Risks and mitigations

| Risk | Mitigation |
|---|---|
| `git mv` loses history on directory renames | Use `git mv` per-file, verify with `git log --follow` after |
| Cross-references break after plan migration | Grep + sed pass, then manual verification |
| Root AGENTS.md `@imports` aren't yet supported by all tools | Fallback: tools that don't support @import just see the raw path as a link — degrades gracefully |
| Plan-tree filename collisions | Check before merge; prefix colliders with `strategic-` |
| Nested AGENTS.md drift from code | Accept — they're hand-curated per 2026 research; plan a quarterly review |
| 10 ADRs is too many to write in one pass | Each ADR is ≤1 page, backfilled from existing sources — extraction not invention |

## 14. Open questions

None — all design questions resolved during brainstorming (see session transcript). Implementation plan will be generated next via `superpowers:writing-plans`.
