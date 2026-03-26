# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** Phase 3 — Ownership Graph Rebuild (Plan 1 of 2 complete)

## Current Position

Phase: 3 of 10 (Ownership Graph Rebuild)
Plan: 1 of 2 complete
Status: Executing
Last activity: 2026-03-26 — Completed 03-01 (core table rebuilds: name-based owners, PLUTO buildings, ownership edges)

Progress: ███▌░░░░░░ 25%

## Performance Metrics

**Velocity:**
- Total plans completed: 5 (v2.0) / 8 (v1.0)
- Average duration: ~63 min
- Total execution time: 4.75 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Data Audit | 2/2 | 60 min | 30 min |
| 2. Bug Fixes | 2/2 | 135 min | 68 min |
| 3. Ownership Rebuild | 1/2 | 90 min | 90 min |

**Recent Trend:**
- Last 5 plans: 01-02 ✓, 02-01 ✓, 02-02 ✓, 03-01 ✓
- Trend: Infrastructure changes (OOM debugging) slower than data fixes

## Accumulated Context

### Decisions

- 13 entity types defined for classification
- oath_hearings (21.6M, 7 entity types) is the single highest-value ungraphed table
- Expanded graphs fit in ~1.1GB CSR (4% of 28GB limit)
- 2 new graphs proposed: Enforcement (P0), Civic (P1)
- resolved_entities cluster_id is the Phase 7 bridge key
- **DuckLake ignores CREATE SECRET — only SET s3_* works** (02-01 discovery)
- **MinIO switched to HTTP-only** (internal Docker network, firewall-protected)
- **Reconnect path is primary startup path** — warm-up always fails, all init must be in reconnect
- **QUALIFY must be inside CTE** — not on outer `SELECT *` (02-02 discovery)
- **docker cp required for container updates** — mcp_server.py baked into image, not mounted
- **Name-based PK for graph_owners** — UPPER(TRIM(owner_name)) replaces registrationid (03-01)
- **PLUTO BBL is float string** — use LEFT(bbl, 10) to extract 10-char BBL (03-01)
- **Stage DuckLake reads as temp tables** — avoids full-table scans during UNION (03-01)
- **Mega-owner filter (>100 buildings)** — prevents O(n^2) in graph_shared_owner self-join (03-01)
- **DuckDB config options lock after startup** — must SET at initial connection, not dynamically (03-01)
- **Memory tuning: 18GB limit, 4 threads, insertion order off** — required for expanded PLUTO data (03-01)

### Prior Milestone Context (v1.0 Entity Resolution)

- Splink 4.0 → 55.5M name index → 33.1M clusters across 44 tables
- `lake.federal.resolved_entities` — the unifying key for cross-domain graphs

### Deferred Issues

- 11 tables missed by column query truncation — corrections appended to registry
- ~~S3 credential breakage~~ — **FIXED** in 02-01
- ~~duckpgq not loading~~ — **FIXED** in 02-01
- ~~property_history date sort crash~~ — **FIXED** in 02-02
- ~~graph_corps 312K duplicates~~ — **FIXED** in 02-02 (104K unique)
- ~~graph_has_violation 13.7K orphans~~ — **FIXED** in 02-02 (0 orphans)
- MinIO HTTP change needs syncing to local infra/ directory
- neighborhood_portrait returns skeleton-only data (known limitation)

### Blockers/Concerns

- DuckPGQ MATCH inside CTEs/UNION segfaults — must avoid in Phase 9

## Session Continuity

Last session: 2026-03-26 06:00
Stopped at: 03-01 complete (core table rebuilds). graph_owners/buildings/owns rebuilt with name-based PK + PLUTO. Ready for 03-02.
Resume file: None
