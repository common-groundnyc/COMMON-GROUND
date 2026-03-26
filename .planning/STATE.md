# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** Phase 3 — Ownership Graph Rebuild (Phase 2 complete)

## Current Position

Phase: 3 of 10 (Ownership Graph Rebuild)
Plan: 0 of 2 complete
Status: Ready to start
Last activity: 2026-03-26 — Completed 02-02 (data bug fixes: property_history, graph_corps dedup, orphaned violations)

Progress: ███░░░░░░░ 20%

## Performance Metrics

**Velocity:**
- Total plans completed: 4 (v2.0) / 8 (v1.0)
- Average duration: ~56 min
- Total execution time: 3.25 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Data Audit | 2/2 | 60 min | 30 min |
| 2. Bug Fixes | 2/2 | 135 min | 68 min |

**Recent Trend:**
- Last 5 plans: 01-01 ✓, 01-02 ✓, 02-01 ✓, 02-02 ✓
- Trend: Data fixes faster than infrastructure (45 min vs 90 min)

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

Last session: 2026-03-26 02:20
Stopped at: 02-02 complete (data bug fixes). Phase 2 done. Ready for Phase 3 (Ownership Graph Rebuild).
Resume file: None
