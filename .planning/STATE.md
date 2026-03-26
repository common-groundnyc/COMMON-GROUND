# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-25)

**Core value:** Cross-domain connections — trace any entity across every dataset in the lake
**Current focus:** Phase 2 — Bug Fixes (Plan 01 done, Plan 02 next)

## Current Position

Phase: 2 of 10 (Bug Fixes)
Plan: 1 of 2 complete
Status: Executing
Last activity: 2026-03-26 — Completed 02-01 (infrastructure fixes: S3 + duckpgq)

Progress: ██░░░░░░░░ 15%

## Performance Metrics

**Velocity:**
- Total plans completed: 3 (v2.0) / 8 (v1.0)
- Average duration: ~60 min
- Total execution time: 2.5 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Data Audit | 2/2 | 60 min | 30 min |
| 2. Bug Fixes | 1/2 | 90 min | 90 min |

**Recent Trend:**
- Last 5 plans: 01-01 ✓, 01-02 ✓, 02-01 ✓
- Trend: Infrastructure fix took longer (debugging)

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

### Prior Milestone Context (v1.0 Entity Resolution)

- Splink 4.0 → 55.5M name index → 33.1M clusters across 44 tables
- `lake.federal.resolved_entities` — the unifying key for cross-domain graphs

### Deferred Issues

- 11 tables missed by column query truncation — corrections appended to registry
- ~~S3 credential breakage~~ — **FIXED** in 02-01
- ~~duckpgq not loading~~ — **FIXED** in 02-01
- MinIO HTTP change needs syncing to local infra/ directory

### Blockers/Concerns

- DuckPGQ MATCH inside CTEs/UNION segfaults — must avoid in Phase 9
- ~~S3 credentials and duckpgq loading~~ — **RESOLVED**

## Session Continuity

Last session: 2026-03-26 02:00
Stopped at: 02-01 complete (infrastructure fixes). Ready for 02-02 (data bug fixes).
Resume file: None
