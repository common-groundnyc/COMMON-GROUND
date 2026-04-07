---
phase: 05-batch-by-last-name-processor
plan: "01"
subsystem: entity-resolution
tags: [splink, batch-processing, entity-resolution, duckdb, ducklake]

requires:
  - phase: 03-materialized-name-index
    provides: lake.federal.name_index (55.5M rows)
  - phase: 04-splink-model-training
    provides: models/splink_model.json (trained Fellegi-Sunter parameters)
provides:
  - lake.federal.resolved_entities (55.5M rows, 33.1M clusters, 3.25M multi-record matches)
  - Batch processing script at scripts/batch_resolve_entities.py
affects: [06-ducklake-persistence, 07-mcp-tool-integration, 08-cross-reference-dashboard]

tech-stack:
  added: []
  patterns: [greedy bin-packing for batch sizing, per-batch Linker creation with pre-trained model]

key-files:
  created: [scripts/batch_resolve_entities.py]
  modified: []

key-decisions:
  - "500K records per batch with greedy bin-packing → 115 batches"
  - "55.5M records → 33.1M clusters, 3.25M multi-record matches (22.4M deduped records)"
  - "18k records/sec throughput, 51 min total"
  - "Cluster IDs globally unique (based on unique_id from name_index)"

patterns-established:
  - "Greedy last-name bin-packing for memory-safe Splink batch processing"
  - "Per-batch Linker with pre-trained model JSON"

issues-created: []

duration: 59min
completed: 2026-03-17
---

# Phase 5 Plan 1: Batch-by-Last-Name Processor Summary

**Processed 55.5M name records in 115 batches (51 min, 18k rec/sec), producing 33.1M clusters with 3.25M multi-record matches in lake.federal.resolved_entities**

## Performance

- **Duration:** 59 min (including script creation + full run)
- **Started:** 2026-03-17T14:33:14Z
- **Completed:** 2026-03-17T15:32:41Z
- **Tasks:** 2
- **Files modified:** 1 (created)

## Accomplishments
- Created batch processing script with greedy bin-packing (500K per batch)
- Processed all 55.5M records in 115 batches (0 failures)
- 33.1M clusters, 3.25M multi-record matches (22.4M records share a cluster with others)
- Throughput: 18,013 records/sec, 51.4 min total processing
- Results written to `lake.federal.resolved_entities` (7.9s DuckLake write)
- 0 NULL cluster_ids — complete coverage

## Cluster Size Distribution

| Size | Clusters | Records |
|------|----------|---------|
| 1 (singletons) | 29,886,978 | 29,886,978 |
| 2 | 1,109,215 | 2,218,430 |
| 3 | 528,191 | 1,584,573 |
| 4 | 330,308 | 1,321,232 |
| 5 | 220,027 | 1,100,135 |
| 6+ | 1,064,717 | 19,426,543 |

## Task Commits

1. **Task 1: Create batch processing script** - `e92d83c` (feat)
2. **Task 2: Full run and validate** - no code changes (execution only)

**Plan metadata:** (this commit)

## Files Created/Modified
- `scripts/batch_resolve_entities.py` — Batch processor with greedy bin-packing, --dry-run flag, error handling, DuckLake write

## Decisions Made
- 500K batch size proved safe — no OOM across 115 batches
- Greedy bin-packing by last_name frequency, sorted alphabetically
- Per-batch Linker creation (cannot reuse Linker across different data)
- Single-pass write to DuckLake after all batches complete (avoids partial writes on failure)

## Deviations from Plan

### Batch Table Fix

`clusters.as_duckdbpyrelation()` returns a Python relation, not a SQL-queryable table. Fixed by materializing to a `batch_results` table before appending to `all_results`. Minor implementation detail.

---

**Total deviations:** 1 auto-fixed (implementation detail)
**Impact on plan:** None — all tasks completed successfully.

## Issues Encountered
None

## Next Phase Readiness
- `lake.federal.resolved_entities` materialized with 55.5M rows, 33.1M clusters
- 3.25M multi-record matches ready for cross-table entity linking
- Phase 6 (DuckLake Persistence): Wire as Dagster asset, add incremental refresh
- Phase 7 (MCP Integration): Replace LIKE matching with cluster_id joins
- No blockers

---
*Phase: 05-batch-by-last-name-processor*
*Completed: 2026-03-17*
