---
phase: 03-materialized-name-index
plan: "01"
subsystem: entity-resolution
tags: [duckdb, ducklake, dagster, name-index, sql, materialization]

requires:
  - phase: 02-universal-name-extractor
    provides: Validated extraction SQL for all 47 tables (120.7M rows)
  - phase: 01-name-column-discovery
    provides: NAME_REGISTRY with 47 tables, get_extraction_sql()
provides:
  - lake.federal.name_index table (55.5M rows, 44 source tables)
  - Dagster @asset for re-materialization
  - _connect_ducklake() reusable helper
  - scripts/materialize_name_index.py for ad-hoc runs
affects: [04-splink-model-training, 05-batch-by-last-name-processor]

tech-stack:
  added: []
  patterns: [plain Dagster @asset for SQL transformations, DuckLake connection helper]

key-files:
  created: [src/dagster_pipeline/defs/name_index_asset.py, scripts/materialize_name_index.py]
  modified: [src/dagster_pipeline/definitions.py]

key-decisions:
  - "55.5M filtered rows (not 20-30M estimate) — common names match across many tables"
  - "MCP is read-only — materialization done via Python script using _connect_ducklake() helper"
  - "44 of 47 tables contribute to filtered index (3 had no cross-table matches)"
  - "Plain @asset instead of @dlt_assets — SQL transformation, not ingestion"

patterns-established:
  - "_connect_ducklake() helper for reusable DuckLake connections"
  - "scripts/ directory for one-shot materialization scripts"

issues-created: []

duration: 11min
completed: 2026-03-17
---

# Phase 3 Plan 1: Materialized Name Index Summary

**Materialized 55.5M cross-referenced name records from 44 source tables to lake.federal.name_index via Dagster @asset + Python script**

## Performance

- **Duration:** 11 min
- **Started:** 2026-03-17T13:51:08Z
- **Completed:** 2026-03-17T14:01:57Z
- **Tasks:** 2
- **Files modified:** 3 (2 created, 1 modified)

## Accomplishments
- Created Dagster @asset (`name_index`) with `_connect_ducklake()` helper for DuckLake connections
- Built full UNION ALL extraction (53 SQL queries from 47 tables) into materialization pipeline
- Materialized `lake.federal.name_index` with 55.5M rows filtered from 111M raw (2+ source table cross-refs)
- 44 source tables contribute — top: acris_parties (11.8M), dob_permit_issuance (8.7M), fec_contributions (5.6M)
- Data quality validated: 0 NULL/empty names, cross-referencing confirmed (SMITH/JOHN in 10+ tables)

## Task Commits

1. **Task 1: Create name index Dagster asset** - `bdec762` (feat)
2. **Task 2: Materialize to DuckLake and validate** - `d43533c` (feat)

**Plan metadata:** (this commit)

## Files Created/Modified
- `src/dagster_pipeline/defs/name_index_asset.py` — Dagster @asset with _connect_ducklake(), _build_union_sql(), three-step materialization
- `scripts/materialize_name_index.py` — One-shot script for running materialization outside Dagster
- `src/dagster_pipeline/definitions.py` — Imported and registered name_index asset

## Decisions Made
- Used plain Dagster @asset instead of @dlt_assets — this is a SQL transformation, not an ingestion
- Created scripts/materialize_name_index.py because DuckDB MCP is read-only (cannot CREATE TABLE)
- Kept raw→filtered→cleanup three-step approach for memory safety
- 55.5M filtered rows (higher than 20-30M estimate) — correct behavior, common names match widely

## Deviations from Plan

### MCP Read-Only Limitation

Plan specified materializing via `mcp__duckdb__sql_query`, but MCP tool is read-only (no DDL). Created `scripts/materialize_name_index.py` using the same `_connect_ducklake()` helper from the asset module. Functionally equivalent — the table is materialized and validated.

### Higher Filtered Count

Plan estimated 20-30M filtered rows; actual is 55.5M. This is correct — the 2+ source table filter on (last_name, first_name) pairs pulls in all rows for any name that appears in multiple tables, and common names like SMITH/JOHN appear across many tables.

---

**Total deviations:** 1 workaround (MCP read-only), 1 estimate correction
**Impact on plan:** Both tasks completed successfully. MCP workaround is cleaner (reusable script). Higher row count doesn't affect Phase 4 (Splink handles it via batching).

## Issues Encountered
None

## Next Phase Readiness
- `lake.federal.name_index` materialized with 55.5M rows — ready for Phase 4 (Splink Model Training)
- `_connect_ducklake()` helper ready for reuse in entity_resolution.py refactor
- Dagster asset registered for future re-materialization
- No blockers

---
*Phase: 03-materialized-name-index*
*Completed: 2026-03-17*
