---
phase: 01-name-column-discovery
plan: "01"
subsystem: entity-resolution
tags: [duckdb, splink, name-matching, data-discovery, sql]

requires:
  - phase: none
    provides: first phase
provides:
  - NAME_REGISTRY with 47 tables classified by extraction pattern
  - get_extraction_sql() for all 6 pattern types
  - NameSource dataclass for downstream phases
affects: [02-universal-name-extractor, 03-materialized-name-index]

tech-stack:
  added: []
  patterns: [dataclass registry pattern, SQL extraction per pattern type]

key-files:
  created: [src/dagster_pipeline/sources/name_registry.py]
  modified: [src/dagster_pipeline/sources/entity_resolution.py]

key-decisions:
  - "47 tables (not 50) — nys_campaign_finance excluded (0 rows in city_government copy; federal copy retained)"
  - "6 pattern types: STRUCTURED (21), COMBINED_SPACE (12), COMBINED_COMMA (6), OWNER (4), RESPONDENT (2), MULTI (3)"
  - "MULTI tables produce multiple extraction SQL statements (53 total from 47 tables)"

patterns-established:
  - "NameSource dataclass as the canonical table-to-name-columns mapping"
  - "get_extraction_sql() generates pattern-specific SELECT for any NameSource"

issues-created: []

duration: 8min
completed: 2026-03-17
---

# Phase 1 Plan 1: Name Column Discovery Summary

**Auto-discovered 47 person-name tables across the DuckLake, classified into 6 extraction patterns, built registry module with SQL generation**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-17T13:21:14Z
- **Completed:** 2026-03-17T13:29:14Z
- **Tasks:** 5
- **Files modified:** 2

## Accomplishments
- Discovered 47 tables with person-name columns across 10 schemas (from 300+ total tables)
- Classified all tables into 6 extraction patterns: STRUCTURED (21), COMBINED_SPACE (12), COMBINED_COMMA (6), OWNER (4), RESPONDENT (2), MULTI (3)
- Built `name_registry.py` with `NameSource` dataclass, `NAME_REGISTRY` list, and `get_extraction_sql()` function
- Validated all 47 entries against live lake data (SELECT LIMIT 5 per table)
- Replaced hardcoded 15-table `NAME_SOURCES` in `entity_resolution.py` with registry import

## Task Commits

1. **Tasks 1-3: Discovery, classification, registry build** - `ab4f001` (feat)
2. **Task 4: Validate registry against live data** - `5f53478` (fix)
3. **Task 5: Update entity_resolution.py** - `30851e0` (refactor)

**Plan metadata:** pending (this commit)

## Files Created/Modified
- `src/dagster_pipeline/sources/name_registry.py` — New registry module (520 lines), 47 NameSource entries with pattern classification and address columns
- `src/dagster_pipeline/sources/entity_resolution.py` — Replaced hardcoded NAME_SOURCES with `from .name_registry import NAME_REGISTRY`

## Decisions Made
- Excluded `city_government.nys_campaign_finance` (0 rows) — the `federal.nys_campaign_finance` copy with 7.1M rows is retained
- Final count 47 tables (within expected 45-55 range)
- Tasks 1-3 committed together since all feed into the single registry file

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed empty table from registry**
- **Found during:** Task 4 (validation)
- **Issue:** `city_government.nys_campaign_finance` returned 0 rows
- **Fix:** Excluded from registry; federal copy retained
- **Committed in:** `5f53478`

---

**Total deviations:** 1 auto-fixed (empty table), 0 deferred
**Impact on plan:** Minimal — 47 instead of 48 tables. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- NAME_REGISTRY with 47 tables ready for Phase 2 (Universal Name Extractor)
- All pattern types classified — extractor needs to handle 6 patterns
- Address columns tagged for Splink matching in Phase 4
- No blockers

---
*Phase: 01-name-column-discovery*
*Completed: 2026-03-17*
