---
phase: 02-universal-name-extractor
plan: "01"
subsystem: entity-resolution
tags: [duckdb, splink, name-extraction, data-validation, sql]

requires:
  - phase: 01-name-column-discovery
    provides: NAME_REGISTRY with 47 tables, get_extraction_sql(), NameSource dataclass
provides:
  - Correct parse-order-aware COMBINED_SPACE extraction (last_name_first flag)
  - LLC entity filtering for mixed person/business tables (needs_llc_filter flag)
  - Validated extraction across all 47 tables (120.7M rows total)
affects: [03-materialized-name-index, 04-splink-model-training]

tech-stack:
  added: []
  patterns: [per-table parse order flags on dataclass, LLC filter injection in SQL generation]

key-files:
  created: []
  modified: [src/dagster_pipeline/sources/name_registry.py]

key-decisions:
  - "120.7M total rows (not 50-70M estimate) — OWNER tables much larger than expected (acris_parties: 37M)"
  - "civil_list and nys_daily_corp_filings confirmed FIRSTNAME LASTNAME — no last_name_first needed"
  - "nys_notaries confirmed space-separated FIRSTNAME LASTNAME — kept as COMBINED_SPACE"
  - "3 tables set last_name_first=True: nys_cosmetology_licenses, nys_re_appraisers, nys_re_brokers"
  - "2 tables set needs_llc_filter=True: nys_re_brokers, nys_child_care"

patterns-established:
  - "Boolean flags on NameSource for per-table extraction behavior"

issues-created: []

duration: 6min
completed: 2026-03-17
---

# Phase 2 Plan 1: Universal Name Extractor Summary

**Fixed COMBINED_SPACE parse order with per-table last_name_first flag, added LLC filtering, validated all 47 tables producing 120.7M extractable rows**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-17T13:35:55Z
- **Completed:** 2026-03-17T13:41:59Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments
- Added `last_name_first` and `needs_llc_filter` boolean fields to NameSource dataclass
- Fixed 3 COMBINED_SPACE tables that were swapping first/last names (nys_cosmetology_licenses, nys_re_appraisers, nys_re_brokers)
- Added LLC entity filtering for 2 mixed person/business tables (nys_re_brokers, nys_child_care)
- Validated all 47 tables (53 SQL queries) against live lake — 47/47 pass
- Integration test: full UNION ALL produces 120.7M rows across all sources

## Task Commits

1. **Task 1: Add parse order and LLC filter** - `2ba1f40` (fix)
2. **Task 2: Validate extraction quality** - no code changes (validation only)
3. **Task 3: Integration test — full UNION ALL** - no code changes (validation only)

**Plan metadata:** (this commit)

## Files Created/Modified
- `src/dagster_pipeline/sources/name_registry.py` — Added `last_name_first` and `needs_llc_filter` fields to NameSource, updated 5 registry entries, fixed COMBINED_SPACE branch in `get_extraction_sql()`

## Decisions Made
- Confirmed civil_list and nys_daily_corp_filings are FIRSTNAME LASTNAME (not last_name_first) after checking live data
- Confirmed nys_notaries is space-separated — kept as COMBINED_SPACE, no reclassification
- Total extraction is 120.7M rows (2x the 50-70M estimate — acris_parties alone is 37M)

## Deviations from Plan

### Estimate Correction

Row count estimate was 50-70M; actual is 120.7M. This is because OWNER-pattern tables (acris_parties: 37M, acris_pp_parties: 8M) are much larger than anticipated. Not a problem — Phase 5 batch processor handles this via last-name batching.

### Fewer Commits Than Expected

Tasks 2 and 3 were validation-only (no code changes), so only 1 commit was produced instead of 3.

---

**Total deviations:** 0 auto-fixed, 0 deferred
**Impact on plan:** Plan executed as written. Row count estimate updated for downstream phases.

## Issues Encountered
None

## Next Phase Readiness
- All 47 tables extract correctly — ready for Phase 3 (Materialized Name Index)
- Total extraction: 120.7M rows (will filter to ~22M+ after 2+ table dedup)
- Top sources: acris_parties (37M), oath_hearings (16M), nys_campaign_finance (7M), citywide_payroll (6.7M)
- No blockers

---
*Phase: 02-universal-name-extractor*
*Completed: 2026-03-17*
