# Phase 1 Plan 2: Graph Expansion Plan Summary

**Current graphs use <5% of available entity data. Expansion to ~1.1GB CSR (4% of 28GB limit) unlocks 10x coverage across all domains.**

## Accomplishments

- Mapped coverage gaps for all 5 existing graphs
- Identified 11 high-value ungraphed tables (42.6M+ rows)
- Proposed 2 new graphs: Enforcement (oath_hearings + violations) and Civic (311 complaints)
- Calculated memory budget: all expanded graphs fit in ~1.1GB (4% of 28GB)
- Mapped specific tables/columns to Phases 3-6
- Documented resolved_entities integration strategy for Phase 7

## Key Findings

- **Housing graph**: 489 shared edges → 31K+ with name-based rebuild. PLUTO adds +511K buildings.
- **Corporate web**: 1.3% coverage → 6%+ by adding ACRIS/OATH/DOB/campaign corps. Dedup fixes shell_detector.
- **Influence network**: 311K entities → 2M+ with NYS campaign finance (69M), FEC (44M), lobbying (17.3M)
- **oath_hearings (21.6M)**: Most entity-dense table, NOT in any graph. Should be P0.
- **Memory**: All graphs fit in ~1.1GB. No memory concerns even after full expansion.

## Files Created/Modified

- `.planning/phases/01-data-audit/graph_expansion_plan.md`

## Decisions Made

- P0: oath_hearings integration, PLUTO building expansion, graph_corps dedup, S3 credential fix, duckpgq loading fix
- P1: ACRIS corp expansion, NYS/FEC political data, personal property transactions, citywide payroll
- P2: Marriage/death certificates, attorney/broker registrations, contractor registry
- New Enforcement graph proposed (P0)
- New Civic graph proposed (P1)

## Issues Encountered

- **S3 credential breakage**: DuckLake ignores both `CREATE SECRET` and `SET s3_*` for its internal file reads. The `s3://ducklake/data/` path resolves to `ducklake.s3.amazonaws.com` instead of MinIO. Workaround: use graph cache. Root fix needed in Phase 2 (likely needs DuckLake ATTACH with storage credentials or Dockerfile fix).
- **duckpgq not loading**: Extension is installed but `LOAD` silently fails after container rebuild. Likely version mismatch between core and community repos. Phase 2 fix.

## Next Phase Readiness

**Phase 1 complete.** Phase 2 (Bug Fixes) should proceed next — it now includes:
1. S3 credential fix (launch blocker)
2. duckpgq extension loading (launch blocker)
3. property_history date sort
4. graph_corps deduplication
5. graph_has_violation orphans

Phases 3-6 should reference `graph_expansion_plan.md` for specific tables, columns, and priorities.
