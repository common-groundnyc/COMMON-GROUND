---
phase: 03-ownership-rebuild
plan: 01
subsystem: duckpgq-ownership
tags: [duckpgq, mcp, graph, ownership, pluto, name-matching]

requires:
  - phase: 02-bug-fixes/02
    provides: All graph tools passing, graph cache infrastructure
provides:
  - Name-based ownership graph (823K owners, was 194K)
  - HPD+PLUTO building union (871K buildings, was 348K)
  - 1.48M ownership edges (was 195K)
  - Zero null/empty owner names, zero orphaned edges
affects: [03-ownership-rebuild/02, 04-corporate-web-rebuild]

tech-stack:
  added: []
  patterns: [staged-pluto-extraction, mega-owner-filter, left-bbl-truncation]

key-files:
  modified:
    - /opt/common-ground/duckdb-server/mcp_server.py

key-decisions:
  - "Name-based PK for graph_owners: UPPER(TRIM(owner_name)) replaces registrationid — enables cross-source owner matching"
  - "PLUTO BBL is stored as float string ('5010730042.00000000') — use LEFT(bbl, 10) to extract 10-char BBL"
  - "Staged PLUTO extraction via temp tables (_pluto_owners, _pluto_buildings, _pluto_owns) — avoids DuckDB reading full PLUTO table during UNION"
  - "Mega-owner filter on graph_shared_owner: BETWEEN 2 AND 100 buildings — prevents O(n^2) explosion from government/institutional owners"
  - "DuckDB memory_limit increased 8GB → 18GB for expanded graph tables (container has 28GB)"
  - "threads reduced 8 → 4, preserve_insertion_order = false — reduces per-query memory peak"
  - "max_temp_directory_size increased 50GB → 150GB for spill-to-disk during large joins"

patterns-established:
  - "Stage DuckLake reads as temp tables before UNION — prevents full-table scans of wide source tables"
  - "Filter self-joins with HAVING COUNT(DISTINCT) cap to prevent combinatorial explosion"
  - "Use LEFT(col, N) for float-encoded BBL columns in PLUTO"
  - "Configuration options (memory_limit, threads, preserve_insertion_order) must be SET at startup — cannot be changed after connection is locked"

duration: 90min
completed: 2026-03-26
---

# Phase 3 Plan 1: Core Table Rebuilds Summary

**Rebuilt graph_owners from registrationid-based to name-based PK, expanded graph_buildings from HPD-only to HPD+PLUTO union, rebuilt graph_owns with name-based edges. All referential integrity verified.**

## Performance

- **Duration:** ~90 min (including 6 OOM debug cycles)
- **Started:** 2026-03-26T04:00:00Z
- **Completed:** 2026-03-26T06:00:00Z
- **Tasks:** 2
- **Files modified:** 1 (remote)

## Accomplishments

- graph_owners: 194,488 → 823,084 (4.2x — name-based PK, HPD+PLUTO union)
- graph_buildings: 348,207 → 870,915 (2.5x — HPD+PLUTO union)
- graph_owns: ~195K → 1,484,668 (7.6x — name-based edges, HPD+PLUTO)
- graph_shared_owner: ~31K → 356,395 (11.5x — with mega-owner filter)
- Zero null/empty owner_name values
- Zero orphaned edges (owner_id → owners, bbl → buildings)
- registrationid removed from nyc_housing property graph PROPERTIES

## Task Commits

No local git commits — all changes were remote on Hetzner server via SSH.

1. **Task 1: Expand graph_buildings to HPD+PLUTO union** — Python patch script
2. **Task 2: Rebuild graph_owners/graph_owns with name-based PK** — Python patch script

## Changes Applied

### Change 1: graph_owners (name-based PK)
- Old: registrationid-based from HPD registration contacts only (194K)
- New: UPPER(TRIM(owner_name)) from HPD contacts + PLUTO ownername (823K)
- Staged: PLUTO owners extracted to temp table first to avoid full-table scan

### Change 2: graph_buildings (HPD+PLUTO union)
- Old: HPD jurisdiction only (348K BBLs)
- New: HPD + PLUTO union with GROUP BY bbl (871K BBLs)
- Staged: PLUTO buildings extracted to temp table first
- PLUTO BBL format fix: LEFT(bbl, 10) to handle float-encoded string

### Change 3: graph_owns (name-based edges)
- Old: registrationid → bbl from HPD only (~195K)
- New: UPPER(TRIM(owner_name)) → bbl from HPD join + PLUTO (1.48M)
- Staged: Both HPD join and PLUTO owns extracted to temp tables
- Filtered: bbl IN (SELECT bbl FROM graph_buildings) for referential integrity

### Change 4: nyc_housing property graph
- Removed `registrationid` from graph_owners PROPERTIES (column no longer exists)

### Change 5: graph_shared_owner (mega-owner filter)
- Added HAVING COUNT(DISTINCT bbl) BETWEEN 2 AND 100 to prevent OOM on self-join
- Owners with >100 buildings (government agencies, utilities) excluded from pair generation

### Change 6: Memory/performance tuning
- memory_limit: 8GB → 18GB
- threads: 8 → 4
- preserve_insertion_order: false
- max_temp_directory_size: 50GB → 150GB

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] PLUTO BBL stored as float string**
- **Found during:** Task 1 verification (0 PLUTO buildings matched)
- **Issue:** PLUTO bbl column contains '5010730042.00000000' (19 chars), not 10-char BBL
- **Fix:** Use LEFT(bbl, 10) instead of filtering LENGTH(bbl) = 10
- **Impact:** Required extra restart cycle

**2. [Rule 3 - Blocking] DuckDB OOM on graph build (6 cycles)**
- **Found during:** First deployment (8GB limit)
- **Issue:** PLUTO expansion + name-based ownership + graph_shared_owner self-join exceeded memory
- **Fix:** Three-pronged approach:
  1. Staged temp tables for PLUTO reads (avoid full-table DuckLake scans)
  2. Mega-owner filter on shared_owner (prevent O(n^2) pairs)
  3. Memory/thread tuning (18GB limit, 4 threads, insertion order off)
- **Impact:** 6 restart cycles during debugging

**3. [Rule 2 - Info] DuckDB config options locked after startup**
- **Found during:** Dynamic SET memory_limit/preserve_insertion_order failed
- **Issue:** Config options cannot be changed after DuckDB connection is established
- **Fix:** Set all optimization options at initial connection time (both primary + reconnect paths)

---

**Total deviations:** 3 auto-fixed (2 blocking, 1 info), 0 deferred
**Impact on plan:** Extra debug cycles for OOM. No scope creep.

## Issues Encountered

- EPA facilities table still missing `lon` column — skipped during graph build (pre-existing)
- hnsw_acorn extension unavailable for DuckDB 1.5.0 on linux_amd64 (pre-existing)

## Next Step Readiness

- 03-01 complete — core tables rebuilt with name-based PK and PLUTO expansion
- Ready for 03-02: Rebuild graph_shared_owner with improved matching, verify nyc_housing + nyc_building_network graphs
- graph_shared_owner mega-owner filter (>100 buildings) may need tuning in 03-02

---
*Phase: 03-ownership-rebuild*
*Completed: 2026-03-26*
