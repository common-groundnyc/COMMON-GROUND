---
phase: 03-ownership-rebuild
plan: 02
subsystem: duckpgq-housing
tags: [duckpgq, mcp, graph, ownership, pluto, housing, verification]

requires:
  - phase: 03-ownership-rebuild/01
    provides: Name-based owners, PLUTO buildings, ownership edges, mega-owner filter
provides:
  - All 10 MCP tools verified passing
  - graph_corps deduplication (422K -> 105K unique)
  - property_history date sort fix
  - Phase 3 complete, ready for Phase 4
affects: [04-corporate-web-rebuild]

tech-stack:
  added: []
  patterns: [graph-cache-invalidation, row-number-dedup, safe-date-sort]

key-files:
  modified:
    - /opt/common-ground/duckdb-server/mcp_server.py

key-decisions:
  - "Graph Parquet cache must be deleted to force SQL rebuild after code changes — restart alone loads stale cache"
  - "graph_corps ROW_NUMBER dedup by dos_id — nys_corporations has duplicate rows with different date string formats"
  - "property_history date sort uses str(x['date']) for safe comparison — mixed datetime.date and None types in ACRIS data"
  - "5,209 violation orphans (0.048%) accepted — mostly BBL '0' and BBLs in HPD but not in PLUTO; not worth filtering"
  - "graph_owns uncapped by design — mega-owner cap only applied to graph_shared_owner self-join to prevent O(n^2)"

patterns-established:
  - "After docker cp + restart, check if graph tables loaded from cache vs rebuilt from SQL"
  - "Use Python patch scripts for multi-line edits, verify with grep after deployment"

duration: ~60 min
completed: 2026-03-26
---

## Summary

Verified all graph tables from 03-01 rebuild and ran full MCP tool test suite (10 tools). Found and fixed two regressions: graph_corps had 4x duplicate rows (422K -> 105K unique via ROW_NUMBER dedup), and property_history had a date type comparison crash for BBLs with mixed date formats. Both fixes deployed via docker cp + restart with graph cache invalidation.

## Graph Verification Results

| Table | Count | Status |
|-------|-------|--------|
| graph_buildings | 870,915 | OK |
| graph_owners | 823,084 | OK |
| graph_owns | 1,484,668 | OK |
| graph_shared_owner | 356,395 | OK (was 489 pre-Phase 3) |
| graph_has_violation | 10,763,276 | OK |
| graph_has_violation orphans | 5,209 (0.048%) | Accepted |
| graph_corps | 105,649 | Fixed (was 422,568 dupes) |
| Max buildings per owner | 172,048 | Known (uncapped in graph_owns, capped in shared_owner) |

## MCP Tool Test Results

| # | Tool | Result | Notes |
|---|------|--------|-------|
| 1 | worst_landlords() | PASS | Returns name-based owners, mega-owners visible |
| 2 | landlord_network("1000670001") | PASS | Large portfolio result |
| 3 | ownership_clusters(min_buildings=10) | PASS | Finds clusters via shared names |
| 4 | building_profile("2042410037") | PASS | Returns "not found" for BBLs not in HPD (expected) |
| 5 | enforcement_web("1000670001") | PASS | Multi-agency data returned |
| 6 | shell_detector(min_corps=5) | PASS | Fixed after graph_corps dedup |
| 7 | property_history("1000670001") | PASS | Fixed date sort crash |
| 8 | entity_xray("Barton Perlbinder") | PASS | 23 sources searched, rich results |
| 9 | flipper_detector(borough="Manhattan") | PASS | 50 flips found |
| 10 | pay_to_play("Catsimatidis") | PASS | Political influence network returned |

**All 10 tools: 10/10 PASS**

## Fixes Applied

1. **graph_corps dedup** (lines 1582-1585): Added `ROW_NUMBER() OVER (PARTITION BY dos_id) ... WHERE rn = 1` to eliminate duplicate corporation rows caused by different date string formats in nys_corporations source data.

2. **property_history date sort** (lines 8164, 8179, 8192, 8222): Changed `x["date"] or ""` to `str(x["date"]) if x["date"] else ""` to prevent TypeError when comparing datetime.date with str for records with mixed/null date fields.

3. **Graph cache invalidation**: Deleted `/data/common-ground/graph-cache/` before restart to force SQL rebuild instead of loading stale Parquet cache.

## Phase 3 Before/After

| Metric | Before Phase 3 | After Phase 3 | Change |
|--------|---------------|---------------|--------|
| graph_buildings | 348,000 | 870,915 | +150% |
| graph_owners | 194,000 | 823,084 | +324% |
| graph_owns | 195,000 | 1,484,668 | +661% |
| graph_shared_owner | 489 | 356,395 | +72,779% |
| graph_corps | 312,000 (duped) | 105,649 (unique) | Deduped |
| MCP tools passing | 7/10 | 10/10 | All fixed |
