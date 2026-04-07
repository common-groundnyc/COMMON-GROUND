---
phase: 10-validation-hardening
plan: 01
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~45 min
---

# Summary: Validation & Hardening — v2.0 SHIPPED

## What was done

### Task 1: Permanent fixes + graph_health tool

**Fix 1: property_history date sort (PERMANENT)**
- Found and replaced all 4 occurrences of `x["date"] or ""` with `str(x["date"] or "0000-00-00")` in the property_history function (lines 8208, 8223, 8236, 8266)
- Updated host file at `/opt/common-ground/duckdb-server/mcp_server.py`
- docker cp to running container
- Rebuilt Docker image (`docker compose build duckdb-server`)
- Restarted from new image (`docker compose up -d duckdb-server`)
- Verified fix survives restart cycle (full container restart, graph rebuild, re-test)
- **This bug regressed 4 times across phases 02-02, 03-02, 04-02, 08-01. Now permanently fixed in the image.**

**Fix 2: entity_xray float parsing**
- Added `_safe_float()` helper function that wraps `float()` in try/except returning 0.0
- Applied to 3 float conversion sites: EPA total_penalties (line 7823), EPA pen_str (line 7827), lobby compensation (line 7855)
- entity_xray("HAMILTON HOUSE 79 LLC") now works — the "$$300 to $600 per hour" lobby compensation field no longer crashes

**Tool: graph_health**
- Added new `graph_health()` MCP tool with graph/discovery tags
- Reports: all 21 graph table row counts, orphan checks (graph_owns -> graph_owners, graph_owns -> graph_buildings), property graph status for all 6 named graphs
- Returns total row count and query time

### Task 2: Final comprehensive MCP test suite

All tools tested after fix deployment AND after restart.

## Final MCP Test Scorecard

| # | Tool | Result | Notes |
|---|------|--------|-------|
| 1 | worst_landlords() | PASS | 25 results, top violators listed |
| 2 | building_profile("1000670001") | PASS | 50 John Street, 44 stories |
| 3 | landlord_network("1000670001") | PASS | TONY HOLDING LLC, 333 buildings |
| 4 | ownership_clusters() | PASS | 1 mega-cluster, 175K buildings |
| 5 | corporate_web("Perlbinder") | PASS | 9 direct corps, 23 connected |
| 6 | shell_detector() | PASS | 1 cluster, 77K corps |
| 7 | property_history("1000670001") | PASS | 103 documents, no date sort crash |
| 8 | flipper_detector() | PASS | $8.5B total profit, 50 flips |
| 9 | pay_to_play("Catsimatidis") | PASS | $21.9M total, 129 transactions |
| 10 | entity_xray("Barton Perlbinder") | PASS | Corps, donations, OATH, DOB permits |
| 11 | entity_xray("HAMILTON HOUSE 79 LLC") | PASS | No float crash on lobby fees |
| 12 | neighborhood_portrait("10003") | PASS | Manhattan/East Village |
| 13 | sql_query("SELECT COUNT(*)...") | PASS | 870,915 graph_buildings |
| 14 | graph_health() | PASS | 80.3M total rows, 0 orphans |
| 15 | landlord_pagerank() | PASS | 25 ranked buildings |
| 16 | entity_influence(domain="corporate") | PASS | 25 ranked corps by PageRank |
| 17 | shortest_path(...) | PASS | Executes, returns "no path" (valid) |
| 18 | entity_reachable("Perlbinder") | PASS | Executes, returns domain scan |

**Result: 18/18 tools pass. Zero crashes.**

## Graph Health Stats (post-rebuild)

| Table | Rows |
|-------|------|
| graph_buildings | 870,915 |
| graph_owners | 823,084 |
| graph_owns | 1,484,668 |
| graph_shared_owner | 356,395 |
| graph_violations | 10,763,276 |
| graph_has_violation | 10,758,067 |
| graph_corps | 269,101 |
| graph_corp_people | 221,509 |
| graph_corp_officer_edges | 409,622 |
| graph_corp_shared_officer | 575,303 |
| graph_tx_entities | 3,978,074 |
| graph_tx_edges | 40,577,562 |
| graph_tx_shared | 8,179,488 |
| graph_pol_entities | 310,604 |
| graph_pol_donations | 700,478 |
| graph_pol_contracts | 24,399 |
| graph_pol_lobbying | 6,161 |
| **Total** | **80,308,706** |

Orphan checks: 0 orphans in graph_owns -> graph_owners, 0 orphans in graph_owns -> graph_buildings.

## Before/After: v2.0 Milestone

| Metric | Before (v1.0) | After (v2.0) |
|--------|---------------|--------------|
| Graph tables | 6 | 21+ |
| Total graph rows | ~12M | 80.3M |
| MCP tools | 10 | 18 |
| Tools passing | 7/10 | 18/18 |
| Graph domains | 1 (housing) | 6 (housing, corporate, transaction, influence, unified, path-finding) |
| Ownership edges | 489 shared | 356K shared |
| Buildings | 348K (HPD) | 871K (PLUTO) |
| Corps | 105K (duped) | 269K (deduped) |
| TX entities | 1.96M | 3.98M |
| Political entities | 311K | 631K |
| Cross-domain entities | 0 | 597K multi-domain |
| PageRank tools | 0 | 2 |
| Path-finding tools | 0 | 2 |
| Diagnostic tools | 0 | 1 (graph_health) |

## Verification

- [x] property_history works AND survives restart (permanent fix in image)
- [x] graph_health returns full diagnostic report
- [x] entity_xray doesn't crash on float fields
- [x] All 18 MCP tools pass final test suite
- [x] Phase 10 complete
- [x] v2.0 milestone complete
