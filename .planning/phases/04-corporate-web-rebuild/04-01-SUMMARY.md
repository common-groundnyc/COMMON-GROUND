---
phase: 04-corporate-web-rebuild
plan: 01
type: summary
status: complete
started: 2026-03-26T09:45:00Z
completed: 2026-03-26T10:15:00Z
duration: ~90 min (including OOM recovery)
---

# 04-01 Summary: Corporate Web Expansion

## Objective

Expand graph_corps from 105K (HPD/PLUTO name match only) to 250K+ by adding ACRIS party corps, OATH respondent corps, and campaign employer corps as matching sources.

## Results

### Before/After Counts

| Table | Before | After | Change |
|-------|--------|-------|--------|
| graph_corps | 105,649 | 268,449 | +154% |
| graph_corp_people | 83,296 | 220,811 | +165% |
| graph_corp_officer_edges | 146,963 | 408,243 | +178% |
| graph_corp_shared_officer | 182,612 | 572,401 | +213% |

All dos_ids unique (268,449 = 268,449 distinct).

### Staging Stats

- Source 1 (HPD/PLUTO owners): 74,586 corp names
- Source 2 (ACRIS grantees, party_type=2): +784,097 names
- Source 3 (OATH respondents): +816,307 names
- Source 4 (Campaign employers): +49,543 names
- After dedup: 1,525,617 unique corp name targets
- Matched against 16.6M NYS corps: 268,449 unique corps

### MCP Tool Verification

| # | Tool | Result |
|---|------|--------|
| 1 | corporate_web("Perlbinder") | PASS - 9 direct corps, 23 connected |
| 2 | shell_detector() | PASS - 77K corps in largest WCC cluster |
| 3 | entity_xray("Barton Perlbinder") | PASS - contributions + OATH + DOB |
| 4 | worst_landlords() | PASS - no regression |
| 5 | building_profile("1000670001") | PASS - 50 John Street |
| 6 | llc_piercer("NIVISA REALTY LLC") | PASS - NYS corp + ACRIS |

## Implementation

### Strategy: Incremental Temp Table Staging

Instead of a single UNION across 4 large DuckLake tables (which OOMs), each source is staged independently:

1. CREATE TEMP TABLE from owners (smallest, ~75K)
2. Stage ACRIS grantees into separate temp, INSERT into main temp, DROP
3. INSERT OATH respondents directly
4. INSERT campaign employers directly
5. Dedup into final temp table
6. Single IN-match against nys_corporations with ROW_NUMBER dedup

The ACRIS scan is limited to `party_type='2'` (grantees only) to reduce the 46M row scan to ~23M.

### OOM Recovery

The initial UNION-based approach (v1) OOM-killed the server, taking down SSH. Recovery required:
- Hetzner rescue mode to access disk
- Finding the WORKING mcp_server.py (with reconnect logic) in Docker overlay layers
- Disabling container auto-restart via hostconfig.json edit
- Applying the safe incremental staging patch (v3) to the working version
- Reducing container memory limit from 28GB to 20GB

### Files Changed

- `/opt/common-ground/duckdb-server/mcp_server.py` (remote, line ~1476)
  - Replaced inline HPD+PLUTO IN subqueries with 4-source incremental temp table staging
  - Added ROW_NUMBER dedup for duplicate dos_ids

## Decisions

- **ACRIS grantees only (party_type=2)**: Full ACRIS scan (46M rows) is too expensive. Grantees are the relevant party (buyers/recipients).
- **Incremental staging over UNION**: Large UNION across DuckLake tables causes OOM. INSERT INTO temp table + DROP is safe.
- **Container memory limit 20GB**: Reduced from 28GB to prevent system-wide OOM on a 32GB server.
- **Working version from Docker overlay**: The image layer has old code. Working version (with reconnect, graph-cache, etc.) lives in Docker overlay layers from previous docker-cp operations.

## Deferred

- graph_corps currently has no dedup in the ORIGINAL image. The ROW_NUMBER dedup was added via docker-cp and must be re-applied after any image rebuild.
- Container health check fails (curl not in image) -- cosmetic only.
- 04-02 plan may be unnecessary: downstream tables (people, edges, shared) are already rebuilt automatically by the expanded graph_corps.
