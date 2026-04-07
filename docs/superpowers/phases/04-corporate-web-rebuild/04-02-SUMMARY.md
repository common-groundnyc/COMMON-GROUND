# 04-02 Summary: Corporate Web Verification

phase: 04-corporate-web-rebuild
plan: 02
subsystem: duckpgq-corporate
tags: [duckpgq, mcp, graph, corporate, shell-detector, verification]

## Objective

Full MCP tool re-test of all 12 graph-dependent tools after Phase 4 graph_corps expansion, plus graph table stats collection.

## Test Results (12/12 pass)

| # | Tool | Input | Result | Notes |
|---|------|-------|--------|-------|
| 1 | `corporate_web` | "Perlbinder" | PASS | 9 direct corps, 23 connected, 10 people found |
| 2 | `shell_detector` | (defaults) | PASS | 9 clusters returned (sizes 5-8) |
| 3 | `llc_piercer` | "NIVISA REALTY LLC" | PASS | NYS corp found, 6 ACRIS transactions, attorney Steven Lovitch |
| 4 | `contractor_network` | "PERLBINDER" | PASS | No contractor found (valid — Perlbinder is a landlord, not licensed contractor) |
| 5 | `worst_landlords` | (defaults) | PASS | 25 ranked landlords returned, top: QUEENS FRESH MEADOWS LLC |
| 6 | `landlord_network` | "1000670001" | PASS | No ownership graph (valid — 50 John St is commercial, not HPD-registered) |
| 7 | `ownership_clusters` | (defaults) | PASS | Large output (55KB), clusters up to 180 buildings |
| 8 | `building_profile` | "1000670001" | PASS | 50 John St, 44 stories, C5-5 zoning, owner 59 MAIDEN LANE ASSOC LLC |
| 9 | `property_history` | "1000670001" | PASS | 103 documents, 2 deeds, 17 mortgages — **required bug fix** |
| 10 | `entity_xray` | "Barton Perlbinder" | PASS | 20 campaign donations, 12 OATH hearings, 15 DOB permits, 5 Splink clusters |
| 11 | `flipper_detector` | (defaults) | PASS | 50 flips, $8.5B total profit, 8 serial flippers identified |
| 12 | `pay_to_play` | "Catsimatidis" | PASS | $21.9M total, 129 transactions, donations to 20+ candidates, Red Apple Group |

## Bug Fix

**property_history date sort crash** — `'<' not supported between instances of 'datetime.date' and 'str'`

Root cause: `sorted(deeds, key=lambda x: x["date"] or "")` compares `datetime.date` objects with `""` string when some dates are None.

Fix: Changed to `lambda x: str(x["date"]) if x["date"] else ""` on all 4 sort lambdas (lines 8144, 8159, 8172, 8202 of mcp_server.py).

Applied via SSH, container restarted. Re-test confirmed fix.

## Graph Table Stats

| Table | Rows |
|-------|------|
| graph_corps | 268,449 |
| graph_corp_people | 220,811 |
| graph_corp_officer_edges | 408,243 |
| graph_corp_shared_officer | 572,401 |
| graph_buildings | 348,207 |
| graph_owners | 194,488 |
| graph_owns | 194,621 |
| graph_shared_owner | 489 |

## Key Observations

- graph_corps expanded 2.5x from 105K (Phase 3) to 268K (Phase 4) via 4-source staging
- Corporate graph is the largest: 572K shared officer edges vs 489 shared owner edges
- All 12 tools return valid data or valid "not found" responses — no crashes
- property_history date sort bug was a regression from the Phase 3 rebuild (previously fixed in 03-02)

## Duration

~25 min (including server restart wait time for graph cache rebuild)
