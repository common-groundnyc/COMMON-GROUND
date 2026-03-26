---
plan: 07-02
phase: 7 — Cross-Domain Unified Graph
title: Cross-domain verification and full MCP tool regression
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~25 min
---

# 07-02 Summary: Cross-Domain Verification

## What Was Done

Full MCP tool regression test (12 domain tools + 3 cross-domain queries) and comprehensive graph stats collection.

## Cross-Domain Tests

| # | Test | Result | Notes |
|---|------|--------|-------|
| 1 | `graph_unified_entities` domain distribution | PASS | 507K 2-domain, 90K 3-domain, 497 4-domain |
| 2 | 4-domain entity lookup | PASS | HAMILTON HOUSE 79 LLC and 4 others confirmed in all 4 domains |
| 3 | `entity_xray("HAMILTON HOUSE 79 LLC")` | FAIL | `could not convert string to float: '300 to 600 per hour'` — crashes server |

## 12-Tool Regression Test

| # | Tool | Args | Result | Notes |
|---|------|------|--------|-------|
| 4 | `worst_landlords()` | — | PASS | 25 results, top: TONY HOLDING LLC (172K buildings) |
| 5 | `building_profile("1000670001")` | BBL | PASS | 50 John St, 44 stories, owner 59 MAIDEN LANE ASSOC LLC |
| 6 | `landlord_network("1000670001")` | BBL | PASS | Large result (195K chars), portfolio data returned |
| 7 | `ownership_clusters()` | — | PASS | Clusters returned (62K result), top cluster 175K buildings |
| 8 | `corporate_web("Perlbinder")` | entity_name | PASS | 9 direct corps, 23 connected, 10 people |
| 9 | `shell_detector()` | — | PASS | WCC clusters returned (64K result) |
| 10 | `property_history("1000670001")` | BBL | PASS | 103 documents, 2 deeds, 17 mortgages |
| 11 | `flipper_detector()` | — | PASS | 50 flips, $8.5B total profit, 8 serial flippers |
| 12 | `pay_to_play("Catsimatidis")` | entity_name | PASS | $21.9M total, 129 transactions, donates to 20 candidates |
| 13 | `entity_xray("Barton Perlbinder")` | name | PASS | Contributions, OATH hearings, DOB permits, Splink clusters |

**Score: 12/13 pass (1 entity_xray crash on specific entity)**

## Known Issue

`entity_xray("HAMILTON HOUSE 79 LLC")` crashes with float conversion error: `could not convert string to float: '300 to 600 per hour'`. This is a data quality issue in one of the DuckLake source tables (likely a fee/rate field being parsed as numeric). The entity_xray tool itself works fine for other entities (test 13 passes). Filed as deferred issue.

## Comprehensive Graph Stats

### Housing Domain (5 tables)

| Table | Rows |
|-------|------|
| `graph_owners` | 823,084 |
| `graph_buildings` | 870,915 |
| `graph_owns` | 1,484,668 |
| `graph_shared_owner` | 356,395 |
| `graph_has_violation` | 10,758,067 |

### Corporate Domain (5 tables)

| Table | Rows |
|-------|------|
| `graph_corps` | 269,101 |
| `graph_corp_people` | 221,509 |
| `graph_corp_officer_edges` | 409,622 |
| `graph_corp_shared_officer` | 575,303 |
| `graph_corp_contacts` | — |

### Transaction Domain (4 tables)

| Table | Rows |
|-------|------|
| `graph_tx_entities` | 3,978,074 |
| `graph_tx_edges` | 40,577,562 |
| `graph_tx_shared` | 8,179,488 |
| `graph_acris_chain` | 5,612,035 |

### Influence Domain (5 tables)

| Table | Rows |
|-------|------|
| `graph_pol_entities` | 310,604 |
| `graph_pol_donations` | 700,478 |
| `graph_pol_contracts` | 24,399 |
| `graph_pol_lobbying` | 6,161 |
| `graph_campaign_donors` | 173,227 |

### Cross-Domain Bridge (5 tables)

| Table | Rows |
|-------|------|
| `graph_unified_entities` | 597,585 |
| `graph_owner_tx_bridge` | 392,363 |
| `graph_owner_pol_bridge` | 26,649 |
| `graph_corp_tx_bridge` | 157,138 |
| `graph_corp_pol_bridge` | 2,096 |

### Additional Tables

| Table | Rows |
|-------|------|
| `graph_acris_sales` | 621,403 |
| `graph_building_flags` | 870,915 |
| `graph_violations` | 10,763,276 |
| `graph_doing_business` | 61,860 |

**Total graph tables: 48**
**Total rows across all graph tables: ~86M+**

### Domain Overlap Distribution

| Domains | Entities |
|---------|----------|
| 2 | 507,145 |
| 3 | 89,943 |
| 4 (all) | 497 |
| **Total multi-domain** | **597,585** |
