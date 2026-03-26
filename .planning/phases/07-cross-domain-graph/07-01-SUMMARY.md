---
plan: 07-01
phase: 7 — Cross-Domain Unified Graph
title: Cross-domain bridge tables + unified property graph
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~35 min
---

# 07-01 Summary: Cross-Domain Bridge Tables

## What Was Done

Built 4 cross-domain bridge tables and 1 unified entity summary, then created a unified property graph (`nyc_unified`) connecting housing, transaction, corporate, and influence domains.

### Bridge Tables Created

| Bridge | Join Key | Rows |
|--------|----------|------|
| `graph_owner_tx_bridge` | owner_id = tx entity_name | 392,363 |
| `graph_owner_pol_bridge` | owner_id = pol entity_name | 26,649 |
| `graph_corp_pol_bridge` | UPPER(TRIM(corp name)) = pol entity_name | 2,096 |
| `graph_corp_tx_bridge` | UPPER(TRIM(corp name)) = tx entity_name | 157,138 |
| `graph_unified_entities` | Multi-domain summary | 597,585 |

### Domain Overlap Distribution

| Domains | Entities |
|---------|----------|
| 2 | 507,145 |
| 3 | 89,943 |
| 4 | 497 |

497 entities appear in ALL 4 domains (housing + corporate + transaction + influence).

### Unified Property Graph (`nyc_unified`)

5 vertex tables (Owner, Building, TxEntity, Corp, PoliticalEntity) + 5 edge types (Owns, OwnerIsTransactor, OwnerIsPolitical, CorpIsPolitical, CorpIsTransactor). Full version created successfully on first attempt — no fallback needed.

All 5 MATCH edge types verified working:
- `(Owner)-[OwnerIsTransactor]->(TxEntity)` — landlords who transact
- `(Owner)-[OwnerIsPolitical]->(PoliticalEntity)` — landlords who donate/lobby
- `(Corp)-[CorpIsPolitical]->(PoliticalEntity)` — corps in politics
- `(Corp)-[CorpIsTransactor]->(TxEntity)` — corps in ACRIS deals
- `(Owner)-[Owns]->(Building)` — base ownership edges

## Key Decision

**Direct name matching over resolved_entities** — resolved_entities has 56M rows and would require expensive staging. Direct UPPER(TRIM()) joins on existing graph tables produce 597K multi-domain entities with zero additional DuckLake reads. The trade-off is missing fuzzy matches (e.g., "JOHN SMITH" vs "SMITH, JOHN"), but the volume (392K owner-tx links, 26K owner-pol links) proves direct matching captures the high-value connections.

## Regression Check

- `entity_xray("Barton Perlbinder")` — full cross-domain results, no crash
- `landlord_network("1000670001")` — returns large portfolio, no crash
- Core graph tables unchanged: 823K owners, 870K buildings, 10.7M violations
- All 5 MATCH query patterns return correct results

## Server Changes

- Patched `/opt/common-ground/duckdb-server/mcp_server.py` with 3 patches:
  1. Added 5 bridge table names to `GRAPH_TABLES` cache list
  2. Added bridge table SQL after DOB respondent block
  3. Added `nyc_unified` property graph definition after tradewaste graph
- Graph cache deleted and rebuilt from scratch
