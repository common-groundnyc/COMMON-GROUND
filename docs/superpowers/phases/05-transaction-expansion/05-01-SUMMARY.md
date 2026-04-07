---
phase: 05-transaction-expansion
plan: 01
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~45 min
---

# 05-01 Summary: Transaction Network Expansion

## What Changed

Expanded the ACRIS transaction graph from DEED-only to include mortgages (MTGE), assignments (ASST), satisfactions (SAT), and agreements (AGMT). The build now uses incremental temp table staging (3 stages) instead of a single monolithic CTE.

## Results

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| tx_entities | 1,960,000 | 3,978,074 | +103% |
| tx_edges | 8,900,000 | 40,577,562 | +356% |
| tx_shared | 3,960,000 | 8,179,488 | +107% |

### Staging Breakdown

- Stage 1 (deeds): 14,889,343 party rows
- Stage 2 (mortgages): 13,536,544 party rows
- Stage 3 (assignments/satisfactions): 19,814,226 party rows
- Total staged: 48,240,113 party rows

## MCP Tool Test Results (10/10 pass)

| # | Tool | Result | Notes |
|---|------|--------|-------|
| 1 | property_history("1000670001") | PASS | 103 documents including mortgages, assignments, satisfactions |
| 2 | flipper_detector() | PASS | 50 flips, $8.5B total profit |
| 3 | transaction_network("Perlbinder") | PASS | 152 tx, 225 properties, co-transactors found |
| 4 | entity_xray("Barton Perlbinder") | PASS | Cross-domain: corps, donations, OATH, permits |
| 5 | worst_landlords() | PASS | No regression |
| 6 | building_profile("1000670001") | PASS | No regression |
| 7 | corporate_web("Perlbinder") | PASS | 9 direct corps, 23 connected |
| 8 | shell_detector() | PASS | No regression |
| 9 | pay_to_play("Catsimatidis") | PASS | Donations to 20+ candidates |
| 10 | ownership_clusters() | PASS | No regression |

## Key Decisions

- **3-stage incremental staging**: Deeds, mortgages, assignments/satisfactions as separate temp tables to avoid OOM on the 48M-row 3-way JOIN
- **Mega-entity cap lowered to 200 tx** (from 500): Banks and title companies with 500+ transactions caused OOM on the self-join; added per-document party cap of 20 to filter out bulk filings
- **Role labels changed**: SOLD/BOUGHT to GRANTOR/GRANTEE (more accurate for mortgages/assignments)
- **Personal property deferred**: 11M rows with different schema, high OOM risk, low priority vs. real property expansion
- **doc_type column added**: Temp tables now include doc_type for downstream analysis

## Issues Encountered

- **OOM on first attempt**: tx_shared self-join OOM'd with 500 tx cap and 40M+ edges. Fixed by lowering cap to 200 and adding per-document party count filter (BETWEEN 2 AND 20).

## Files Modified

- `/opt/common-ground/duckdb-server/mcp_server.py` (remote, lines ~1443-1525)
