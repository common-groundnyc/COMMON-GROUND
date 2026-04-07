---
plan: 08-01
phase: 8 — PageRank Integration
status: complete
started: 2026-03-26
completed: 2026-03-26
duration: ~30 min
---

# 08-01 Summary: PageRank Integration

## What was done

1. **Fixed nyc_housing graph definition** — removed stale `registrationid` column reference from Phase 3 rebuild. This was silently breaking ALL property graph creation on startup (`graph_ready = False`), causing every graph tool to report "Property graph unavailable."

2. **Added 2 new MCP tools:**
   - `landlord_pagerank(borough, top_n)` — ranks buildings by PageRank in the shared-owner network. Reveals the most networked properties (connected through shared landlords).
   - `entity_influence(domain, top_n)` — ranks entities by PageRank across 4 domain graphs: building, corporate, transaction, political.

3. **DuckPGQ PageRank syntax discovery:**
   - Correct syntax: `pagerank(graph_name, vertex_label, edge_label)` (NOT table names)
   - Returns columns: `(key_column, pagerank)`
   - Labels used: Building/SharedOwner, Corp/SharedOfficer, TxEntity/SharedTransaction, PoliticalEntity/DonatesTo

4. **Full MCP tool regression: 14/15 pass**

| Tool | Result |
|------|--------|
| landlord_pagerank (NEW) | PASS |
| entity_influence - building (NEW) | PASS |
| entity_influence - corporate (NEW) | PASS |
| entity_influence - transaction (NEW) | PASS |
| entity_influence - political (NEW) | PASS |
| worst_landlords | PASS |
| building_profile | PASS |
| landlord_network | PASS |
| ownership_clusters | PASS |
| corporate_web | PASS |
| shell_detector | PASS |
| property_history | FAIL (known date sort bug, deferred) |
| flipper_detector | PASS |
| pay_to_play | PASS |
| entity_xray | PASS |

## Key findings

- Top PageRank buildings are in Staten Island (borough 5) — high shared-owner density in SI suburban developments
- Top political PageRank: Zohran Mamdani (0.059), Scott Stringer (0.043), Maya Wiley (0.031) — reflects donation network centrality
- Corporate PageRank identifies densely connected corps by shared-officer relationships
- Transaction PageRank reveals entities involved in the most co-transactions

## Decisions

- **PageRank uses edge labels, not table names** — DuckPGQ `pagerank()` function takes `(graph, vertex_label, edge_label)`, not `(graph, table_name, key_column)` as originally planned
- **property_history date sort bug persists** — known deferred issue from 03-02, not blocking Phase 8
