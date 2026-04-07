# DuckPGQ Graph Infrastructure Rebuild

## What This Is

A full rebuild of the DuckPGQ property graph layer inside the Common Ground MCP server (`mcp_server.py` on Hetzner). Fixes broken tools, expands data coverage to every table in the DuckLake (960M+ rows across 539 tables), and unlocks cross-domain entity tracing — so any person, company, or building can be traced across property records, corporate filings, political donations, enforcement actions, and transactions.

## Core Value

Cross-domain connections: trace a single entity across every dataset in the lake — from property ownership through corporate filings through political donations through enforcement actions. The full web of NYC power and accountability.

## Requirements

### Validated

- ✓ DuckPGQ extension loaded and property graphs defined — existing
- ✓ 8 property graphs created (housing, building_network, transaction_network, corporate_web, influence_network, contractor_network, officer_network, tradewaste_network) — existing
- ✓ Graph-powered MCP tools (worst_landlords, landlord_network, ownership_clusters, ownership_cliques, entity_xray, corporate_web, shell_detector, pay_to_play, etc.) — existing
- ✓ Graph cache system (Parquet export/import, 24h TTL) — existing
- ✓ 40+ graph tables built at startup from lake data — existing

### Active

- [ ] Fix `property_history` date sort crash (datetime.date vs str comparison)
- [ ] Fix `shell_detector` WCC crash (312K duplicate dos_ids in graph_corps vertex table)
- [ ] Fix ownership graph: rebuild graph_shared_owner using owner NAME not registrationid (489 → 31K+ edges)
- [ ] Fix `graph_has_violation` orphaned edges (13,701 BBLs not in graph_buildings)
- [ ] Audit ALL DuckLake tables and identify every entity (person, company, address, BBL) that should be a graph node
- [ ] Expand graph_corps beyond HPD/PLUTO matches — include ACRIS-referenced corps, campaign employers, OATH respondents, DOB permit owners
- [ ] Expand graph_buildings from PLUTO (860K lots vs HPD's 348K)
- [ ] Add PageRank tools — rank most connected landlords, politicians, contractors by network centrality
- [ ] Add GRAPH_TABLE MATCH queries — multi-hop corporate chain tracing, shortest path between any two entities
- [ ] Add reachability checks — "Is entity A connected to entity B?"
- [ ] Add `summarize_property_graph()` health check output as diagnostic tool
- [ ] Ensure all graph tools have SQL fallbacks for DuckPGQ crash resilience
- [ ] Validate every graph tool passes (re-run full MCP Inspector test suite)

### Out of Scope

- New data ingestion — only use what's already in the DuckLake
- UI/frontend/dashboards — MCP tools only
- External graph algorithms (Python networkx, Neo4j export) — stay within DuckPGQ
- Community detection (Louvain/Leiden) — not available in DuckPGQ yet
- Betweenness centrality — not available in DuckPGQ yet
- Temporal graph queries — not supported in DuckPGQ
- Weighted path queries via PGQ MATCH syntax — only available as scalar UDF (cheapest_path_length)

## Context

### Current State (audited 2026-03-25)

**Graph table sizes:**
| Table | Rows | Notes |
|-------|------|-------|
| graph_corps | 417K (104K unique) | 312K duplicate dos_ids — crashes WCC |
| graph_corp_people | 82K | Clean |
| graph_corp_officer_edges | 145K | Clean |
| graph_corp_shared_officer | 185K | Clean but built from duplicated corps |
| graph_owners | 194K | 34K have null names |
| graph_buildings | 348K | HPD only — PLUTO has 860K |
| graph_owns | 195K | Clean |
| graph_shared_owner | 489 | **Broken** — should be ~31K (keyed on registrationid not name) |
| graph_violations | 10.8M | Clean |
| graph_has_violation | 10.8M | 13,701 orphaned BBLs |
| graph_tx_entities | 1.96M | Clean |
| graph_tx_edges | 8.9M | Clean |
| graph_tx_shared | 3.96M | Clean |
| graph_pol_entities | 311K | Clean |
| graph_pol_donations | 700K | Clean |
| graph_contractors | 58K | Clean |
| graph_permit_edges | 1.46M | Clean |

**DuckLake source data:**
| Source | Total Records | In Graph | Coverage |
|--------|--------------|----------|----------|
| NYS Corporations | 8.3M (NYC) | 417K | 5% |
| Entity Addresses | 34.7M | 145K edges | 0.4% |
| HPD Contacts | 804K | 194K owners | 24% |
| ACRIS Parties | 46.2M | 1.96M tx entities | 4.2% |
| Campaign Contributions | 1.67M | 311K pol entities | 19% |

**DuckPGQ algorithms available:**
- PageRank — works, NOT used
- WCC (Weakly Connected Components) — works, used 2 places
- LCC (Local Clustering Coefficient) — works, used 1 place
- BFS Shortest Path (ANY SHORTEST in MATCH) — works, NOT used
- Cheapest Path (Bellman-Ford scalar UDF) — works, NOT used
- Reachability (scalar UDF) — works, NOT used

**DuckPGQ gotchas:**
- CSR materializes full graph in RAM per query — 28GB container limit constrains max graph size
- MATCH inside CTEs/UNION segfaults (open bugs)
- Only ANY SHORTEST works — no ALL SHORTEST, no Top-K
- Left-directed paths don't work — only right-directed or undirected
- Duplicate vertex PKs crash WCC — root cause of shell_detector bug
- Research project — pin versions, test everything, SQL fallbacks required

### Server

- Hetzner: 178.156.228.119
- Container: `common-ground-duckdb-server-1` (28GB memory limit)
- File: `/opt/common-ground/duckdb-server/mcp_server.py` (9,686 lines)
- DuckDB 1.5.0 + FastMCP 3.1+
- Graph cache: `/data/common-ground/graph-cache/` (Parquet, 24h TTL)

## Constraints

- **Memory**: 28GB container limit — CSR graph materialization must fit. At ~16 bytes/edge, max ~1.7B edges before OOM. Current largest graph (tx_edges) is 8.9M edges = ~140MB, well within limits. But expanding to full ACRIS (46M parties) needs careful subgraph design.
- **Stability**: DuckPGQ is a research project with known crashes. Every graph query must have a SQL fallback.
- **Single file**: All changes are in one 9,686-line `mcp_server.py`. No refactoring into modules — that's out of scope.
- **Deployment**: Changes require `docker compose restart duckdb-server` on Hetzner. Graph rebuild happens at container startup (~2-5 min).

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Rebuild ownership graph on owner NAME not registrationid | registrationid is per-building, so same landlord with 222 buildings = 222 separate "owners". Name-based gives 31K edges vs 489. | — Pending |
| Expand graph_corps to all NYC-relevant corps | Currently only 5% coverage. Missing ACRIS-referenced corps, campaign employers, OATH respondents. | — Pending |
| Add PageRank as first new algorithm | Already works in DuckPGQ, tested successfully on tx_network. High impact, low risk. | — Pending |
| Use GRAPH_TABLE MATCH for cross-domain tracing | Multi-hop pattern queries are the untapped power feature. Enables "person → corp → building → violation" chains. | — Pending |
| SQL fallbacks for all graph queries | DuckPGQ has known segfault bugs in CTEs/UNION. Every graph tool needs a non-graph SQL path. | — Pending |
| Stay within DuckPGQ only | No external tools (networkx, Neo4j). Keeps stack simple, single container. Missing algorithms (Louvain, betweenness) aren't worth the complexity. | — Pending |

---
*Last updated: 2026-03-25 after initialization*
