# Roadmap: DuckPGQ Graph Infrastructure Rebuild

## Overview

Full rebuild of the DuckPGQ property graph layer powering 60+ MCP tools in the Common Ground NYC data lake. Starting with a comprehensive audit of all 539 tables to identify every entity that should be a graph node, then fixing broken tools, expanding data coverage from 5% to maximum reach, and unlocking unused DuckPGQ capabilities (PageRank, shortest path, MATCH pattern queries). The end state: any person, company, or building can be traced across every dataset in the lake.

## Milestones

- ✅ **v1.0 Entity Resolution** — Phases 1-8 (shipped 2026-03-17)
- 🚧 **v2.0 DuckPGQ Graph Rebuild** — Phases 1-10 (in progress)

## Domain Expertise

None (DuckPGQ-specific — research done in-session)

## Phases

<details>
<summary>✅ v1.0 Entity Resolution (Phases 1-8) — SHIPPED 2026-03-17</summary>

Splink probabilistic entity resolution across 44 tables, 55.5M records, producing resolved_entities table with cluster IDs. See previous ROADMAP for full details. All 8 phases complete.

</details>

### 🚧 v2.0 DuckPGQ Graph Rebuild

**Milestone Goal:** Rebuild every property graph to maximum data coverage, fix all broken tools, unlock unused DuckPGQ algorithms, and enable cross-domain entity tracing.

- [ ] **Phase 1: Data Audit** — Catalog every entity type across all 539 DuckLake tables
- [ ] **Phase 2: Bug Fixes** — Fix property_history crash, shell_detector crash, graph_has_violation orphans
- [ ] **Phase 3: Ownership Graph Rebuild** — Name-based ownership + PLUTO building expansion
- [ ] **Phase 4: Corporate Web Rebuild** — Deduplicate graph_corps + expand to all NYC-relevant corps
- [ ] **Phase 5: Transaction Network Expansion** — Maximize ACRIS party coverage
- [ ] **Phase 6: Influence Network Expansion** — Full campaign + lobbying + contracts + doing business
- [ ] **Phase 7: Cross-Domain Unified Graph** — Connect all domain graphs through shared entities
- [ ] **Phase 8: PageRank Integration** — Rank entities by network centrality across all graphs
- [ ] **Phase 9: Path-Finding Tools** — Shortest path, reachability, GRAPH_TABLE MATCH queries
- [ ] **Phase 10: Validation & Hardening** — SQL fallbacks, health checks, full MCP Inspector test suite

## Phase Details

### Phase 1: Data Audit
**Goal**: Catalog every entity type (person, company, BBL, address) across all 539 tables in the DuckLake. Produce a complete entity map: which tables have which entity types, which columns contain them, row counts, and cross-reference potential. This drives all subsequent phases.
**Depends on**: Nothing (first phase)
**Research**: Likely (need to query information_schema across 12+ schemas, classify columns)
**Research topics**: Column naming patterns for entities across Socrata datasets, resolved_entities coverage gaps, spatial join potential
**Plans**: 2 plans

Plans:
- [x] 01-01: Schema introspection — 246 tables classified, 588 entity columns, 13 types ✓
- [ ] 01-02: Entity coverage report — produce the definitive map of what entities exist where, cross-reference with current graph coverage

### Phase 2: Bug Fixes
**Goal**: Fix the 3 broken tools identified in MCP Inspector testing: property_history date sort crash, shell_detector WCC crash from duplicate vertex PKs, graph_has_violation orphaned edges
**Depends on**: Nothing (independent)
**Research**: Unlikely (root causes already identified)
**Plans**: 1 plan

Plans:
- [ ] 02-01: Fix property_history sort, deduplicate graph_corps, filter orphaned violations

### Phase 3: Ownership Graph Rebuild
**Goal**: Rebuild the housing ownership graph from registrationid-based (489 shared edges) to name-based (31K+ shared edges). Expand graph_buildings from HPD-only (348K) to include PLUTO (860K lots). Fix null owner names (34K records).
**Depends on**: Phase 1 (entity audit identifies additional ownership sources)
**Research**: Unlikely (SQL rebuilds of existing tables)
**Plans**: 2 plans

Plans:
- [ ] 03-01: Rebuild graph_owners with name-based keys, expand graph_buildings from PLUTO
- [ ] 03-02: Rebuild graph_shared_owner, graph_owns, verify nyc_housing + nyc_building_network graphs

### Phase 4: Corporate Web Rebuild
**Goal**: Deduplicate graph_corps (312K dupes → unique dos_ids), expand beyond HPD/PLUTO matching to include ACRIS-referenced corps, campaign employers, OATH respondents, DOB permit owners. Target: 5% → 30%+ of NYC corps in graph.
**Depends on**: Phase 1 (audit identifies corp references across tables), Phase 2 (dedup fix)
**Research**: Unlikely (expanding existing SQL patterns)
**Plans**: 2 plans

Plans:
- [ ] 04-01: Deduplicate graph_corps, expand source matching to ACRIS + campaign + OATH + DOB
- [ ] 04-02: Rebuild graph_corp_people, officer_edges, shared_officer with expanded corps

### Phase 5: Transaction Network Expansion
**Goal**: Maximize ACRIS party coverage in the transaction graph. Currently 1.96M tx_entities from 46.2M ACRIS parties (4.2%). Expand entity matching, add personal property transactions (4.5M records).
**Depends on**: Phase 1 (audit identifies ACRIS coverage gaps)
**Research**: Unlikely (expanding existing SQL)
**Plans**: 1 plan

Plans:
- [ ] 05-01: Expand graph_tx_entities, add personal property (pp_*) tables, rebuild shared edges

### Phase 6: Influence Network Expansion
**Goal**: Expand political influence graph to include lobbying registrations, city contracts (doing business disclosures), COIB donor network, and off-year contributions. Currently 311K entities from 1.67M contributions.
**Depends on**: Phase 1 (audit identifies political data sources)
**Research**: Unlikely (expanding existing SQL patterns)
**Plans**: 1 plan

Plans:
- [ ] 06-01: Expand graph_pol_entities with lobbying + contracts + COIB + off-year, rebuild edges

### Phase 7: Cross-Domain Unified Graph
**Goal**: Create a single property graph that connects all domain graphs through shared entities. A landlord (housing graph) who is also a corp officer (corporate web) who donates to politicians (influence graph) who has OATH violations (enforcement) — all traversable in one query. Uses resolved_entities cluster_ids as the unifying key.
**Depends on**: Phases 3-6 (all domain graphs rebuilt), v1.0 resolved_entities
**Research**: Likely (DuckPGQ multi-vertex-table graph design, MATCH across labels)
**Research topics**: Maximum vertex/edge tables per graph, MATCH performance with 10+ labels, CSR memory at combined scale
**Plans**: 2 plans

Plans:
- [ ] 07-01: Design unified graph schema — vertex tables, edge tables, label strategy
- [ ] 07-02: Build unified graph, verify cross-domain MATCH queries work

### Phase 8: PageRank Integration
**Goal**: Add PageRank-powered tools — rank most connected/influential entities in each domain graph AND the unified graph. "Who are the most connected landlords in Brooklyn?" "Most influential political donors?" Already tested and working in DuckPGQ, just not wired up.
**Depends on**: Phases 3-7 (graphs rebuilt with real data)
**Research**: Unlikely (PageRank already tested on tx_network)
**Plans**: 1 plan

Plans:
- [ ] 08-01: Add pagerank tools — landlord_pagerank, politician_pagerank, contractor_pagerank, entity_influence

### Phase 9: Path-Finding Tools
**Goal**: Add GRAPH_TABLE MATCH-powered tools — shortest path between any two entities, reachability checks, multi-hop corporate chain tracing. "How is person A connected to building B?" Uses ANY SHORTEST path-finding with quantifiers.
**Depends on**: Phase 7 (unified graph), Phase 8 (PageRank for context)
**Research**: Likely (DuckPGQ MATCH gotchas — CTE segfaults, left-directed paths, bounded path bugs)
**Research topics**: MATCH syntax patterns that work reliably, avoiding known segfault triggers, performance on large graphs
**Plans**: 2 plans

Plans:
- [ ] 09-01: Add shortest_path and reachability tools with SQL fallbacks
- [ ] 09-02: Add multi-hop MATCH tools — corporate chain tracer, influence tracer

### Phase 10: Validation & Hardening
**Goal**: Every graph tool has a SQL fallback for DuckPGQ crashes. Health check tool using summarize_property_graph(). Full MCP Inspector re-test of all tools. Orphan edge validation on all graphs. Memory profiling under 28GB limit.
**Depends on**: All previous phases
**Research**: Unlikely (testing and defensive coding)
**Plans**: 2 plans

Plans:
- [ ] 10-01: Add SQL fallbacks to all graph tools, add graph_health diagnostic tool
- [ ] 10-02: Full MCP Inspector test suite — verify every tool passes, fix any remaining failures

## Progress

**Execution Order:** 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10

| Phase | Plans | Status | Completed |
|-------|-------|--------|-----------|
| 1. Data Audit | 1/2 | In progress | — |
| 2. Bug Fixes | 0/1 | Not started | — |
| 3. Ownership Rebuild | 0/2 | Not started | — |
| 4. Corporate Web Rebuild | 0/2 | Not started | — |
| 5. Transaction Expansion | 0/1 | Not started | — |
| 6. Influence Expansion | 0/1 | Not started | — |
| 7. Cross-Domain Graph | 0/2 | Not started | — |
| 8. PageRank Integration | 0/1 | Not started | — |
| 9. Path-Finding Tools | 0/2 | Not started | — |
| 10. Validation & Hardening | 0/2 | Not started | — |
