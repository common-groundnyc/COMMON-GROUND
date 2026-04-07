# Roadmap: Common Ground NYC Data Platform

## Overview

Common Ground is an NYC civic intelligence platform — 294 tables, 60M+ rows, 14 schemas — with probabilistic entity resolution (Splink, 55M records), vector search (Lance, 2.96M embeddings), 8 DuckPGQ property graphs, and 14 MCP super tools. v1.0 built entity resolution, v2.0 rebuilt the graph layer. v3.0 transforms the platform from a powerful engine into a user-facing intelligence system — semantic queries, disambiguation, shareable investigations, real-time alerts, and proactive anomaly detection.

## Milestones

- ✅ **v1.0 Entity Resolution** — Phases 1-8 (shipped 2026-03-17)
- ✅ **v2.0 DuckPGQ Graph Rebuild** — Phases 1-10 (shipped 2026-03-26)
- 🚧 **v3.0 UX Innovation & Intelligence** — Phases 11-21 (in progress)

## Domain Expertise

None (DuckPGQ-specific — research done in-session)

## Phases

<details>
<summary>✅ v1.0 Entity Resolution (Phases 1-8) — SHIPPED 2026-03-17</summary>

Splink probabilistic entity resolution across 44 tables, 55.5M records, producing resolved_entities table with cluster IDs. See previous ROADMAP for full details. All 8 phases complete.

</details>

### ✅ v2.0 DuckPGQ Graph Rebuild

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

### 🚧 v3.0 UX Innovation & Intelligence

**Milestone Goal:** Transform Common Ground from a powerful data engine (9/10) into a user-facing intelligence platform (matching the engine's quality). Ship the surfaces — semantic queries, disambiguation, shareable investigations, alerts, anomaly detection, and visual exploration.

**Source:** Innovation audit by 5-agent team (April 2026) — see `.planning/audits/INNOVATION-ROADMAP.md`

- [ ] **Phase 11: Entity Master** — Stable UUIDs, canonical names, confidence scores, entity types
- [ ] **Phase 12: Address Standardization** — libpostal normalization + fuzzy Splink address comparison
- [ ] **Phase 13: Semantic Layer** — Metric definitions, join paths, `query(mode="nl")` for natural language queries
- [ ] **Phase 14: Elicitation** — Server-side disambiguation ("did you mean the cop or the landlord?")
- [ ] **Phase 15: Anomaly Detection** — Z-score analysis on daily deltas, `suggest(view="anomalies")`
- [ ] **Phase 16: Receipt Generator** — Shareable investigation permalinks with citations
- [ ] **Phase 17: Watchdog Alerts** — Entity subscription + change notification system
- [ ] **Phase 18: Graph RAG** — Hybrid vector + graph retrieval for multi-hop queries
- [ ] **Phase 19: Neighborhood Digests** — Auto-generated monthly ZIP-level intelligence briefings
- [ ] **Phase 20: Evidence Builder** — Court-ready PDF packets for housing court tenants
- [ ] **Phase 21: Visual Graph Explorer** — Interactive network visualization of entity connections

## Phase Details

### 🚧 v3.0 Phases

### Phase 11: Entity Master
**Goal**: Create `lake.foundation.entity_master` — a canonical entity table with stable UUIDs that persist across Dagster rematerializations. Each entity gets: entity_id (UUID), entity_type (PERSON/ORGANIZATION), canonical_name (most frequent variant), name_variants[], confidence (mean Splink match probability), source_count, record_count, first_seen, last_seen, addresses[], pagerank, community_id. This is the keystone — almost every Phase 12-21 feature depends on stable entity IDs.
**Depends on**: v1.0 resolved_entities, v2.0 graph tables
**Research**: Likely (UUID generation strategy in DuckDB, merge/split tracking across rematerializations, entity type classification heuristics)
**Research topics**: DuckDB uuid_generate_v4(), deterministic UUID from cluster contents for stability, LLC_FILTER_TERMS for type classification, Splink match probability extraction from clustering output
**Plans**: 3 plans

Plans:
- [ ] 11-01: Design entity_master schema and UUID stability strategy
- [ ] 11-02: Build Dagster asset that produces entity_master from resolved_entities + graph tables
- [ ] 11-03: Wire entity_master into MCP entity() and network() tools, verify stable IDs across re-run

### Phase 12: Address Standardization
**Goal**: Add libpostal address normalization as a preprocessing step in the name_index asset. Replace ExactMatch("address") in the Splink model with multi-level comparison (exact → component-split → Jaro-Winkler → else). Currently ~40% of true address matches are lost because "123 W 45TH ST" ≠ "123 WEST 45 STREET". This is the single highest-ROI improvement to entity resolution quality — pure model fix, invisible to users but they get dramatically better results.
**Depends on**: Phase 11 (entity_master benefits from improved matching)
**Research**: Likely (libpostal installation on macOS/Docker, DuckDB UDF integration, Splink CustomComparison syntax)
**Research topics**: libpostal Python bindings (postal package), address component splitting (house_number, street, unit), Splink 4.0 CustomComparison and LevenshteinAtThresholds, performance impact on 55M record batching
**Plans**: 3 plans

Plans:
- [ ] 12-01: Install libpostal, build address normalization UDF, benchmark on sample data
- [ ] 12-02: Add normalized address columns to name_index asset, retrain Splink model with fuzzy address comparison
- [ ] 12-03: Re-run resolved_entities, measure precision/recall improvement, rebuild entity_master

### Phase 13: Semantic Layer
**Goal**: Build a semantic layer that defines metrics, dimensions, and join paths for the 294-table lake. Add `query(mode="nl")` that translates natural language to validated SQL using vector-matched metric templates. This eliminates the #1 failure mode: LLMs writing broken SQL against tables they don't understand. Pure metadata work — frozen dataclasses defining "slumlord score = open Class C violations / buildings owned", embedded alongside catalog descriptions.
**Depends on**: Nothing (independent, but benefits from entity_master for entity-typed metrics)
**Research**: Likely (semantic layer design patterns, metric definition formats, few-shot SQL generation)
**Research topics**: Cube.dev semantic layer architecture, dbt metrics layer, embedding metric descriptions with existing embed_fn, SQL validation against DuckDB information_schema, few-shot examples from SCHEMA_DESCRIPTIONS
**Plans**: 3 plans

Plans:
- [ ] 13-01: Define semantic layer schema — Metric, Dimension, JoinPath dataclasses + 20 initial metrics across top domains
- [ ] 13-02: Embed metric descriptions, build NL→SQL translation pipeline with validation
- [ ] 13-03: Add query(mode="nl") to MCP server, test against 30 natural language queries, measure accuracy

### Phase 14: Elicitation
**Goal**: When entity() returns 5+ ambiguous matches, use FastMCP's elicitation API to ask the user "did you mean the cop, the landlord, or the judge?" instead of dumping all results. Also apply to semantic_search() when query is ambiguous between domains (construction noise vs party noise vs aircraft noise). Requires FastMCP 2.10+ elicitation support and client capability detection.
**Depends on**: Phase 11 (entity_master provides entity_type for better disambiguation options)
**Research**: Likely (FastMCP elicitation API, client capability detection, schema design for choices)
**Research topics**: FastMCP ctx.elicit() API, session.supports_elicitation check, JSON schema for choice presentation, graceful fallback for clients without elicitation support
**Plans**: 2 plans

Plans:
- [ ] 14-01: Research FastMCP elicitation API, implement in entity() tool for ambiguous name matches
- [ ] 14-02: Extend to semantic_search() and building() tools, add fallback for non-elicitation clients

### Phase 15: Anomaly Detection
**Goal**: Background Dagster asset that runs Z-score anomaly detection on time-series metrics across the lake. Detect: 311 complaint spikes by ZIP/week, violation surges by owner/month, ACRIS transaction velocity changes, restaurant inspection failures, crime hotspot shifts. Write findings to `lake.foundation.anomalies`. Surface via `suggest(view="anomalies")` in the MCP server. LLM-generated natural language summaries using existing Gemini API.
**Depends on**: Phase 13 (semantic layer defines which metrics to monitor)
**Research**: Likely (time-series anomaly detection methods, Z-score vs IQR vs seasonal decomposition, Dagster sensor patterns for derived assets)
**Research topics**: scipy.stats z-score on rolling windows, Dagster @asset with daily partition, Gemini Flash for anomaly summarization, alert severity ranking
**Plans**: 3 plans

Plans:
- [ ] 15-01: Design anomaly detection framework — select 10 key metrics, define detection thresholds, design anomalies table schema
- [ ] 15-02: Build Dagster asset that computes anomalies on daily delta, writes to lake.foundation.anomalies
- [ ] 15-03: Add suggest(view="anomalies") to MCP server, generate LLM summaries, test with historical data

### Phase 16: Receipt Generator
**Goal**: Turn any MCP tool output into a permanent, cited, shareable URL at `/receipts/[slug]`. The `/receipts/kessler` page already proves the concept. Server-side render tool outputs to static HTML with unique slugs, citation middleware metadata, data provenance chains, and timestamps. Every shared receipt becomes distribution — users share investigations, each one advertises Common Ground.
**Depends on**: Phase 11 (entity_master provides stable entity links in receipts)
**Research**: Likely (static site generation from tool outputs, slug generation, Cloudflare caching)
**Research topics**: Jinja2 or server-side HTML templating from tool result dicts, citation_middleware metadata format, persistent storage for receipts (MinIO or filesystem), Cloudflare cache rules for /receipts/* paths
**Plans**: 3 plans

Plans:
- [ ] 16-01: Design receipt template — HTML structure, citation format, slug strategy, storage backend
- [ ] 16-02: Build receipt generation endpoint in MCP server, wire to tool output pipeline
- [ ] 16-03: Deploy, add to website navigation, test 5 investigation types, verify Cloudflare caching

### Phase 17: Watchdog Alerts
**Goal**: Subscribe to any entity (landlord, building, politician, ZIP code). Get notified when: new violations filed, property sold, campaign donation made, new lawsuit, 311 spike in area. The daily Dagster pipeline already produces deltas — alerts are diffs between materialized views across runs. Store subscriptions in Postgres, deliver via email webhook or as MCP resource. This is the retention/engagement loop that no civic tech competitor has.
**Depends on**: Phase 11 (entity_master for stable entity subscriptions), Phase 15 (anomaly detection for ZIP-level alerts)
**Research**: Likely (materialized view diff strategy, subscription storage, email delivery)
**Research topics**: DuckDB EXCEPT for delta detection between runs, Postgres subscription table design, email delivery (Resend, SendGrid, or self-hosted), MCP resource subscriptions, webhook delivery patterns
**Plans**: 3 plans

Plans:
- [ ] 17-01: Design subscription model — entity types, event types, delivery channels, storage schema
- [ ] 17-02: Build Dagster sensor that computes deltas per subscribed entity after each pipeline run
- [ ] 17-03: Build delivery pipeline (email + webhook), add subscribe/unsubscribe MCP tools, test end-to-end

### Phase 18: Graph RAG
**Goal**: Combine the 8 DuckPGQ property graphs with the 2.96M entity embeddings for hybrid vector + graph retrieval. A `graph_query()` tool accepts natural language, routes to the appropriate graph, executes multi-hop traversals, and enriches results with vector-similar context. "Show me everyone connected to this landlord through corporate + political + property networks" in one query instead of 5 manual tool calls.
**Depends on**: Phase 11 (entity_master as graph node identity), Phase 13 (semantic layer for query understanding)
**Research**: Likely (Graph RAG architecture, DuckPGQ MATCH + vector hybrid, traversal result enrichment)
**Research topics**: HybridRAG patterns (Memgraph, Neo4j), extending lance_route_entity() to route across graphs, DuckPGQ MATCH performance on unified graph, result synthesis prompting
**Plans**: 3 plans

Plans:
- [ ] 18-01: Design Graph RAG pipeline — query→graph routing, entity extraction, traversal strategy
- [ ] 18-02: Build graph_query() tool with DuckPGQ traversal + Lance vector enrichment
- [ ] 18-03: Test against 20 investigation scenarios, benchmark latency, add to MCP routing instructions

### Phase 19: Neighborhood Digests
**Goal**: Auto-generated monthly intelligence briefings for any ZIP code. Content: new buildings permitted, violations filed, crimes reported, restaurants opened/closed, property sales, demographic shifts. Cross-referenced, percentile-ranked against city averages. Delivered as static web pages and optionally via email. Uses the existing neighborhood() tool data + percentile middleware + anomaly detection.
**Depends on**: Phase 15 (anomaly detection for highlighting significant changes), Phase 16 (receipt generator for rendering)
**Research**: Unlikely (combines existing tools and data — neighborhood(), percentile middleware, anomaly table)
**Plans**: 2 plans

Plans:
- [ ] 19-01: Design digest template — sections, data sources per section, percentile integration, rendering
- [ ] 19-02: Build Dagster asset that generates monthly digests per active ZIP, deploy to website

### Phase 20: Evidence Builder
**Goal**: Generate court-ready PDF evidence packets for housing court tenants. Contents: violation history with dates and classes, complaint timeline, owner identification with corporate tree, portfolio comparison (other buildings owned + their violation rates), settlement history, relevant OATH hearings. Uses building() + network(type="ownership") + legal() + entity() outputs. Export as structured PDF with proper legal citations.
**Depends on**: Phase 11 (entity_master for owner identification), Phase 16 (receipt generator for rendering framework)
**Research**: Likely (PDF generation in Python, legal citation formatting, housing court document requirements)
**Research topics**: weasyprint or reportlab for PDF generation, NYC Housing Court evidence requirements, legal citation format for administrative records, template design for pro-se litigants
**Plans**: 2 plans

Plans:
- [ ] 20-01: Design evidence packet template — sections, data sources, legal citation format, PDF layout
- [ ] 20-02: Build evidence generation endpoint, test with 5 real buildings, validate with legal aid org feedback

### Phase 21: Visual Graph Explorer
**Goal**: Interactive web-based network visualization where users click any entity and see connections radiate outward — buildings owned, donations made, lawsuits filed, officers shared. Uses the DuckPGQ graph data + PageRank for node sizing + community detection for clustering. The `lashley-graph-3d.html` prototype already exists as a reference. Ship as an embeddable component on the website and as a link from receipt pages.
**Depends on**: Phase 11 (entity_master for stable node IDs), Phase 18 (Graph RAG for data retrieval)
**Research**: Likely (graph visualization libraries, force-directed layout, WebGL performance at scale)
**Research topics**: d3-force vs sigma.js vs deck.gl for graph rendering, 3D vs 2D tradeoffs, WebGL node limits, progressive disclosure for large graphs (show top PageRank first, expand on click)
**Plans**: 3 plans

Plans:
- [ ] 21-01: Evaluate graph visualization libraries, build prototype with sample ownership network data
- [ ] 21-02: Build graph data API endpoint, connect to entity_master + DuckPGQ, implement interactive exploration
- [ ] 21-03: Integrate into website, add to receipt pages, optimize for mobile, test with 5 entity types

---

## v1.0 & v2.0 Phase Details (Completed)

<details>
<summary>✅ v1.0 Entity Resolution (Phases 1-8) — SHIPPED 2026-03-17</summary>

Splink probabilistic entity resolution across 44 tables, 55.5M records, producing resolved_entities table with cluster IDs. See previous ROADMAP for full details. All 8 phases complete.

</details>

<details>
<summary>✅ v2.0 DuckPGQ Graph Rebuild (Phases 1-10) — SHIPPED 2026-03-26</summary>

### Phase 1: Data Audit
**Goal**: Catalog every entity type (person, company, BBL, address) across all 539 tables in the DuckLake. Produce a complete entity map: which tables have which entity types, which columns contain them, row counts, and cross-reference potential. This drives all subsequent phases.
**Depends on**: Nothing (first phase)
**Research**: Likely (need to query information_schema across 12+ schemas, classify columns)
**Research topics**: Column naming patterns for entities across Socrata datasets, resolved_entities coverage gaps, spatial join potential
**Plans**: 2 plans

Plans:
- [x] 01-01: Schema introspection — 246 tables classified, 588 entity columns, 13 types ✓
- [x] 01-02: Entity coverage report — 5 graph gaps mapped, 2 new graphs proposed, memory budget confirmed ✓

### Phase 2: Bug Fixes
**Goal**: Fix the 3 broken tools identified in MCP Inspector testing: property_history date sort crash, shell_detector WCC crash from duplicate vertex PKs, graph_has_violation orphaned edges
**Depends on**: Nothing (independent)
**Research**: Unlikely (root causes already identified)
**Plans**: 2 plans

Plans:
- [x] 02-01: Infrastructure fixes — S3 creds (reconnect path), duckpgq (FORCE INSTALL), MinIO HTTP ✓
- [x] 02-02: Data fixes — property_history sort, graph_corps dedup, orphaned violations + full MCP re-test ✓

### Phase 3: Ownership Graph Rebuild
**Goal**: Rebuild the housing ownership graph from registrationid-based (489 shared edges) to name-based (31K+ shared edges). Expand graph_buildings from HPD-only (348K) to include PLUTO (860K lots). Fix null owner names (34K records).
**Depends on**: Phase 1 (entity audit identifies additional ownership sources)
**Research**: Unlikely (SQL rebuilds of existing tables)
**Plans**: 2 plans

Plans:
- [x] 03-01: Rebuild graph_owners with name-based keys, expand graph_buildings from PLUTO ✓
- [x] 03-02: Verify graphs, fix graph_corps dedup + property_history date sort, full MCP re-test (10/10 pass) ✓

### Phase 4: Corporate Web Rebuild
**Goal**: Deduplicate graph_corps (312K dupes → unique dos_ids), expand beyond HPD/PLUTO matching to include ACRIS-referenced corps, campaign employers, OATH respondents, DOB permit owners. Target: 5% → 30%+ of NYC corps in graph.
**Depends on**: Phase 1 (audit identifies corp references across tables), Phase 2 (dedup fix)
**Research**: Unlikely (expanding existing SQL patterns)
**Plans**: 2 plans

Plans:
- [x] 04-01: Expand graph_corps from 105K to 268K via 4-source incremental staging (ACRIS + OATH + campaign) ✓
- [x] 04-02: Full MCP tool re-test (12/12 pass), property_history date sort fix, graph stats collected ✓

### Phase 5: Transaction Network Expansion
**Goal**: Maximize ACRIS party coverage in the transaction graph. Currently 1.96M tx_entities from 46.2M ACRIS parties (4.2%). Expand entity matching, add personal property transactions (4.5M records).
**Depends on**: Phase 1 (audit identifies ACRIS coverage gaps)
**Research**: Unlikely (expanding existing SQL)
**Plans**: 1 plan

Plans:
- [x] 05-01: Expand tx graph from DEED-only to DEED+MTGE+ASST+SAT+AGMT — 3.97M entities (+103%), 40.6M edges (+356%), 8.2M shared (+107%) ✓

### Phase 6: Influence Network Expansion
**Goal**: Expand political influence graph to include lobbying registrations, city contracts (doing business disclosures), COIB donor network, and off-year contributions. Currently 311K entities from 1.67M contributions.
**Depends on**: Phase 1 (audit identifies political data sources)
**Research**: Unlikely (expanding existing SQL patterns)
**Plans**: 1 plan

Plans:
- [x] 06-01: Expand graph_pol_entities with expenditures + offyear + doing business + payroll — 630K entities (+103%), 1.5M expenditure edges, 169 payroll agencies ✓

### Phase 7: Cross-Domain Unified Graph
**Goal**: Create a single property graph that connects all domain graphs through shared entities. A landlord (housing graph) who is also a corp officer (corporate web) who donates to politicians (influence graph) who has OATH violations (enforcement) — all traversable in one query. Uses resolved_entities cluster_ids as the unifying key.
**Depends on**: Phases 3-6 (all domain graphs rebuilt), v1.0 resolved_entities
**Research**: Likely (DuckPGQ multi-vertex-table graph design, MATCH across labels)
**Research topics**: Maximum vertex/edge tables per graph, MATCH performance with 10+ labels, CSR memory at combined scale
**Plans**: 2 plans

Plans:
- [x] 07-01: Build cross-domain bridge tables + unified property graph (nyc_unified) — 578K links, 597K multi-domain entities ✓
- [x] 07-02: Full MCP tool regression (12/13 pass) + cross-domain verification + comprehensive graph stats ✓

### Phase 8: PageRank Integration
**Goal**: Add PageRank-powered tools — rank most connected/influential entities in each domain graph AND the unified graph. "Who are the most connected landlords in Brooklyn?" "Most influential political donors?" Already tested and working in DuckPGQ, just not wired up.
**Depends on**: Phases 3-7 (graphs rebuilt with real data)
**Research**: Unlikely (PageRank already tested on tx_network)
**Plans**: 1 plan

Plans:
- [x] 08-01: Add pagerank tools — landlord_pagerank + entity_influence (4 domains), fix nyc_housing graph definition, 14/15 MCP tools pass ✓

### Phase 9: Path-Finding Tools
**Goal**: Add GRAPH_TABLE MATCH-powered tools — shortest path between any two entities, reachability checks, multi-hop corporate chain tracing. "How is person A connected to building B?" Uses ANY SHORTEST path-finding with quantifiers.
**Depends on**: Phase 7 (unified graph), Phase 8 (PageRank for context)
**Research**: Likely (DuckPGQ MATCH gotchas — CTE segfaults, left-directed paths, bounded path bugs)
**Research topics**: MATCH syntax patterns that work reliably, avoiding known segfault triggers, performance on large graphs
**Plans**: 2 plans

Plans:
- [x] 09-01: Add shortest_path and entity_reachable tools with SQL-based cross-domain path finding ✓
- ~~09-02: Skipped — SQL approach in 09-01 covers multi-hop tracing without MATCH~~

### Phase 10: Validation & Hardening
**Goal**: Every graph tool has a SQL fallback for DuckPGQ crashes. Health check tool using summarize_property_graph(). Full MCP Inspector re-test of all tools. Orphan edge validation on all graphs. Memory profiling under 28GB limit.
**Depends on**: All previous phases
**Research**: Unlikely (testing and defensive coding)
**Plans**: 2 plans

Plans:
- [x] 10-01: Permanent property_history fix + graph_health tool + entity_xray float fix + full 18/18 MCP test suite ✓
- ~~10-02: Merged into 10-01 — full test suite run as part of validation~~


</details>

## Progress

**Execution Order:** 11 → 12 → 13 → 14 → 15 → 16 → 17 → 18 → 19 → 20 → 21

| Phase | Milestone | Plans | Status | Completed |
|-------|-----------|-------|--------|-----------|
| 1-8. Entity Resolution | v1.0 | 8/8 | Complete | 2026-03-17 |
| 1-10. Graph Rebuild | v2.0 | 12/12 | Complete | 2026-03-26 |
| 11. Entity Master | v3.0 | 0/3 | Not started | - |
| 12. Address Standardization | v3.0 | 0/3 | Not started | - |
| 13. Semantic Layer | v3.0 | 0/3 | Not started | - |
| 14. Elicitation | v3.0 | 0/2 | Not started | - |
| 15. Anomaly Detection | v3.0 | 0/3 | Not started | - |
| 16. Receipt Generator | v3.0 | 0/3 | Not started | - |
| 17. Watchdog Alerts | v3.0 | 0/3 | Not started | - |
| 18. Graph RAG | v3.0 | 0/3 | Not started | - |
| 19. Neighborhood Digests | v3.0 | 0/2 | Not started | - |
| 20. Evidence Builder | v3.0 | 0/2 | Not started | - |
| 21. Visual Graph Explorer | v3.0 | 0/3 | Not started | - |
