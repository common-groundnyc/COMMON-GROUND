# Common Ground Innovation Roadmap
## Fresh-Eyes Acquisition Audit — April 2026

> Synthesized from 5 parallel specialist audits: Platform Architecture, AI/MCP Integration, Product Strategy, Entity Resolution, and Infrastructure.

---

## Executive Summary

Common Ground's core engine — a cross-referenced NYC data lake with probabilistic entity resolution, property graphs, and MCP-native distribution — is **genuinely differentiated**. No competitor combines 294 tables / 60M+ rows with Splink ER across 55M records, 2.96M vector-embedded entities, 8 DuckPGQ property graphs, and 14 AI-native super tools. The infrastructure is production-hardened (0% errors at 50 concurrent).

**The gap is clear**: The data engine is a 9/10. The user-facing product is a 4/10. The areas CG has NOT yet traversed — alerts, shareable artifacts, graph analytics, semantic queries, temporal intelligence, and agent discovery — represent the next wave of value creation.

---

## Top 10 Innovations (Ranked by Combined Impact)

### 1. WATCHDOG: Real-Time Entity Alerts (Product + AI)
**Impact: Transformative | Effort: 2 weeks**

Subscribe to any entity (landlord, building, politician, ZIP). Get notified when: new violations filed, property sold, campaign donation made, lawsuit filed, 311 spike detected. The daily Dagster pipeline already produces the deltas — alerts are just diffs between materialized views.

**Why nobody else has this**: LittleSis is static. NYC OpenData has no notification layer. PropertyShark charges for single-property alerts. Nobody alerts on *cross-referenced* changes ("your landlord just donated $10K to the council member voting on your rezoning").

**Synergy**: Feeds into anomaly detection (AI), requires offsite state (Infra), uses entity_master (Data Science).

---

### 2. Semantic Layer + Text-to-SQL (AI)
**Impact: Transformative | Effort: 2-3 weeks**

LLMs currently write raw SQL against 294 tables blind. Build a semantic layer: define metrics ("slumlord score = open Class C violations / buildings owned"), dimensions (borough, ZIP, year), and join paths. Add `query(mode="nl")` that translates natural language to validated SQL via vector-matched metric templates.

**Why it matters**: Eliminates the #1 failure mode. Every major platform (Snowflake Cortex, Databricks, Dremio) now ships a semantic layer. CG is the only one that doesn't.

**Synergy**: Uses existing catalog embeddings, powers the product layer (Receipts, Digests).

---

### 3. Knowledge Graph with Typed Edges (Data Science + AI)
**Impact: Transformative | Effort: 3-4 weeks**

Replace the 42 ad-hoc graph tables with a unified property graph. Two node types (Entity, Property), 8+ typed edges (OWNS, DONATED_TO, SHARES_OFFICER_WITH, LITIGATED_AGAINST, etc.) with timestamps and weights. DuckPGQ's `CREATE PROPERTY GRAPH` syntax supports this natively.

**Why it matters**: Enables multi-hop queries ("find all entities within 3 hops of Steven Croman who donated to the same committees") in a single SQL statement instead of 5 manual tool calls. This is the transition from "we have graph data" to "we have a knowledge graph."

**Synergy**: Powers Graph RAG (AI), enables graph analytics (Onager), feeds Receipt Generator (Product).

---

### 4. Entity Master with Stable IDs + Confidence Scores (Data Science)
**Impact: High | Effort: 2 weeks**

Create `lake.foundation.entity_master`: UUID entity_id, entity_type (PERSON/ORG), canonical_name, name_variants[], confidence score, source_count, first_seen, last_seen, addresses[], pagerank, community_id. Currently cluster_ids change every run and match probabilities are discarded.

**Why it matters**: Every downstream feature (alerts, receipts, graph analytics, API) needs a stable entity identifier. This is the foundation for everything else.

**Synergy**: Required by Watchdog, Knowledge Graph, Graph Analytics, Evidence Builder.

---

### 5. Fuzzy Address Matching + libpostal (Data Science)
**Impact: High | Effort: 1 week**

The Splink model uses ExactMatch on raw addresses — "123 MAIN ST" ≠ "123 MAIN STREET". This loses ~30-40% of true matches. Add libpostal normalization (1M addr/sec on CPU) and replace ExactMatch with multi-level comparison (exact → transposed → Jaro-Winkler → else).

**Why it matters**: Single highest-ROI improvement to entity resolution quality. Pure model fix, no infrastructure changes.

**Synergy**: Improves everything downstream — entity_master, knowledge graph, network tools.

---

### 6. Graph Analytics: PageRank, Louvain, Centrality (Data Science + AI)
**Impact: High | Effort: 1-2 weeks**

Install the Onager DuckDB extension. Run PageRank (influence scoring), Louvain (community detection), and betweenness centrality (bridge entity detection) on the ownership/corporate/political graphs. Materialize scores to `lake.foundation.entity_influence`.

**Why it matters**: Transforms the system from pattern matching to intelligence. "Most influential actors in NYC real estate" is a headline. "Hidden ownership empires detected by community analysis" is an investigation. The current WCC-based `clusters` type over-clusters.

**Synergy**: Feeds entity_master (pagerank, community_id), powers Watchdog (anomalous centrality = alert), enables new product features (influence rankings).

---

### 7. Receipt Generator: Shareable Investigation Reports (Product)
**Impact: High | Effort: 1-2 weeks**

Turn any MCP tool output into a permanent, cited, shareable URL. The `/receipts/kessler` page proves the concept — but every investigation currently dies in the chat window. Server-side render tool outputs to static HTML with unique slugs, source citations, and data provenance chains.

**Why it matters**: Makes investigations permalink-able. Every shared receipt is distribution. Turns users into advocates.

**Synergy**: Uses citation middleware (already exists), entity_master for stable links, semantic layer for context.

---

### 8. Proactive Anomaly Detection (AI + Product)
**Impact: Medium-High | Effort: 1-2 weeks**

Background Dagster asset running Z-score anomaly detection on time-series metrics (311 complaints by ZIP/week, violations by owner/month, ACRIS sales velocity). LLM-generated summaries. Surface via `suggest(view="anomalies")`.

**Why it matters**: 60M+ rows sit inert until someone asks. Nobody notices when eviction filings spike or a landlord acquires 20 buildings in a month. This transforms passive data into active intelligence.

**Synergy**: Powers Watchdog alerts, Neighborhood Digests, feeds the semantic layer.

---

### 9. Offsite Backup + Observability (Infrastructure)
**Impact: Critical (Risk) | Effort: 2 days**

**Backup**: Postgres backups and MinIO data live on the SAME Hetzner volume. A volume failure destroys both production data AND backups. Add `mc mirror` to Hetzner Storage Box (EUR 3.50/month) + push pg_dumps offsite.

**Observability**: No metrics, no alerting, no log aggregation. Deploy Prometheus + Grafana + Loki. Add disk space alerts, container crash alerts, DuckDB memory pressure monitoring.

**Why this is #9 not #1**: It's not innovation — it's hygiene. But it's the most urgent risk.

---

### 10. MCP Server Card + Agent Discovery (AI)
**Impact: Medium | Effort: 1 day**

Add `.well-known/mcp/server-card.json` and `.well-known/agent.json` for machine-readable capability advertisement. Makes CG discoverable by MCP registries, A2A orchestrators, and agent crawlers.

**Why it matters**: When MCP adds agent-to-agent coordination (expected Q3 2026), CG becomes civic data infrastructure that any agent can discover and call. The long-term play: not a destination, but a sub-agent.

---

## Areas NOT Yet Traversed

These are capabilities that don't exist anywhere in the current system and represent genuine new territory:

| Area | What It Means | Why It's Transformative |
|------|--------------|----------------------|
| **Temporal intelligence** | Track how entities, buildings, and neighborhoods change over time. Entity timelines, address histories, trend modeling. | Transforms "what is" into "how did it become" and "what's coming" |
| **Predictive modeling** | Displacement forecasting, violation prediction, gentrification early warning | Nobody in civic tech does prediction. CG has 20+ years of historical depth to model. |
| **LLM-assisted disambiguation** | Use cheap LLMs to resolve the 5% of ambiguous entity clusters Splink can't handle | Multi-agent RAG achieves 94.3% accuracy on exactly this problem. Cost: <$5 for all 2.96M entities. |
| **Graph RAG** | Hybrid vector + graph retrieval for multi-hop reasoning | "Follow the money" in one query instead of 5 manual tool calls |
| **Elicitation** | Server asks users for clarification when results are ambiguous | FastMCP supports this. "Did you mean this John Smith or that one?" |
| **Visual network exploration** | Interactive 3D graph visualization of entity networks | The `lashley-graph-3d.html` prototype exists. Nobody ships auto-generated cross-domain graphs. |
| **Cross-city portability** | Package CG as a deployable kit for other cities | The architecture is city-agnostic. Only source definitions are NYC-specific. |
| **FOIL request generation** | When data gaps are detected, auto-generate public records requests | Nobody connects data gaps to records requests. CG knows what's missing. |
| **Civic reputation scoring** | Composite scores for buildings and landlords (like a credit score) | The percentile middleware exists. This is a product layer on top. |
| **Evidence assembly** | Court-ready PDF packets for housing court tenants | Legal aid multiplier. Data exists, packaging doesn't. |

---

## Phased Roadmap

### Phase 1: Foundation (Weeks 1-2) — "Fix the Floor"
- [ ] Offsite backup (3 hours, EUR 3.50/mo)
- [ ] Observability stack (1 day)
- [ ] Fuzzy address matching + libpostal (1 week)
- [ ] Entity master with stable IDs (1 week, starts after address fix)
- [ ] Security: move API keys out of embedder.py source code
- [ ] Remove vestigial dlt dependencies
- [ ] Unify `_connect_ducklake()` into Dagster resource injection

### Phase 2: Intelligence (Weeks 3-6) — "Make It Smart"
- [ ] Knowledge graph with typed edges (2 weeks)
- [ ] Graph analytics via Onager: PageRank, Louvain, centrality (1 week)
- [ ] Semantic layer + text-to-SQL `query(mode="nl")` (2 weeks)
- [ ] Anomaly detection background job (1 week)
- [ ] MCP Server Card + agent discovery (1 day)
- [ ] Elicitation for entity disambiguation (1 week)

### Phase 3: Product (Weeks 7-10) — "Make It Useful"
- [ ] Watchdog: subscription alerts on any entity (2 weeks)
- [ ] Receipt generator: shareable investigation reports (1-2 weeks)
- [ ] Neighborhood digests: monthly automated briefings (1 week)
- [ ] Evidence builder: legal-grade PDF assembly (1 week)
- [ ] Compare mode: side-by-side multi-entity analysis (1 week)

### Phase 4: Scale (Weeks 11-16) — "Make It Big"
- [ ] REST API for non-AI consumers (2 weeks)
- [ ] DuckDB read replicas behind load balancer (1 day)
- [ ] Graph RAG: hybrid vector + graph retrieval (2-3 weeks)
- [ ] Visual graph explorer (2 weeks)
- [ ] Temporal entity tracking + timelines (2 weeks)
- [ ] LLM-assisted disambiguation for ambiguous clusters (1 week)
- [ ] Incremental entity resolution (delta processing) (2 weeks)

### Phase 5: Expansion (Q3 2026+) — "Make It Everywhere"
- [ ] Predictive displacement modeling
- [ ] Cross-city portable format ("Common Ground Chicago")
- [ ] A2A federation (when MCP spec supports it)
- [ ] FOIL request generator
- [ ] Civic reputation scoring system
- [ ] Monetization tiers (Pro/Institutional)

---

## Cross-Cutting Synergies

```
Entity Master (stable IDs)
    ├── Knowledge Graph (typed edges need entity_id)
    │   ├── Graph Analytics (PageRank, Louvain on the KG)
    │   ├── Graph RAG (traverse KG for multi-hop queries)
    │   └── Visual Graph Explorer (render the KG)
    ├── Watchdog Alerts (subscribe to entity_id changes)
    ├── Receipt Generator (permalink entities by ID)
    └── Evidence Builder (assemble by entity_id)

Semantic Layer
    ├── Text-to-SQL (validate NL queries against metrics)
    ├── Anomaly Detection (compute anomalies on defined metrics)
    └── Neighborhood Digests (template from metric definitions)

Address Standardization (libpostal)
    ├── Entity Resolution quality (+30-40% true matches)
    ├── Building tool accuracy
    └── Knowledge Graph edge quality
```

**The critical insight**: Entity Master is the keystone. Almost every Phase 2-4 feature depends on stable entity IDs. Build it first.

---

## Competitive Position After Execution

| Capability | Before | After Phase 2 | After Phase 4 |
|-----------|--------|--------------|--------------|
| Entity resolution | Good (Splink) | Best-in-class (+ libpostal, confidence scores) | World-class (+ LLM disambiguation, temporal) |
| Graph intelligence | Ad-hoc (42 tables) | Knowledge graph with analytics | Graph RAG + visual exploration |
| Query interface | Raw SQL via MCP | Semantic layer + NL queries | + REST API + comparison tools |
| User engagement | Ephemeral chat sessions | Alerts + receipts + digests | Full product with monetization |
| Infrastructure | Single server, no monitoring | Monitored, backed up, hardened | Read replicas, CI/CD, scaled |
| Agent ecosystem | MCP server (manual config) | Discoverable (server card) | Federated (A2A) |

**Bottom line**: Common Ground is already the most sophisticated open civic data platform in existence. With this roadmap, it becomes the first **civic intelligence platform** — not just data access, but active monitoring, investigation tools, and predictive analysis. The moat deepens from "we have more data" to "we understand the data better than anyone."

---

*Generated by 5-agent innovation audit team, April 2026*
*Agents: platform-architect, ai-strategist, product-strategist, data-scientist, infra-engineer*
