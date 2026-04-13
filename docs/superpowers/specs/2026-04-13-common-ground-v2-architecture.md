# Common Ground v2 Architecture: Graph Intelligence Layer

**Date:** 2026-04-13
**Status:** Design spec — validated through research, not yet implemented

## The Problem

Common Ground serves 400M+ rows of NYC civic data through 15 MCP super tools. When an LLM answers "who owns the worst buildings in Sunset Park?", it needs 5+ sequential tool calls because it can't see the shape of the data. It guesses which tools to chain, often misses connections, and cuts corners. The data is there but the LLM can't navigate it.

## The Insight

Every successful civic knowledge graph system (Turku, LittleSis, Aleph, Kadaster, OpenCorporates) kept their underlying storage and added a **navigation layer** on top. The storage doesn't matter — the intelligence layer does.

Three research findings drive the architecture:

1. **Semantic context = 3.2x accuracy** — LLMs go from 16.7% to 54.2% SQL accuracy when given a business-domain model instead of raw DDL (WrenAI/data.world research)
2. **LazyGraphRAG = 1000x cheaper** — Microsoft's LazyGraphRAG skips LLM summarization during indexing, builds a lightweight graph index, does LLM reasoning only at query time. 0.1% of full GraphRAG cost.
3. **SpaCy = 94% of GPT-4o** — Dependency parsing achieves 94% of LLM quality for entity/relationship extraction (SAP research, arxiv 2507.03226). Classical NLP for graph construction, LLM only for reasoning.

## What Stays

The existing stack is validated. Nothing migrates.

| Component | Version | Role | Status |
|-----------|---------|------|--------|
| DuckDB | 1.5 "Variegata" | Analytical engine, native GEOMETRY, concurrent R/W | Production |
| DuckLake | 1.0 (released today) | Lakehouse catalog (Postgres metadata + Parquet) | Production |
| DuckPGQ | Community ext | Property graph views over lakehouse tables | Production |
| hnsw_acorn | Community ext | HNSW vector search for name embeddings | Production |
| Splink 4.0 | Python | Probabilistic entity resolution | Production |
| ADBC | Arrow transport | Zero-copy bulk data movement | Production |
| FastMCP 3.x | Python | MCP + REST + Mosaic server | Production |
| Dagster 1.12 | Docker | Orchestration, sensors, schedules | Production |
| Postgres | Catalog DB | DuckLake metadata + future platform schema | Production |

## What's New: The Intelligence Layer

Three new components sit on top of the existing stack. No storage migration. No new databases. Metadata and views only.

### 1. Entity Property Graph (DuckPGQ)

Define a unified property graph over the lakehouse tables:

```sql
CREATE PROPERTY GRAPH civic_graph
VERTEX TABLES (
    -- Core entities
    lake.pluto.pluto          LABEL Building,
    lake.entity.resolved      LABEL Entity,
    lake.entity.llcs          LABEL Corporation,
    -- Events
    lake.hpd.violations       LABEL Violation,
    lake.dob.complaints       LABEL Complaint,
    lake.dob.permits          LABEL Permit,
    -- Political
    lake.campaign.contributions LABEL Donation,
    lake.lobbying.registrations LABEL LobbyingRelation,
    -- Transactions
    lake.acris.master          LABEL Transaction
)
EDGE TABLES (
    -- Ownership
    lake.acris.parties         LABEL OWNED_BY,
    -- Violations/complaints belong to buildings
    lake.hpd.violations        LABEL HAS_VIOLATION,
    lake.dob.complaints        LABEL HAS_COMPLAINT,
    lake.dob.permits           LABEL HAS_PERMIT,
    -- Political connections
    lake.campaign.contributions LABEL DONATED_TO,
    -- Corporate control
    lake.entity.corporate_links LABEL CONTROLS
);
```

Zero data copied. The graph IS the lakehouse tables, queried with graph pattern matching:

```sql
FROM GRAPH_TABLE (civic_graph
    MATCH (b:Building)-[:OWNED_BY]->(e:Entity)
                      -[:CONTROLS]->(c:Corporation)
                      -[:DONATED_TO]->(pol:Entity)
    WHERE b.zip = '11220'
    COLUMNS (e.name AS owner, c.name AS llc, pol.name AS politician,
             COUNT(*) AS connection_count)
)
```

**Implementation:** Dagster asset that creates/updates the property graph DDL. Runs on deploy, not on schedule (schema changes only when new tables are added).

### 2. Community Detection + Summaries (LazyGraphRAG Pattern)

#### 2a. Community Detection

Extract subgraphs from DuckPGQ via ADBC, run Leiden clustering using graphify's `cluster.py`:

```
DuckPGQ graph → ADBC Arrow extraction → NetworkX subgraph → Leiden → communities
```

Scope: run community detection on the **ownership network** first (buildings ↔ entities ↔ corporations). This is the highest-value subgraph — landlord portfolios, shell company clusters, political donor networks.

**Output:** A DuckLake table `lake.graphs.communities` with columns:
- `community_id` (int)
- `entity_ids` (list of entity IDs in the community)
- `size` (node count)
- `cohesion` (float, intra-community edge density)
- `label` (auto-generated or LLM-summarized name)

**Implementation:** Dagster asset, runs daily. Uses graphify's `cluster.py` (Leiden with oversized-community splitting). Materializes to `lake.graphs.communities`.

#### 2b. Community Summaries (LazyGraphRAG approach)

For each community, generate a natural language summary. Two strategies:

**Cheap path (SpaCy + templates):** For most communities, a template-based summary from graph statistics:
> "Portfolio of 47 buildings in Sunset Park, controlled by 3 LLCs linked to Entity X. Average 12.3 HPD violations per building (3.2x borough average). Entity X donated $45K to Council Member Y."

**LLM path (top communities only):** For the top ~50 communities by size/cohesion, call an LLM to generate an investigative-quality summary from the community's full subgraph context.

**Output:** A DuckLake table `lake.graphs.community_summaries` with columns:
- `community_id` (int)
- `summary` (text, 100-300 words)
- `key_entities` (list of top entity names)
- `key_metrics` (JSON: violation counts, donation totals, building counts)
- `generated_at` (timestamp)

**Implementation:** Dagster asset, runs daily after community detection. Template-based for most, LLM for top 50.

### 3. Semantic Model + MCP Resources

#### 3a. Semantic Model (WrenAI-inspired MDL)

Define the 294 tables in business terms. This is metadata, not code — a YAML/JSON document the LLM reads before calling tools:

```yaml
entities:
  Building:
    description: "A physical structure in NYC identified by BBL (Borough-Block-Lot)"
    primary_key: bbl
    dimensions: [address, borough, zip, year_built, building_class, units]
    measures:
      violation_count: "COUNT of HPD violations"
      complaint_count: "COUNT of DOB complaints"
      last_sale_price: "Most recent ACRIS sale amount"
    relationships:
      - OWNED_BY → Entity (via acris.parties)
      - HAS_VIOLATION → Violation (via hpd.violations)
      - HAS_COMPLAINT → Complaint (via dob.complaints)
      - NEAR → Building (via H3 spatial proximity)

  Entity:
    description: "A resolved person or company — deduplicated across agencies via Splink"
    primary_key: entity_id
    dimensions: [name, entity_type, confidence_score]
    relationships:
      - OWNS → Building (via acris.parties)
      - CONTROLS → Corporation (via corporate_links)
      - DONATED_TO → Entity (via campaign.contributions)

  # ... all entity types
```

#### 3b. MCP Resources

Expose the semantic model, community summaries, and data map as MCP Resources. The LLM reads these *before* deciding which tools to call:

```python
@mcp.resource("civic://schema")
def get_schema() -> str:
    """Business-domain description of all entity types and relationships."""
    return load_semantic_model()

@mcp.resource("civic://communities")
def get_communities() -> str:
    """Natural language summaries of the top ownership/political communities."""
    return load_community_summaries()

@mcp.resource("civic://data-map")
def get_data_map() -> str:
    """Navigable hierarchy: Domains → Schemas → Entity types → Available measures."""
    return load_data_map()
```

**Query flow after v2:**

```
Before:  LLM → guess tool → call → guess next → call → call → call → synthesize
After:   LLM → read schema resource → read community summary → one targeted tool call → done
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│  LLM (Claude via MCP)                               │
│  1. Reads MCP Resources (schema, communities, map)  │
│  2. Makes ONE informed tool call                     │
└──────────────┬──────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────┐
│  FastMCP Server (mcp.common-ground.nyc)             │
│  ├── MCP Resources (semantic model, summaries)      │
│  ├── 15 Super Tools (building, entity, network...)  │
│  └── Mosaic REST API                                │
└──────────────┬──────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────┐
│  Intelligence Layer (NEW)                           │
│  ├── DuckPGQ property graph (views, zero copy)      │
│  ├── Community detection (Leiden via graphify)       │
│  ├── Community summaries (template + LLM)           │
│  └── Semantic model (MDL YAML)                      │
└──────────────┬──────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────┐
│  DuckDB 1.5 + DuckLake 1.0                          │
│  400M+ rows, 294 tables, 14 schemas                 │
│  Native GEOMETRY, hnsw_acorn, Splink ER             │
│  Parquet on NVMe + Postgres catalog                 │
└─────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Property Graph DDL (1-2 days)
- Define `CREATE PROPERTY GRAPH civic_graph` covering core entity types
- Dagster asset that runs the DDL on deploy
- Test graph traversals: ownership chain, violation aggregation, political connections
- Validate DuckPGQ handles the scale (400M nodes across vertex tables)

### Phase 2: Community Detection (2-3 days)
- ADBC extraction of ownership subgraph → Arrow → NetworkX
- graphify `cluster.py` Leiden clustering
- Materialize `lake.graphs.communities` table
- Dagster asset, daily schedule
- graphify `analyze.py` for god_nodes, surprising_connections

### Phase 3: Community Summaries (2-3 days)
- Template-based summaries for all communities (counts, averages, key entities)
- LLM summaries for top 50 communities
- Materialize `lake.graphs.community_summaries` table
- Dagster asset, daily after Phase 2

### Phase 4: Semantic Model + MCP Resources (2-3 days)
- Write semantic MDL YAML describing all entity types, relationships, measures
- Implement MCP Resource endpoints (schema, communities, data-map)
- Update MCP tool routing prompt to reference resources
- Test: LLM answers "who owns the worst buildings in Sunset Park?" in one tool call

### Phase 5: Integration Testing (1-2 days)
- End-to-end: question → resource read → tool call → answer
- Compare: v1 (5+ calls, frequent errors) vs v2 (1-2 calls, informed)
- Measure accuracy improvement
- Deploy to production

## Key Design Decisions

### Why DuckPGQ over Neo4j/Kuzu?
- Zero data movement — graph is a view over existing lakehouse tables
- Single process — no client-server overhead on the Hetzner box
- SQL/PGQ is the ISO standard (also coming to Postgres natively)
- Already in production, already tested

### Why LazyGraphRAG pattern over full GraphRAG?
- 1000x cheaper indexing (no LLM calls during graph construction)
- Your data is already structured (tables, not documents) — entity extraction is SQL, not NLP
- LLM reasoning at query time on relevant subgraph only
- Template summaries for 90% of communities, LLM for top 10%

### Why not WrenAI as a dependency?
- WrenAI is a full deployment (Docker, DataFusion engine, UI)
- The insight is the MDL format, not the engine
- Hand-rolled semantic YAML + MCP Resources achieves the same LLM context improvement
- Can adopt WrenAI later if the semantic layer needs grow

### Why graphify's cluster.py?
- Already in the codebase (opensrc/repos/github.com/safishamsi/graphify)
- Leiden algorithm with oversized-community splitting — handles the hub problem
- Domain-agnostic — works on civic entities same as code entities
- Proven: produced 140 communities from your codebase graph

## Research Sources

- [DuckLake v1.0 release](https://ducklake.select/2026/04/13/ducklake-10/) (April 13, 2026)
- [DuckDB 1.5 announcement](https://duckdb.org/2026/03/09/announcing-duckdb-150) — native GEOMETRY, concurrent R/W
- [PostgreSQL SQL/PGQ commit](https://commitfest.postgresql.org/patch/4904/) (January 2026)
- [LazyGraphRAG](https://www.microsoft.com/en-us/research/blog/lazygraphrag-setting-a-new-standard-for-quality-and-cost/) — 0.1% of GraphRAG indexing cost
- [Practical GraphRAG](https://arxiv.org/abs/2507.03226) — SpaCy dependency parsing at 94% of GPT-4o quality
- [GraphRAG 90% cost reduction](https://medium.com/graph-praxis/cutting-graphrag-token-costs-by-90-in-production-5885b3ffaef0) (March 2026)
- [WrenAI semantic layer](https://github.com/Canner/WrenAI) — 10x text-to-SQL improvement with MDL
- [Cognee agent memory](https://github.com/topoteretes/cognee) — Claude SDK integration, 70+ companies
- [Turku smart city](https://n-bridges.com/) — property graph, bottom-up design, Neo4j
- [OCCRP Aleph](https://github.com/alephdata/aleph) — FollowTheMoney model, ElasticSearch
- [LittleSis](https://github.com/public-accountability/littlesis-rails) — 301K people, PostgreSQL, community wiki
- [OpenCorporates](https://opencorporates.com/) — migrated Neo4j → TigerGraph at 220M companies
- [Kadaster knowledge graph](https://data.labs.kadaster.nl/) — 2B RDF triples, cross-agency property data
- [KnowWhereGraph](https://knowwheregraph.org/) — 29B triples, geospatial knowledge graph
- [graphify source analysis](https://github.com/safishamsi/graphify) — reusable build/cluster/analyze/serve/export pipeline
- concept-hop cross-domain scan (this session) — 440 packages across 14 platforms

## What This Enables

Once the intelligence layer is in place:

1. **One-call answers** — LLM reads community summary, makes one informed tool call, done
2. **Automatic investigation leads** — graphify's `surprising_connections()` surfaces non-obvious links daily
3. **Temporal tracking** — graphify's `graph_diff()` shows what changed since last sync
4. **Obsidian investigation vaults** — graphify's `wiki.py` generates browsable `[[wikilink]]` files for analysts
5. **Cross-community analysis** — "this donor network connects to that landlord portfolio" emerges from graph structure
6. **Foundation for Mirror** — the graph navigation layer IS what Mirror would replicate to other consumers
