# Entity Resolution & Data Quality Audit

**Auditor**: data-scientist (CG Innovation Audit)
**Date**: 2026-04-01
**Scope**: Splink ER pipeline, name index, Lance embeddings, entity/network tools, graph infrastructure

---

## 1. Current State Assessment

### 1.1 Architecture Overview

The ER system is a 4-stage pipeline:

```
47 lake tables
    --> name_index (UNION ALL + cross-ref filter, ~55M rows)
        --> resolved_entities (Splink predict + cluster in 500K batches)
            --> entity_name_embeddings (Gemini vectors in Lance, 2.96M names)
```

**Runtime tooling** (MCP server) adds:
- Phonetic search via `double_metaphone` + `jaro_winkler_similarity` (`entity.py`)
- Fuzzy matching via `rapidfuzz_token_sort_ratio` (`entity.py`)
- Lance HNSW vector search for name expansion and source routing (`shared/lance.py`)
- DuckPGQ property graphs for ownership, corporate, political, and contractor networks (`shared/graph.py`, `tools/network.py`)

### 1.2 What Works Well

1. **Scale**: Processing 55M records through Splink in ~500K batches with phonetic grouping is solid engineering. Greedy bin-packing by `dm_last` keeps batches cohesive.

2. **Name Registry** (`sources/name_registry.py`): 47 tables auto-discovered with 6 extraction patterns (STRUCTURED, COMBINED_COMMA, COMBINED_SPACE, OWNER, RESPONDENT, MULTI). LLC filtering is well-designed. This is a genuinely impressive piece of domain modeling.

3. **Lance Vector Index**: 2.96M embedded names with HNSW search enables semantic name expansion (finding "SMITH" when searching "SMYTH") and source routing (skipping empty tables). The 10-27s to ~2s speedup from Lance routing is significant.

4. **DuckPGQ Property Graphs**: Already using SQL/PGQ with `GRAPH_TABLE` queries and `weakly_connected_component()` for ownership clustering. 41 graph tables covering housing, corporate, political, contractor, officer, and trade waste networks. This is ahead of most civic data platforms.

5. **Phonetic Blocking**: Double Metaphone for blocking + Jaro-Winkler for scoring is the textbook approach for person names. The Splink model uses 3 blocking rules (phonetic+phonetic, phonetic+zip, exact+city) which provides good coverage.

### 1.3 Gaps and Weaknesses

#### A. Entity Resolution Quality

1. **No ground truth / evaluation framework**: The model was validated on 10K records with self-referential metrics (multi-record cluster count). No precision/recall/F1 against labeled data. No clerical review sample.

2. **Address comparison is binary**: The Splink model uses `ExactMatch` for address, city, and zip. No fuzzy address matching (e.g., "123 MAIN ST" vs "123 MAIN STREET" vs "123 MAIN ST APT 2"). This is the single biggest accuracy gap -- address is the strongest disambiguation signal after name, but only works when strings are identical.

3. **No date-of-birth or age comparison**: Many datasets contain DOB or age fields that aren't used in the model. DOB is the gold standard for person disambiguation.

4. **Cross-batch linkage is absent**: Batches are grouped by phonetic last name, so "SMITH" records are never compared against "SMYTH" records. The Splink clustering is batch-local, meaning the same person with different last name spellings across sources will never be linked.

5. **No entity type distinction**: Persons and organizations flow through the same pipeline. An LLC named "SMITH HOLDINGS" could cluster with person "John Smith". The `LLC_FILTER_TERMS` in name_registry helps but isn't applied during Splink comparison.

6. **Cluster IDs are ephemeral**: Each full materialization generates new cluster IDs. There's no stable entity ID that persists across runs. This makes temporal tracking impossible.

#### B. Data Quality

7. **No address standardization**: Addresses arrive in 47 different formats from 47 different sources. No normalization step (libpostal, USPS API, or even regex-based cleaning) runs before comparison.

8. **Name normalization is minimal**: Only `UPPER()` and `TRIM()` are applied. No handling of: suffixes (Jr., III), prefixes (Dr., Rev.), hyphenated names, middle name vs. first name swaps, nicknames (Bill/William, Bob/Robert).

9. **No data quality metrics pipeline**: No automated checks for: null rate by column, duplicate rate, format consistency, referential integrity between graph tables.

10. **No freshness tracking on entity tables**: `resolved_entities` and `entity_name_embeddings` have no automated schedule. Stale embeddings mean new data sources won't appear in searches.

#### C. Graph & Network

11. **Graph tables are materialized as flat Parquet, not a true graph store**: 41 tables cached as Parquet files, loaded into DuckDB memory on startup. No persistent graph index. Each restart re-materializes from lake queries.

12. **No centrality or PageRank computation**: Despite having graph infrastructure, no pre-computed importance scores exist. Every query does live traversal, which limits depth and breadth.

13. **No community detection**: The `weakly_connected_component` is used for ownership clustering only. No Louvain/Leiden/label propagation for detecting hidden communities across the full network.

14. **Network types are siloed**: Ownership, corporate, political, contractor, officer, and trade waste networks are separate property graphs with no cross-type traversal. A landlord's political donations aren't connected to their building violations in the graph.

---

## 2. Recommendations

### 2.1 Quick Wins (1-2 weeks each)

#### R1. Address Standardization with libpostal
Add a pre-processing step in `name_index_asset.py` that normalizes addresses before they enter the name index:
```
raw address -> libpostal parse -> standardized components (house_number, road, city, state, postcode)
```
libpostal is a C library with Python bindings (`postal`), runs locally, handles international addresses, and can process millions of records. This would unlock fuzzy address matching in Splink by comparing parsed components instead of raw strings.

**Impact**: Estimated 15-25% improvement in entity linkage recall based on published benchmarks.

#### R2. Add Fuzzy Address to Splink Model
Replace `ExactMatch("address")` with a `LevenshteinAtThresholds` or `JaroWinklerAtThresholds` comparison. After standardization (R1), add comparisons on parsed components:
```python
cl.LevenshteinAtThresholds("house_number", [1]),
cl.JaroWinklerAtThresholds("road", [0.9, 0.7]),
cl.ExactMatch("postcode").configure(term_frequency_adjustments=True),
```

#### R3. Nickname/Diminutive Expansion
Create a lookup table mapping common name variants (William/Bill/Will/Billy, Robert/Bob/Bobby, etc.). Apply during name_index construction or as an additional Splink comparison level. The `splink` library supports custom comparison functions for this.

#### R4. Stable Entity IDs
Generate deterministic cluster IDs based on the canonical name + strongest address signal, so IDs persist across re-materializations. This is prerequisite for temporal tracking.

### 2.2 Medium-Term Improvements (1-2 months)

#### R5. LLM-Assisted Disambiguation
Use a two-stage approach (per recent research):
1. **Retrieval**: Splink/phonetic/vector search narrows candidates (current approach)
2. **Judgment**: LLM evaluates ambiguous pairs (Splink match probability 0.5-0.9) with full context from all source tables

This addresses the hardest cases: same name + different address (moved?), different name + same address (family?), partial data overlap. Multi-agent RAG frameworks show best results, with specialized agents for semantic similarity, temporal consistency, and contextual disambiguation.

**Key constraint**: LLMs are slow (~1-5 seconds per pair). Only use for the ambiguous middle tier, not the clear matches/non-matches at the tails.

#### R6. Cross-Batch Linkage via Transitive Closure
After all batches complete, run a second pass that:
1. Finds entities matched to the same address across different phonetic batches
2. Uses Lance vector similarity to find name variants across batches
3. Merges clusters with transitive closure

#### R7. Unified Property Graph
Merge all 6 separate graph types into a single property graph with typed edges:
```sql
CREATE PROPERTY GRAPH nyc_unified (
    VERTEX TABLE entities ...,
    VERTEX TABLE buildings ...,
    VERTEX TABLE organizations ...,
    EDGE TABLE owns ...,
    EDGE TABLE donates_to ...,
    EDGE TABLE contracts_with ...,
    EDGE TABLE works_at ...,
    EDGE TABLE lives_at ...,
    ...
)
```
This enables cross-type queries like: "Find landlords who donated to the councilmember whose district contains their worst buildings."

#### R8. Pre-computed Graph Analytics
After graph construction, run and persist:
- **PageRank** per entity (who is most connected?)
- **Betweenness centrality** (who bridges separate networks?)
- **Community detection** via Leiden algorithm (who forms hidden clusters?)
- Store as columns on entity nodes, queryable via MCP tools

### 2.3 Strategic / Long-Term (3-6 months)

#### R9. Knowledge Graph Layer (Kuzu or DuckPGQ)
The current DuckPGQ usage is solid but limited to housing/corporate domains. A full knowledge graph would:
- Store resolved entities as first-class nodes with stable IDs
- Connect to all 294 lake tables (not just the 47 with person names)
- Support temporal edges (relationship valid_from/valid_to)
- Enable Cypher-like path queries: "What connects Person A to Building B?"

**Kuzu** is worth evaluating as a complement to DuckPGQ:
- Embedded (no server), reads DuckDB tables via `ATTACH`
- Native Cypher support with graph analytics (PageRank, WCC, community detection built-in)
- LangChain and LlamaIndex integrations for RAG over the knowledge graph
- But: adds another dependency to the stack

**DuckPGQ** is the lower-friction path:
- Already in use, same DuckDB engine
- SQL/PGQ is the ISO standard (SQL:2023)
- Community extension, still maturing but actively developed
- Missing some advanced algorithms (no native PageRank yet)

**Recommendation**: Deepen DuckPGQ usage first. Add Kuzu only if DuckPGQ can't handle the required graph algorithms.

#### R10. Temporal Entity Tracking
Build a temporal knowledge graph layer:
1. Each entity gets a timeline: first_seen, last_seen, per source
2. Address history: track when entities appear at different addresses
3. Relationship evolution: when did a person become an officer of an LLC?
4. Anomaly detection: sudden appearance/disappearance of entities

This is where the value proposition shifts from "who is this person?" to "what changed about this person?" -- critical for investigative journalism.

#### R11. Data Quality Pipeline
Create a Dagster asset group `quality.*` that computes:
- Null rates per column per table (flag >50% null)
- Name format consistency scores (% matching expected pattern)
- Cross-reference density (how many tables mention each entity?)
- Address geocoding success rate
- Duplicate detection rates per table

Expose via MCP `quality()` tool for users to query data health.

---

## 3. Tool & Technology Evaluation

### 3.1 ER Frameworks Comparison

| Feature | Splink (current) | Zingg | Dedupe.io | AWS Entity Resolution |
|---------|-----------------|-------|-----------|----------------------|
| Approach | Fellegi-Sunter probabilistic | Active learning ML | Active learning ML | Cloud service |
| Scale | 100M+ (DuckDB/Spark) | 100M+ (Spark) | ~1M (Python) | Unlimited |
| Training | EM algorithm, no labels | Active learning, few labels | Active learning, few labels | Pre-trained models |
| DuckDB native | Yes | No (Spark) | No | No |
| Graph output | No | No | No | No |
| Open source | Yes (MIT) | Yes (AGPL) | Yes (MIT) | No |
| Best for | Structured data, known comparisons | Unstructured, ML-first | Small-medium datasets | AWS-native stacks |

**Verdict**: Splink remains the best fit for CG. It's DuckDB-native, handles 55M records, and the Fellegi-Sunter model is interpretable. The gaps are in preprocessing (address standardization) and post-processing (cross-batch linkage, graph construction), not in the core ER engine.

### 3.2 Graph Technology Comparison

| Feature | DuckPGQ (current) | Kuzu | Neo4j |
|---------|-------------------|------|-------|
| Deployment | Extension in DuckDB | Embedded library | Server |
| Query language | SQL/PGQ | Cypher + SQL/PGQ | Cypher |
| DuckDB integration | Native | ATTACH tables | ETL required |
| Graph algorithms | WCC, shortest path | PageRank, WCC, Leiden, etc. | Full GDS library |
| Vector search | Via hnsw_acorn | Built-in | Built-in |
| LLM integrations | Manual | LangChain, LlamaIndex | LangChain, LlamaIndex |
| Maturity | Research prototype | Production-ready | Mature |

**Verdict**: DuckPGQ for core graph queries (already working). Evaluate Kuzu for graph analytics (PageRank, community detection) that DuckPGQ doesn't yet support. Avoid Neo4j -- too heavy for an embedded civic data platform.

---

## 4. Priority Roadmap

### Phase 1: Foundation (Weeks 1-4)
- [R1] Address standardization with libpostal
- [R2] Fuzzy address comparison in Splink model
- [R4] Stable entity IDs
- [R11] Data quality metrics pipeline (basic)

### Phase 2: Intelligence (Weeks 5-10)
- [R3] Nickname/diminutive expansion
- [R6] Cross-batch linkage
- [R7] Unified property graph
- [R8] Pre-computed graph analytics (PageRank, centrality)

### Phase 3: Knowledge Graph (Weeks 11-16)
- [R5] LLM-assisted disambiguation for ambiguous pairs
- [R9] Full knowledge graph with temporal edges
- [R10] Temporal entity tracking and anomaly detection

---

## 5. Infrastructure Requirements for Teammates

### For platform-architect:
- **libpostal**: C library, needs to be built and available in the Docker container (~2GB model files). Consider adding to Dockerfile.
- **Kuzu evaluation**: If adopted, runs as an embedded Python library (pip install kuzu). No server infrastructure needed.
- **Graph cache strategy**: Current 41-table Parquet cache works but doesn't scale to a unified graph. Consider keeping DuckPGQ graphs persistent (supported since duckpgq v0.1.0 with DuckDB 1.1.3+).
- **Stable entity ID storage**: Need a persistent mapping table in DuckLake (entity_id -> cluster members) that survives re-materialization.

### For ai-strategist:
- **LLM disambiguation endpoint**: Need a lightweight API for evaluating entity pairs. Could use the existing MCP server's Gemini connection or a dedicated endpoint.
- **Vector search improvements**: Current HNSW index uses flat cosine distance. Consider IVF_PQ for faster search at 2.96M+ scale.
- **RAG over knowledge graph**: Once the unified graph exists (R7), Kuzu's LangChain integration enables graph-aware RAG -- answering questions like "How is X connected to Y?" by traversing the graph rather than scanning tables.
- **Multi-agent ER**: The multi-agent RAG pattern (specialized agents for matching, transitive linkage, household clustering) maps well to CG's domain -- could be exposed as an MCP tool.

---

## Appendix: Key File References

| Component | Path | Lines |
|-----------|------|-------|
| Splink model training | `scripts/train_splink_model.py` | 107 |
| Splink model config | `models/splink_model_v2.json` | 230 |
| Name index asset | `src/.../defs/name_index_asset.py` | 157 |
| Resolved entities asset | `src/.../defs/resolved_entities_asset.py` | 281 |
| Entity embeddings asset | `src/.../defs/entity_embeddings_asset.py` | 171 |
| Name registry (47 tables) | `src/.../sources/name_registry.py` | ~400 |
| Phonetic/fuzzy search | `infra/duckdb-server/entity.py` | 139 |
| Lance vector helpers | `infra/duckdb-server/shared/lance.py` | 64 |
| Graph infrastructure | `infra/duckdb-server/shared/graph.py` | 81 |
| Entity tool (MCP) | `infra/duckdb-server/tools/entity.py` | ~1600 |
| Network tool (MCP) | `infra/duckdb-server/tools/network.py` | ~2100 |
