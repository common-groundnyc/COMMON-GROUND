# AI/MCP Integration Layer Audit

**Auditor**: ai-strategist (cg-innovation-audit team)
**Date**: 2026-04-01
**Scope**: MCP server architecture, tool design, entity resolution, semantic search, middleware pipeline, and alignment with 2026 AI best practices

---

## 1. Current Architecture Assessment

### 1.1 MCP Server Stack

The platform exposes NYC open data through **FastMCP** (`mcp_server.py:1783-1788`) with 14 "super tools" registered as read-only endpoints. The architecture is:

```
Claude/LLM Client
    |  (Streamable HTTP / SSE)
    v
FastMCP Server (:4213)
    |-- Middleware Pipeline (4 layers)
    |-- CursorPool (16 cursors, 30s timeout)
    |-- DuckDB (in-process, 8GB memory, 8 threads)
    |      |-- DuckLake catalog (Postgres metadata)
    |      |-- MinIO S3 (parquet data files)
    |      |-- DuckPGQ property graphs (8 named graphs)
    |      |-- hnsw_acorn embeddings DB (separate .duckdb file)
    |-- Background threads (graph build, embeddings, percentiles)
```

**Key file references:**
- Server entry: `mcp_server.py` (1900+ lines -- the monolith)
- Tool registration: `mcp_server.py:1795-1814`
- Lifespan/startup: `mcp_server.py:164-1736` (1,500+ lines of initialization)
- Tool implementations: `tools/*.py` (14 files, ~6,000 lines total)
- Entity resolution: `entity.py` (phonetic + Jaro-Winkler + rapidfuzz)
- Embeddings: `embedder.py` (Gemini multi-key pool, Vertex AI, ONNX fallback)
- Vector search: `shared/lance.py` (hnsw_acorn cosine distance)
- Cursor pool: `cursor_pool.py` (thread-safe with timeout/replacement)

### 1.2 Super Tool Architecture (Strong)

The consolidation from 63 tools to 14 (`SUPER-TOOLS.md`) is excellent and ahead of industry practice. Each super tool uses a discriminated union pattern with a `view`/`role`/`type` parameter:

| Tool | Absorbed | Pattern |
|------|----------|---------|
| `building()` | 11 tools | `view` param: full/story/block/similar/enforcement/history/flippers |
| `entity()` | 9 tools | `role` param: auto/background/cop/judge/vitals/top |
| `neighborhood()` | 8 tools | `view` param: full/compare/gentrification/environment/hotspot/area/restaurants |
| `network()` | 15 tools | `type` param: landlord/corporate/political/property/contractor/tradewaste/officer/clusters/cliques/worst |
| `query()` | 6 tools | `mode` param: sql/catalog/schemas/tables/describe/health/admin |
| `semantic_search()` | 4 tools | `domain` param: auto/complaints/violations/entities/explore |
| + 8 domain tools | safety, health, legal, civic, transit, services, school, suggest |

**Token budget**: ~3,500 tokens for all 14 tool schemas vs ~15,000 for 63. This is the "fat tool" pattern recommended by the MCP community.

### 1.3 Middleware Pipeline (Strong)

Four middleware layers in `middleware/` process every tool response:

1. **OutputFormatterMiddleware** (`response_middleware.py`): Auto-detects optimal format (kv/markdown/TOON) based on row count and column count. TOON format for 51+ rows is token-efficient.
2. **CitationMiddleware** (`citation_middleware.py`): Appends "Sources: table_name (N rows)" to every response.
3. **FreshnessMiddleware** (`freshness_middleware.py`): Warns when data is stale based on pipeline state.
4. **PercentileMiddleware** (`percentile_middleware.py`): Injects percentile rankings into numeric results.

### 1.4 Entity Resolution (Functional but Limited)

Three matching strategies (`entity.py`):
- **Phonetic blocking**: Double Metaphone on `phonetic_index` table, then Jaro-Winkler scoring (0.6 last + 0.4 first weight)
- **Fuzzy name matching**: `rapidfuzz_token_sort_ratio` with configurable min_score (default 70)
- **Vector similarity**: Gemini embeddings in hnsw_acorn DuckDB, cosine distance < 0.21

The Lance entity index (`shared/lance.py:29-63`) routes entity lookups by finding which source tables contain a name, skipping empty sources. This reduced entity_xray from 10-27s to ~2s.

### 1.5 Semantic Search (Solid Foundation)

`semantic_search()` (`tools/semantic_search.py`) combines:
- ILIKE keyword search across 311, CFPB, HPD, restaurant, OATH tables
- Vector similarity via hnsw_acorn for description categories
- Auto-routing based on keyword heuristics (`_detect_domain()`)

**Current corpus**: ~5,000 embedded descriptions (311 complaint types, restaurant violations, HPD violations, OATH charges), plus ~2.96M entity names.

### 1.6 Graph Layer (Impressive)

8 DuckPGQ property graphs (`mcp_server.py:1310-1484`):
- `nyc_ownership` (owners -> buildings -> violations)
- `nyc_transactions` (ACRIS co-transactors)
- `nyc_corporate_web` (corps -> shared officers)
- `nyc_influence` (donors -> recipients)
- `nyc_contractor_network` (shared permits/buildings)
- `nyc_officer_network` (shared command)
- `nyc_tradewaste_network` (shared locations)
- `nyc_coib_network` (donors -> policymakers)

~45 graph tables materialized at startup, cached as Parquet for 24h fast restart.

---

## 2. Gaps and Opportunities

### 2.1 CRITICAL: Hardcoded API Keys in Source Code

`embedder.py:19-33` contains 14 Gemini API keys in plaintext. This is a security incident waiting to happen -- keys are committed to git and visible in the codebase. These must be moved to environment variables or a secret manager immediately.

### 2.2 No MCP Resources (Schema Discovery)

The server registers zero MCP Resources. The 2026 MCP spec strongly recommends exposing schema information as Resources rather than embedding it in tool docstrings. This would enable:
- LLMs to browse schemas without a tool call
- Selective loading of relevant schema context
- Better context management for large catalogs (294 tables)

**Recommendation**: Expose `catalog://schemas`, `catalog://tables/{schema}`, and `catalog://columns/{schema}/{table}` as MCP Resources.

### 2.3 No Elicitation Support

FastMCP 2.10+ supports Elicitation -- the ability for tools to ask the user for clarification mid-execution. The current tools silently guess intent (e.g., `_detect_domain()` in semantic_search). With elicitation:
- Ambiguous entity names could prompt "Did you mean the cop or the landlord?"
- The `building()` tool could ask for borough when multiple addresses match
- `network()` could confirm graph depth before expensive traversals

### 2.4 No Output Schemas

FastMCP 2.10+ supports Output Schemas -- typed tool return values that downstream agents can parse programmatically. Currently all tools return untyped `ToolResult` with string content. Structured output would enable:
- Multi-agent pipelines to chain tools without parsing text
- Programmatic extraction of BBLs, entity IDs, and scores
- A2A protocol compatibility for cross-platform agent collaboration

### 2.5 No A2A Protocol Support

Google's Agent2Agent (A2A) protocol (now Linux Foundation, v0.3) enables agent-to-agent collaboration. Common Ground could expose an Agent Card describing its capabilities, allowing external agents to discover and delegate NYC data queries. This would position the platform as a data provider in multi-agent ecosystems.

### 2.6 GraphRAG Not Utilized

The platform has both vector search AND graph traversal but uses them independently. GraphRAG (2026's dominant pattern) combines them:

1. Vector search identifies relevant entities/documents
2. Graph traversal enriches context with relationships
3. LLM synthesizes both into a comprehensive answer

Current entity_xray does a primitive version of this (Lance routing -> SQL queries per source), but a true GraphRAG pipeline would:
- Use DuckPGQ path queries to traverse ownership/political/corporate graphs
- Combine graph-discovered entities with vector-similar entities
- Return a unified narrative with relationship context

### 2.7 No Text-to-SQL Layer

The `query(mode='sql')` tool requires the LLM to write DuckDB SQL directly. A semantic layer or text-to-SQL pipeline would:
- Let users ask questions in natural language
- Use schema embeddings to select relevant tables (the catalog_embeddings table already exists!)
- Generate and validate SQL with guardrails
- Handle common mistakes (wrong schema names, column casing)

The `_fuzzy_match_schema()` and `_fuzzy_match_table()` functions in `tools/query.py:96-140` are a good start -- they already correct fuzzy input. A full semantic layer would extend this to natural language queries.

### 2.8 No Data Quality AI

The `lake_health` mode in `query()` shows row counts and null rates, but there's no AI-powered anomaly detection. Opportunities:
- LLM-powered drift detection on key metrics (e.g., "HPD violations dropped 80% this week -- pipeline issue or real?")
- Automated data quality narratives after each ingestion run
- Schema change detection when Socrata modifies datasets

### 2.9 Monolithic mcp_server.py

At 1,900+ lines, the lifespan function alone is ~1,500 lines. The graph build, embedding pipeline, percentile computation, and tool registration are all in one file. This makes testing, modification, and review difficult. The tool implementations are properly separated into `tools/`, but the startup logic should follow suit.

### 2.10 No Prompt Caching / Context Optimization

The server sends full tool descriptions on every session. MCP 2026 roadmap mentions exploration of context caching and selective tool exposure. With 14 tools at ~3,500 tokens, this is manageable now but would matter if tools grow.

---

## 3. Recommendations (Prioritized)

### P0 -- Security Fix (This Week)
1. **Remove hardcoded API keys** from `embedder.py`. Move to environment variables. Rotate all 14 Gemini keys immediately.

### P1 -- High-Value AI Enhancements (Next 30 Days)

2. **Add MCP Resources for schema discovery**
   - Expose catalog as browsable Resources (schema list, table details, column metadata)
   - Use FastMCP `@mcp.resource()` decorator
   - Reduces tool calls for schema exploration by ~60%

3. **Implement GraphRAG pipeline**
   - Create a new `investigate()` tool or enhance `entity()` to combine:
     - Lance vector search (name similarity)
     - DuckPGQ graph traversal (relationship discovery)
     - LLM-generated narrative synthesis
   - The 8 property graphs + 2.96M entity embeddings are the perfect foundation

4. **Add Output Schemas to all tools**
   - Define Pydantic models for each tool's return type
   - Enable `structured_content` to carry typed data alongside text
   - Makes multi-tool chaining reliable for downstream agents

### P2 -- Competitive Differentiation (60 Days)

5. **Text-to-SQL semantic layer**
   - Use existing `catalog_embeddings` to route natural language to relevant tables
   - Generate SQL with schema context, validate with DuckDB EXPLAIN
   - Fall back to `query(mode='sql')` for complex cases
   - Could be a new `ask()` tool: "ask('who are the worst landlords in the Bronx')"

6. **A2A Agent Card**
   - Publish an Agent Card at `/.well-known/agent.json`
   - Describe capabilities: NYC open data, 14 schemas, entity resolution, graph analysis
   - Enable discovery by external agents (Gemini, GPT, other MCP clients)
   - Minimal implementation: JSON file served from the HTTP layer

7. **Elicitation for ambiguous queries**
   - Use FastMCP 2.10 elicitation when entity search returns multiple high-confidence matches
   - Ask user to confirm before running expensive graph traversals
   - Reduces wasted compute on misrouted queries

### P3 -- Advanced Capabilities (90 Days)

8. **AI-powered data quality monitoring**
   - After each Dagster run, generate a quality narrative comparing current vs historical metrics
   - Detect anomalies: sudden drops in row counts, new null columns, schema changes
   - Surface via a `query(mode='health')` enhancement or dedicated alert tool

9. **Multi-agent investigation workflows**
   - The existing MCP Prompts (`investigate_building`, `follow_the_money`, `compare_neighborhoods`) are proto-workflows
   - Evolve these into A2A-compatible task sequences where specialized agents handle sub-tasks
   - E.g., "Investigate this landlord" spawns parallel: entity agent, graph agent, financial agent

10. **Knowledge graph construction from unstructured data**
    - Use LLM extraction on CFPB complaint narratives (1.2M rows) to build a financial misconduct knowledge graph
    - Extract entities (companies, products, issues) and relationships
    - Enable GraphRAG queries: "Show me all companies connected to mortgage fraud complaints"

---

## 4. Competitive Positioning

### What Common Ground Does Well (Ahead of Market)

- **Super tool consolidation** (63 -> 14): Most MCP servers in 2026 still expose dozens of fine-grained tools, wasting context window. CG's approach matches the recommended "fat tool" pattern.
- **Middleware pipeline**: Response formatting, citations, freshness warnings, and percentile injection are sophisticated. Few data MCP servers do this.
- **DuckPGQ property graphs**: Using graph queries for ownership/corporate/political network analysis is a genuine differentiator. Most data platforms stop at tabular queries.
- **Vector + fuzzy hybrid entity resolution**: The combination of phonetic blocking, Jaro-Winkler, rapidfuzz, and embedding similarity is multi-layered. The Lance routing optimization (skip empty sources) is clever.
- **Pre-computed explorations**: The `suggest()` tool with pre-built data highlights is a good onboarding pattern.

### Where the Market Is Moving

| Trend | CG Status | Gap |
|-------|-----------|-----|
| MCP Resources | Not implemented | Medium -- missing schema browsing |
| A2A Protocol | Not implemented | Large -- no cross-agent discovery |
| GraphRAG | Partial (graph + vector exist separately) | Medium -- not combined |
| Text-to-SQL / Semantic Layer | Manual SQL required | Large -- significant UX barrier |
| Elicitation (human-in-loop) | Not implemented | Small -- FastMCP supports it |
| Output Schemas | Not implemented | Medium -- blocks multi-agent chaining |
| AI Data Quality | Basic health dashboard | Medium -- no anomaly detection |
| Streaming Responses | Supported via Streamable HTTP | None -- already implemented |

---

## 5. Specific Code-Level Suggestions

### 5.1 MCP Resources (New file: `resources.py`)
```python
@mcp.resource("catalog://schemas")
async def list_schemas(ctx: Context) -> str:
    catalog = ctx.lifespan_context["catalog"]
    return json.dumps({s: {"tables": len(t), "rows": sum(v["row_count"] for v in t.values())}
                       for s, t in catalog.items()})

@mcp.resource("catalog://tables/{schema}")
async def list_tables(schema: str, ctx: Context) -> str:
    ...
```

### 5.2 GraphRAG Entity Enhancement (Extend `tools/entity.py`)
```python
# After Lance routing finds matching sources, traverse DuckPGQ graphs
# to discover relationship context before returning results
graph_context = _traverse_entity_graph(pool, matched_names, depth=2)
# Merge vector-similar entities + graph-connected entities
# Return unified narrative with relationship annotations
```

### 5.3 A2A Agent Card (`/.well-known/agent.json`)
```json
{
  "name": "Common Ground",
  "description": "NYC open data lake with 294 tables, 60M+ rows across 14 schemas",
  "url": "https://mcp.common-ground.nyc",
  "version": "1.0",
  "capabilities": {
    "streaming": true,
    "pushNotifications": false
  },
  "skills": [
    {"id": "building-lookup", "name": "Building Profile", "description": "..."},
    {"id": "entity-search", "name": "Entity Cross-Reference", "description": "..."}
  ],
  "authentication": {"schemes": ["bearer"]}
}
```

### 5.4 Break Up mcp_server.py
```
mcp_server.py          -> FastMCP app + tool registration (~100 lines)
lifespan/startup.py    -> DuckDB connection, DuckLake attach, config
lifespan/graph.py      -> All graph table builds (core + extended)
lifespan/embeddings.py -> Embedding pipeline, entity names, descriptions
lifespan/warmup.py     -> Percentiles, explorations, JSON views
```

---

## 6. Research Sources

- [2026 MCP Roadmap](http://blog.modelcontextprotocol.io/posts/2026-mcp-roadmap/)
- [MCP Spec 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25)
- [FastMCP Updates](https://gofastmcp.com/updates) -- v2.10 (elicitation), v2.3 (streamable HTTP)
- [A2A Protocol](https://a2a-protocol.org/latest/) -- Google/Linux Foundation, v0.3
- [A2A Getting Started](https://developers.googleblog.com/en/a2a-a-new-era-of-agent-interoperability/)
- [GraphRAG Complete Guide 2026](https://calmops.com/ai/graphrag-complete-guide-2026/)
- [HybridRAG: Vector + Knowledge Graph](https://memgraph.com/blog/why-hybridrag/)
- [Graph RAG: When Vector Search Isn't Enough](https://dasroot.net/posts/2026/03/graph-rag-vector-search-limitations/)
- [Text-to-SQL with Semantic Search for RAG](https://www.llamaindex.ai/blog/combining-text-to-sql-with-semantic-search-for-retrieval-augmented-generation-c60af30ec3b)
- [Secure Text-to-SQL with Advanced RAG](https://kumarshivam-66534.medium.com/secure-text-to-sql-with-advanced-rag-privacy-preserving-database-querying-without-exposing-3c334b77dd18)
- [AI Observability Tools 2026](https://www.ovaledge.com/blog/ai-observability-tools)
- [MCP Production Growing Pains](https://thenewstack.io/model-context-protocol-roadmap-2026/)
