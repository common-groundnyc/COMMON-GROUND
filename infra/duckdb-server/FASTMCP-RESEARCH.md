# FastMCP Server Research & Design Reference

> Compiled 2026-03-10. Use this to build the Common Ground FastMCP server.

---

## Part 1: FastMCP 3.x Feature Reference

### 1.1 ToolResult — Full Output Control

Import: `from fastmcp.tools.tool import ToolResult`

| Parameter | Type | Purpose |
|---|---|---|
| `content` | `list[ContentBlock] \| str \| None` | What the LLM sees |
| `structured_content` | `dict \| None` | Machine-readable data (NOT sent to LLM) |
| `meta` | `dict \| None` | Arbitrary metadata (execution time, row counts) |
| `is_error` | `bool` | Mark response as error |

Content types: `TextContent`, `ImageContent`, `AudioContent`, `EmbeddedResource`

```python
from fastmcp.tools.tool import ToolResult
from fastmcp.types import TextContent

# String shorthand
ToolResult(content="Found 3 users", structured_content={"users": [...]}, meta={"ms": 42})

# Multiple content blocks
ToolResult(content=[
    TextContent(type="text", text="Summary here"),
    ImageContent(type="image", data="base64...", mimeType="image/png"),
])

# Error
ToolResult(content="x must be positive", is_error=True)
```

**StarRocks pattern** — hide large data from LLM:
```python
@mcp.tool()
def query_dump(query: str) -> ToolResult:
    result = db.collect_perf_analysis(query)
    return ToolResult(
        content=[TextContent(type='text', text="Analysis collected (for client only)")],
        structured_content=result,  # Full data, LLM doesn't see this
    )
```

**Foreman pattern** — reusable helper:
```python
def build_tool_result(structured: dict) -> ToolResult:
    content = derive_markdown(structured)
    return ToolResult(
        content=[TextContent(type="text", text=content)],
        structured_content=structured,
    )
```

**ToolError** for exceptions:
```python
from fastmcp.exceptions import ToolError

@mcp.tool
def risky(data: str) -> str:
    if not data:
        raise ToolError("Data is required")
    return process(data)
```

---

### 1.2 Lifespan — Shared State

```python
from fastmcp import FastMCP, Context
from fastmcp.server.lifespan import lifespan

@lifespan
async def app_lifespan(server):
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("ATTACH 'ducklake:postgres:...' AS lake")
    try:
        yield {"db": conn}
    finally:
        conn.close()

mcp = FastMCP("Common Ground", lifespan=app_lifespan)

@mcp.tool
def query(sql: str, ctx: Context) -> str:
    db = ctx.lifespan_context["db"]
    return db.execute(sql).fetchall()
```

**Dataclass pattern** (cleaner):
```python
@dataclass
class AppContext:
    db: duckdb.DuckDBPyConnection
    model: SentenceTransformer  # For embeddings

@lifespan
async def app_lifespan(server) -> AsyncIterator[AppContext]:
    conn = duckdb.connect()
    model = SentenceTransformer("minishlab/potion-base-32M")
    try:
        yield AppContext(db=conn, model=model)
    finally:
        conn.close()
```

---

### 1.3 Server Composition

**Mounting** (live-linked):
```python
main = FastMCP("Main")
main.mount(weather_server)  # All tools available through main
main.mount(analytics_server, prefix="analytics")  # Namespaced
```

**Proxy** (remote servers):
```python
from fastmcp import create_proxy
main.mount(create_proxy("http://remote:8000/mcp"), prefix="remote")
```

**Importing** (static copy):
```python
main.import_server(weather, prefix="wx")
```

**OpenAPI → MCP** (auto-wrap REST APIs):
```python
mcp = FastMCP.from_openapi(
    openapi_url="https://api.example.com/openapi.json",
    client=httpx.AsyncClient(base_url="https://api.example.com")
)
```

---

### 1.4 Authentication

**Static token verifier**:
```python
from fastmcp.server.auth import StaticTokenVerifier
auth = StaticTokenVerifier(tokens={
    "secret-key": {"client_id": "my-client", "scopes": ["read", "write"]}
})
mcp = FastMCP("Secured", auth=auth)
```

**OIDC proxy** (full OAuth):
```python
from fastmcp.server.auth.oidc_proxy import OIDCProxy
auth = OIDCProxy(
    config_url="https://provider.com/.well-known/openid-configuration",
    client_id="...", client_secret="...", base_url="https://my-server.com"
)
```

**Per-tool authorization**:
```python
from fastmcp.server.auth import require_scopes

@mcp.tool(auth=require_scopes("admin"))
def admin_tool() -> str: ...
```

**Token access in tools**:
```python
@mcp.tool
async def user_info(ctx: Context) -> str:
    token = await ctx.get_access_token()
    return f"User: {token.claims.get('sub')}"
```

---

### 1.5 Tool Annotations

```python
from mcp.types import ToolAnnotations

READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)

@mcp.tool(annotations=READONLY, tags={"domain": "housing"})
def get_violations(bbl: str) -> str: ...
```

---

### 1.6 Tool Transforms

```python
from fastmcp.tools import Tool

# Rename + hide args
transformed = Tool.from_tool(
    original,
    name="search_buildings",
    argument_map={"q": "query"},
    hidden_args={"api_key": "hardcoded-value"}
)

# Custom wrapper with forward()
from fastmcp.tools.tool_transform import forward

async def summarized(**kwargs) -> ToolResult:
    result = await forward(**kwargs)
    return ToolResult(content="Summary", structured_content=result)

Tool.from_tool(original, fn=summarized)
```

**Namespace/Visibility transforms**:
```python
from fastmcp.server.transforms import Namespace, Visibility, ResourcesAsTools

mcp = FastMCP("Main", transforms=[
    Namespace(prefix="cg"),
    Visibility(include_tags={"public"}),
])
```

---

### 1.7 Context Object — Complete API

```python
@mcp.tool
async def demo(query: str, ctx: Context) -> str:
    # Logging
    ctx.info("Processing"); ctx.debug("..."); ctx.warning("..."); ctx.error("...")

    # Progress
    await ctx.report_progress(50, 100, "Halfway")

    # Resources
    data = await ctx.read_resource("resource://data")

    # Session state (async in v3)
    await ctx.set_state("key", "value")
    val = await ctx.get_state("key")

    # Dynamic components
    await ctx.enable_components(["advanced_tool"])
    await ctx.disable_components(["deprecated_tool"])

    # Sampling (call LLM back through MCP)
    response = await ctx.sample("Summarize this")

    # Elicitation (ask user)
    answer = await ctx.elicit(prompt="Format?", options=["json", "csv"])

    # Request metadata
    rid = ctx.request_id
    cid = ctx.client_id

    # Lifespan context
    db = ctx.lifespan_context["db"]
```

---

### 1.8 Transport Options

| Transport | Use Case | Config |
|---|---|---|
| `stdio` | Local/Claude Desktop | `mcp.run()` |
| `streamable-http` | Production HTTP | `mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)` |
| `sse` | Legacy SSE | `mcp.run(transport="sse")` |

**Stateless HTTP** (for our use case — no session persistence needed):
```python
mcp.run(transport="streamable-http", host="0.0.0.0", port=4213,
        stateless_http=True, json_response=True)
```

---

### 1.9 Session State

```python
@mcp.tool
async def stateful(ctx: Context) -> str:
    count = (await ctx.get_state("visits") or 0) + 1
    await ctx.set_state("visits", count)
    return f"Visit #{count}"
```

Storage backends: `MemoryStore` (default), `DiskStore`, `RedisStore`.

---

### 1.10 Pagination

```python
server = FastMCP("Registry", list_page_size=50)
# Affects tools/list, resources/list, prompts/list
# Clients get cursor tokens for next page
```

---

### 1.11 Middleware

```python
from fastmcp.server.middleware import Middleware, MiddlewareContext

class TimingMiddleware(Middleware):
    async def on_call_tool(self, context: MiddlewareContext, call_next):
        start = time.time()
        result = await call_next()
        ctx.info(f"Tool took {time.time()-start:.2f}s")
        return result

mcp = FastMCP("Timed", middleware=[TimingMiddleware()])
```

Hooks: `on_call_tool`, `on_list_tools`, `on_read_resource`, `on_list_resources`, `on_get_prompt`, `on_list_prompts`.

---

### 1.12 OpenTelemetry (Native)

```bash
pip install opentelemetry-distro opentelemetry-exporter-otlp
opentelemetry-instrument --service_name common-ground fastmcp run mcp_server.py
```

Auto-traces: tool executions, resource ops, auth flows, provider chains.

---

### 1.13 CLI Tools

```bash
fastmcp list server.py                    # List tools
fastmcp list server.py --input-schema     # With JSON schemas
fastmcp call server.py tool '{"arg":"v"}' # Call a tool
fastmcp discover                          # Find configured servers
fastmcp generate-cli server.py            # Generate typed CLI
fastmcp install server.py                 # Install in Claude Desktop
fastmcp run server.py --transport streamable-http --port 8000
```

---

### 1.14 Resources & Prompts

```python
@mcp.resource("data://schemas")
def list_schemas() -> list[str]:
    return ["housing", "public_safety", "health"]

@mcp.resource("data://table/{schema}/{table}")
def get_table_info(schema: str, table: str) -> dict: ...

@mcp.prompt
def analysis_prompt(dataset: str) -> str:
    return f"Analyze the {dataset} dataset..."
```

Convert to tools: `transforms=[ResourcesAsTools(), PromptsAsTools()]`

---

### 1.15 FileSystemProvider (Auto-Discovery)

```python
from fastmcp.server.providers import FileSystemProvider

mcp = FastMCP("Server", providers=[
    FileSystemProvider(Path("./tools"), dev_mode=True)  # Hot reload
])
```

Tool files use standalone decorators:
```python
# tools/housing.py
from fastmcp.decorators import tool

@tool
def building_profile(bbl: str) -> str: ...
```

---

### 1.16 Output Schema

```python
from dataclasses import dataclass

@dataclass
class BuildingProfile:
    bbl: str
    address: str
    units: int
    violations: int

@mcp.tool
def get_building(bbl: str) -> BuildingProfile:
    # FastMCP auto-generates output schema from return type
    return BuildingProfile(...)
```

---

### 1.17 Prefab Apps (UI in tool responses)

```python
from fastmcp.server.apps import AppConfig

@mcp.tool(app=AppConfig(resource_uri="ui://chart/view.html"))
def show_chart(data: list[float]) -> str:
    return json.dumps({"values": data})

@mcp.resource("ui://chart/view.html")
def chart_view() -> str:
    return "<html>...</html>"
```

---

### 1.18 CodeMode (Experimental — v3.1.0)

```python
from fastmcp.experimental.transforms.code_mode import CodeMode
mcp = FastMCP("Server", transforms=[CodeMode()])
```

LLM gets meta-tools: searches for tools on demand (BM25), inspects schemas, writes Python that chains `call_tool()` in a sandbox. Intermediate results never touch context.

---

### 1.19 Background Tasks (Docket)

```python
@mcp.tool(task=True)
async def long_job(data: str, ctx: Context) -> str:
    for i in range(100):
        await ctx.report_progress(i, 100)
        await asyncio.sleep(0.1)
    return f"Done: {data}"
```

Client gets task ID immediately, polls for status/result.

---

### 1.20 Dynamic Components (Per-Session)

```python
@mcp.tool
async def activate_premium(ctx: Context) -> str:
    await ctx.enable_components(["bulk_export", "advanced_analytics"])
    return "Premium tools activated"
```

---

## Part 2: Real-World Repos & Patterns

### 2.1 Top Repos

| Repo | Stars | Pattern |
|------|-------|---------|
| **StarRocks/mcp-server-starrocks** | 151 | Best ToolResult usage with DB queries + Plotly |
| **call518/MCP-PostgreSQL-Ops** | 139 | 30+ tools, StaticTokenVerifier auth |
| **dagster-io/erk** | 74 | Dynamic tool registration from CLI commands |
| **raw-labs/mxcp** | 59 | DuckDB, auth, hot reload, telemetry, audit |
| **Dicklesworthstone/mcp_agent_mail** | 1,786 | Agent coordination, SQLite |
| **wshobson/maverick-mcp** | 408 | Stock analysis, focused toolset |
| **sprine/ontario-data-mcp** | - | ToolAnnotations, httpx lifespan, clean modular design |
| **reallocf/data-flow-control** | - | DuckDB + VSS + sentence-transformers lifespan |
| **adilkhash/kase-mcp-server** | - | Full OAuth/OIDC with MCPAuth + DuckDB |
| **simonChoi034/image-gen-mcp** | - | ToolResult with ImageContent, ToolError |

### 2.2 Database Connection Patterns

**Best: Lifespan singleton (our approach)**
```python
@lifespan
async def app_lifespan(server):
    conn = duckdb.connect()
    # Load extensions, attach catalogs once
    try:
        yield {"db": conn}
    finally:
        conn.close()
```

**StarRocks: Module-level singleton with health checking**
```python
db_client = get_db_client()
_health_checker = initialize_health_checker(db_client)
```

**PostgreSQL: asyncpg pool**
```python
engine = create_async_engine("postgresql+asyncpg://...",
    pool_size=20, max_overflow=10, pool_pre_ping=True)
```

### 2.3 Clever Patterns to Steal

**1. ToolAnnotations constants:**
```python
READONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True)
```

**2. Description suffix for context:**
```python
@mcp.tool(description=f"Execute SQL. Default database: `lake`. Schemas: {schema_list}")
```

**3. Global table overview cache:**
Cache expensive DESCRIBE + sample queries at startup.

**4. Dynamic tool description with data context:**
```python
description_suffix = f". Available schemas: {', '.join(schemas)}"
```

**5. Dagster/erk: Custom Tool subclass for dynamic registration:**
```python
class MacroTool(Tool):
    sql_template: str
    async def run(self, arguments: dict) -> ToolResult:
        result = db.execute(self.sql_template, arguments)
        return self.format_result(result)
```

**6. MCP sampling to call LLM back:**
```python
response = await ctx.session.create_message(...)
# Use the connected LLM for summarization within a tool
```

---

## Part 3: Data Analytics MCP Best Practices

### 3.1 Tool Count

- **5-8 tools per server** is ideal
- **15+ means split** into separate servers
- **30 is the hard ceiling** — LLM tool selection degrades
- MotherDuck: 1 tool (raw SQL). Cube.dev: semantic tools. Best: hybrid.

### 3.2 Handling Large Results

**The #1 engineering challenge for data MCP servers.**

| Strategy | Details |
|---|---|
| Default cap | 25-50 rows (MotherDuck uses 1024, too high) |
| Always include | `total_count`, `has_more`, `page_cursor` |
| Format | **CSV/TSV for tables, NOT JSON** — JSON repeats column names per row, massive token waste |
| Pre-aggregate | Return counts, sums, distributions when possible |
| Token budget | Slice data to fit a predetermined token ceiling |
| Narrow time windows | Default to last hour/day, expand on demand |

**Axiom's key insight:** "Ditch JSON for tabular data. LLMs understand tables just fine without JSON ceremony."

**GitHub MCP cautionary tale:** `list_commits()` returning 30 results = 64K+ tokens. Context blown.

### 3.3 Response Format

```
# GOOD: Compact text table (what we do)
bbl         | address          | units | violations
1234567890  | 123 Main St      | 42    | 18
9876543210  | 456 Oak Ave      | 8     | 3
(2 of 847 rows, showing first page)

# BAD: JSON (token-expensive)
[{"bbl":"1234567890","address":"123 Main St","units":42,"violations":18}, ...]
```

### 3.4 Raw SQL vs Summaries

| Question type | Return format |
|---|---|
| Exploratory ("what tables?") | Structured metadata |
| Specific ("top 10 by X") | Actual rows, capped, compact format |
| Aggregate ("trend in X?") | Pre-aggregated stats |
| Large scans ("all 2024 orders") | Summary stats (count, min, max, avg, distribution) |

### 3.5 Auth & Security

- **OAuth 2.1 is now mandatory** for HTTP MCP transport (March 2025 spec)
- Read-only by default, explicit flag for writes
- **43% of analyzed MCP servers had SQL injection** — always parameterize
- Short-lived tokens, per-request validation
- Rate limit per agent, circuit break expensive queries

### 3.6 Semantic Layer as MCP

The Cube.dev pattern:
1. Define metrics/dimensions in semantic layer
2. MCP tools: `list_metrics()`, `query_metric(name, dims, filters)`
3. LLM never writes SQL — queries by metric name
4. Eliminates SQL hallucination

**For us:** Our DuckDB macros (building_profile, complaints_by_zip, owner_violations) + the named FastMCP tools ARE the semantic layer. No need for Cube.dev.

### 3.7 Spatial Queries

- Expose as high-level tools (`find_in_region`, `spatial_join`) not raw `ST_Within()`
- DuckDB spatial extension built on GEOS, works with GeoParquet
- We have 92 spatial views with geometry columns

### 3.8 Embeddings + MCP

**Pattern A — Vector search as MCP tool:**
```python
@mcp.tool
def semantic_search(query: str, table: str, filters: str = "", limit: int = 10) -> str:
    embedding = model.encode(query)
    # Filter first, then cosine similarity on filtered set
```

**Pattern B — Complementary to FTS:**
- FTS (BM25) for keyword matching — fast, no extra storage
- Embeddings for meaning matching — needs FLOAT[512] column
- Expose both as separate tools, let LLM choose

**Our approach:**
- Tier 1: FTS on all text-heavy tables (free, works now)
- Tier 2: Embeddings on CFPB (782K rows), 311 (~3M), restaurants (~500K)
- Query pattern: filter by ZIP/date/category first, THEN cosine similarity
- Model2Vec (potion-base-32M) already in container

### 3.9 Common Mistakes

1. **Data dumping** — unbounded results blow context
2. **Too many tools** — over 15 = confusion
3. **JSON for tables** — wasteful tokens
4. **Silent failures** — empty responses cause LLM to give up
5. **Vague descriptions** — tool descriptions ARE the LLM's instructions
6. **String concat SQL** — injection risk, always parameterize
7. **No result caps** — always cap + report total
8. **No read-only default** — always default safe

---

## Part 4: Our Architecture Plan

### 4.1 Tool Design (8 tools)

**Domain tools (ToolResult-powered):**

| # | Tool | Input | LLM content | structured_content |
|---|------|-------|-------------|-------------------|
| 1 | `building_profile` | bbl | One-line building summary | Full record dict |
| 2 | `complaints_by_zip` | zip, date_range? | "ZIP 10001: 847 complaints, top: noise (312)" | Full breakdown |
| 3 | `owner_violations` | bbl | "23 open violations across 3 properties" | Violation list |
| 4 | `neighborhood_snapshot` | zip | Multi-line housing/safety/health summary | Cross-dataset aggregate |
| 5 | `text_search` | table, query, limit? | Top 5 matches with scores | Full results |
| 6 | `data_catalog` | keyword? | Matching tables with row counts | Schema details |

**Power tools:**

| # | Tool | Purpose |
|---|------|---------|
| 7 | `sql_query` | Raw SQL — first 20 rows as text table, meta has total |
| 8 | `describe_table` | Schema + column stats (min/max/nulls/sample values) |

### 4.2 Response Helpers

```python
MAX_LLM_ROWS = 20
MAX_STRUCTURED_ROWS = 500

def format_text_table(cols, rows, max_rows=MAX_LLM_ROWS):
    """Format as aligned text table for LLM consumption."""
    header = " | ".join(str(c) for c in cols)
    separator = "-" * len(header)
    lines = [header, separator]
    for row in rows[:max_rows]:
        lines.append(" | ".join(str(v) for v in row))
    total = len(rows)
    if total > max_rows:
        lines.append(f"({max_rows} of {total} rows shown)")
    return "\n".join(lines)

def make_result(summary, cols, rows, meta_extra=None):
    """Build ToolResult with text for LLM + structured for clients."""
    return ToolResult(
        content=summary + "\n\n" + format_text_table(cols, rows),
        structured_content=[dict(zip(cols, r)) for r in rows[:MAX_STRUCTURED_ROWS]],
        meta={"total_rows": len(rows), **(meta_extra or {})}
    )
```

### 4.3 Instructions (System Prompt)

```python
INSTRUCTIONS = """NYC open data lake — 50+ datasets, 12 schemas, 60M+ rows.

USE THESE TOOLS FIRST:
- building_profile(bbl) → full building dossier by Borough-Block-Lot
- complaints_by_zip(zip) → complaint analytics by ZIP code
- owner_violations(bbl) → violation history for a property
- neighborhood_snapshot(zip) → cross-dataset area summary
- text_search(table, query) → BM25 keyword search on text columns
- data_catalog(keyword) → discover tables by topic

POWER USER:
- sql_query(sql) → raw SQL when named tools don't cover it
- describe_table(schema, table) → schema + stats

SCHEMAS: housing, public_safety, health, social_services, financial,
         environment, recreation, education, business, transportation,
         city_government, census

SPATIAL: 92 views with geometry columns. Use ST_ functions for geo queries.
BBL FORMAT: 10 digits — borough(1) + block(5) + lot(4). Example: 1000670001
ZIP FORMAT: 5 digits. Manhattan starts with 100xx, Brooklyn 112xx, etc.
"""
```

### 4.4 Dockerfile

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir \
    duckdb==1.5.0 \
    "fastmcp[http]>=3.1.0" \
    model2vec numpy
WORKDIR /app
COPY mcp_server.py .
COPY server.py .
COPY start.sh .
RUN chmod +x start.sh
EXPOSE 9999 4213
CMD ["./start.sh"]
```

### 4.5 start.sh

```bash
#!/bin/bash
set -e
python mcp_server.py &
MCP_PID=$!
python server.py &
API_PID=$!
echo "MCP=$MCP_PID API=$API_PID"
wait -n $API_PID $MCP_PID
kill $API_PID $MCP_PID 2>/dev/null
wait
```

### 4.6 .mcp.json entry

```json
"duckdb": {
    "type": "http",
    "url": "http://178.156.228.119:4213/mcp"
}
```

---

## Part 5: Future Considerations

### 5.1 Server Composition (Phase 3?)

Split into domain servers, compose via mount:
```python
main = FastMCP("Common Ground")
main.mount(housing_server, prefix="housing")
main.mount(safety_server, prefix="safety")
main.mount(health_server, prefix="health")
```

Only worth doing if tool count exceeds 15.

### 5.2 FileSystemProvider (Phase 3?)

Auto-discover tools from `tools/` directory with hot reload:
```python
mcp = FastMCP("CG", providers=[
    FileSystemProvider(Path("./tools"), dev_mode=True)
])
```

Each domain gets its own file: `tools/housing.py`, `tools/safety.py`, etc.

### 5.3 OpenTelemetry (Phase 3?)

Free observability — just add the OTEL packages and run with instrumentation.

### 5.4 Auth (Phase 3?)

StaticTokenVerifier for now. OAuth 2.1 when we expose publicly.

### 5.5 Prefab Apps (Phase 4?)

Return interactive charts/maps in tool responses. Requires MCP client support.

### 5.6 CodeMode (Watch)

Experimental but promising — could solve the "too many tools" problem if we grow past 15.

### 5.7 DuckPGQ (Watch)

Graph queries when community extension builds land for DuckDB 1.5.0.
