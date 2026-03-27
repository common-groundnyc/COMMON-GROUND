# Post-Deploy Verification Checklist

Run these checks after deploying the hardened MCP server.

## 1. Container Startup
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose logs duckdb-server --tail 50"
```
**Verify:** "DuckLake catalog attached" appears. No Python tracebacks.

## 2. Website Data Health Page
Open: https://common-ground.nyc/data
**Verify:** Table loads with row counts. No CORS error in browser console.

## 3. MCP Tool Smoke Test (from Claude Code)
Call: `building_profile(bbl="1000670001")`
**Verify:** Returns building data for 67 Wall St.

Call: `entity_xray(name="BARTON PERLBINDER")`
**Verify:** Returns cross-referenced entity results.

Call: `due_diligence(name="JOHN SMITH")`
**Verify:** Returns background check results without SQL errors.

## 4. SQL Blocklist Test
Call: `sql_query(sql="CALL lake.set_option('enable_compaction', 'true')")`
**Verify:** Returns error: "Only SELECT/WITH/EXPLAIN..." — NOT a success.

Call: `sql_query(sql="LOAD httpserver")`
**Verify:** Returns error.

## 5. lake_health Test
Call: `lake_health(schema="housing")`
**Verify:** Returns table with row counts. No SQL error.

## 6. Spatial Tool Test
Call: `neighborhood_portrait(zipcode="10003")`
**Verify:** Returns neighborhood data for East Village.

## 7. Semantic Search Test
Call: `semantic_search(query="pest problems", source="restaurant")`
**Verify:** Returns results (if embeddings are loaded).

## 8. PostHog Event Check
Open PostHog → Events → filter by "mcp_tool_called"
**Verify:** Events arrive. `arg_*` properties show `<str:Nchars>` format, NOT raw values.

## 9. Graph Build Check
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose logs duckdb-server | grep 'Property graph built'"
```
**Verify:** Shows owner/building/violation counts.

## 10. Embedding Pipeline Check
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose logs duckdb-server | grep 'Lance embedding'"
```
**Verify:** Shows "Lance embedding pipeline complete".

---

## Deferred (NOT fixed in this pass)

| Item | Why Deferred |
|------|-------------|
| Split mcp_server.py into modules | Too risky without integration tests. Separate phase. |
| Rate limiting | Website says "no rate limits, free forever". Needs copy change first. |
| Shared catalog connection pool | Current per-request approach is wasteful but safe. Needs concurrency design. |
| External S3 URLs in rent stabilization | Tradeoff: stale data vs startup reliability. Not a security issue. |
| Test coverage for tool functions | Needed but large effort. Separate phase. |
| DuckPGQ MATCH pattern parameterization | PGQ MATCH syntax doesn't support ? params. Inputs are regex-validated. |
| Lance vector search parameterization | Lance SQL may not support ? params. Inputs validated against frozensets. |
