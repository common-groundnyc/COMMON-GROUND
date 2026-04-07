# Server Quality Fixes — Security, Healthcheck, Error Messages, Memory

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 2 SQL injection vulnerabilities, add a working Docker healthcheck, improve error messages for LLM self-correction, and plug a memory leak — all in the existing MCP server.

**Architecture:** All fixes are surgical edits to `mcp_server.py` and `Dockerfile`. No new files, no architectural changes. Each task is independent and deployable separately. Research with Exa before each task.

**Tech Stack:** FastMCP 3.1.1, DuckDB, Python 3.12, Docker

---

## Task 1: Fix SQL injection in `shell_detector` (CRITICAL)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:9957-9978`

The `borough` parameter is string-interpolated into SQL at line 9975:
```python
"WHERE UPPER(corp.county) LIKE UPPER('%" + borough.strip().replace("'", "") + "%')"
```
The `replace("'", "")` sanitization is insufficient. Fix: use a parameterized query.

- [ ] **Step 1: Research — DuckDB parameterized LIKE queries**

Search with Exa:
1. `duckdb parameterized query LIKE wildcard "%" 2026` — can you use `?` params with LIKE and `%`?
2. `duckdb "execute" parameter binding "LIKE" example` — exact syntax

DuckDB supports `LIKE '%' || ? || '%'` with bind parameters.

- [ ] **Step 2: Fix the SQL injection**

Replace lines 9958-9978. The key change is at line 9975 — replace the f-string interpolation with a parameterized approach:

```python
    borough_clause = ""
    borough_params = []
    if borough.strip():
        borough_clause = "WHERE UPPER(corp.county) LIKE UPPER('%' || ? || '%')"
        borough_params = [borough.strip()]

    try:
        cols, rows = _execute(db, f"""
            WITH wcc AS (
                SELECT * FROM weakly_connected_component(nyc_corporate_web, Corp, SharedOfficer)
            ),
            clusters AS (
                SELECT w.componentid,
                       COUNT(*) AS cluster_size
                FROM wcc w
                GROUP BY w.componentid
                HAVING COUNT(*) >= {min_corps}
            )
            SELECT c.componentid, c.cluster_size,
                   corp.dos_id, corp.current_entity_name, corp.entity_type,
                   corp.chairman_name, corp.county
            FROM clusters c
            JOIN wcc w ON c.componentid = w.componentid
            JOIN main.graph_corps corp ON w.dos_id = corp.dos_id
            {borough_clause}
            ORDER BY c.cluster_size DESC, c.componentid, corp.current_entity_name
            LIMIT 500
        """, borough_params)
```

Note: `min_corps` is safe — it's an integer validated by Pydantic (`ge=2, le=50`), so f-string interpolation is OK for it. Only the string `borough` parameter needed parameterization.

- [ ] **Step 3: Verify the fix**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
grep -n "borough.strip().replace" mcp_server.py
```

Expected: No matches (the old pattern is gone).

```bash
grep -n "LIKE UPPER.*borough" mcp_server.py
```

Expected: Shows the new parameterized pattern.

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "security: fix SQL injection in shell_detector borough parameter

Replace string interpolation with parameterized LIKE query.
The old replace(\"'\", \"\") sanitization was insufficient."
```

---

## Task 2: Fix SQL injection in `gentrification_tracker` (CRITICAL)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:3497-3510`

Query results from the database are interpolated into a VALUES clause via f-string at lines 3505-3509. While the source data comes from a prior parameterized query, this is a second-order injection risk.

- [ ] **Step 1: Research — DuckDB CREATE TABLE AS with bind parameters**

Search with Exa:
1. `duckdb "CREATE TEMP TABLE" "AS SELECT" from values parameterized` — can you parameterize VALUES?
2. `duckdb python "fetchall" "CREATE TABLE AS" alternative to VALUES` — better patterns

The cleanest fix: use `CREATE TEMP TABLE AS SELECT * FROM query_result` where query_result is a DuckDB relation, or use `INSERT INTO ... SELECT` with the original query.

- [ ] **Step 2: Replace f-string VALUES with a direct CREATE TABLE from query**

The `rows` variable comes from a query that already has the data. Instead of dumping it into a VALUES f-string, use DuckDB's Python relational API or re-run the source query into a temp table:

```python
    try:
        with _db_lock:
            tbl = "_gent_tmp"
            db.execute(f"DROP TABLE IF EXISTS {tbl}")
            # Create temp table directly from the source query instead of
            # string-interpolating VALUES (prevents second-order SQL injection)
            db.execute(f"""
                CREATE TEMP TABLE {tbl} AS
                SELECT
                    zip,
                    q,
                    new_restaurants,
                    hpd_complaints,
                    noise_calls,
                    dob_violations,
                    construction_complaints
                FROM ({GENTRIFICATION_TRACKER_SQL})
                WHERE zip IN ({','.join(['?'] * len(zip_codes))})
            """, zip_codes)
```

This re-runs the source query into the temp table directly, eliminating the VALUES f-string entirely. The `zip_codes` are already validated (5-digit strings checked at the top of the function).

If the source query is complex or slow to re-run, an alternative is to use DuckDB's `executemany` with proper parameter binding:

```python
            db.execute(f"CREATE TEMP TABLE {tbl} (zip VARCHAR, q TIMESTAMP, new_restaurants INTEGER, hpd_complaints INTEGER, noise_calls INTEGER, dob_violations INTEGER, construction_complaints INTEGER)")
            db.executemany(f"INSERT INTO {tbl} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
```

Use whichever approach is simpler. The `executemany` approach is more straightforward.

- [ ] **Step 3: Find the source SQL constant name**

```bash
grep -n "GENTRIFICATION_TRACKER_SQL\|_GENT_SQL\|gentrification.*SQL" ~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py | head -5
```

Use the actual constant name in the fix.

- [ ] **Step 4: Verify no f-string VALUES remain**

```bash
grep -n "f\".*VALUES.*f\"" ~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py
grep -n "CAST.*AS TIMESTAMP.*{r\[" ~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py
```

Expected: No matches.

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "security: fix second-order SQL injection in gentrification_tracker

Replace f-string VALUES construction with executemany parameter binding.
Eliminates risk from stored data containing malicious SQL."
```

---

## Task 3: Add `/health` endpoint and fix Docker healthcheck (CRITICAL)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (add health route)
- Modify: `infra/duckdb-server/Dockerfile` (fix healthcheck command)
- Modify: `infra/duckdb-server/start.sh` (may need health route registration)

- [ ] **Step 1: Research — FastMCP custom routes / Starlette health endpoint**

Search with Exa:
1. `fastmcp custom route starlette "/health" endpoint 2026` — how to add custom HTTP routes alongside FastMCP
2. `fastmcp "custom_starlette_routes" OR "additional_routes" health` — FastMCP config for extra routes

FastMCP 3.1.x provides `mcp.custom_route()` for adding HTTP routes alongside the MCP server.

Note: the existing `_well_known_routes` OAuth stubs are dead code — defined but never registered. Don't follow that pattern.

- [ ] **Step 2: Add a `/health` endpoint using `mcp.custom_route()`**

Add near the bottom of the file, before the `if __name__ == "__main__":` block:

```python
from starlette.responses import JSONResponse
from starlette.requests import Request

@mcp.custom_route("/health", methods=["GET"])
async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})
```

Note: `JSONResponse` and `Request` may already be imported (check the OAuth stubs section). If so, don't duplicate the imports.

- [ ] **Step 3: Fix the Dockerfile healthcheck**

Read the Dockerfile. Replace the curl-based healthcheck with a Python-based one:

```dockerfile
# OLD:
# test: ["CMD-SHELL", "curl -f http://localhost:4213/health || exit 1"]

# NEW:
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=15s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:4213/health')" || exit 1
```

Wait — the healthcheck is in the `docker-compose.yml`, not the Dockerfile. Check both:

```bash
grep -n "healthcheck" ~/Desktop/dagster-pipeline/infra/duckdb-server/Dockerfile
grep -A5 "healthcheck" ~/Desktop/dagster-pipeline/infra/duckdb-server/../hetzner-docker-compose.yml
```

Fix whichever file has the healthcheck. Change from `curl` to `python`:

```yaml
    healthcheck:
      test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:4213/health')\""]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 15s
```

- [ ] **Step 4: Test locally**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
grep -n "custom_route.*health" mcp_server.py
```

Expected: Shows the `@mcp.custom_route("/health")` decorator.

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/Dockerfile
git add infra/hetzner-docker-compose.yml  # if compose was changed
git commit -m "fix: add /health endpoint and fix Docker healthcheck

Adds a JSON health endpoint at /health.
Replaces curl-based healthcheck with Python urllib (curl not in container)."
```

---

## Task 4: Improve error messages for LLM self-correction (HIGH)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:544-545`

The `_execute()` function catches `duckdb.Error` and re-raises as `ToolError(f"SQL error: {e}")`. This raw error is unhelpful for LLMs — they don't know what to try next.

- [ ] **Step 1: Research — LLM-friendly error message patterns**

Search with Exa:
1. `"mcp server" "error message" "self-correction" LLM 2026` — best practices
2. `"ToolError" recovery suggestion "try" "instead" mcp` — examples

- [ ] **Step 2: Improve `_execute()` error message**

Replace line 544-545:

```python
        except duckdb.Error as e:
            raise ToolError(f"SQL error: {e}")
```

With:

```python
        except duckdb.Error as e:
            err = str(e)
            hint = ""
            if "does not exist" in err.lower():
                hint = " Use data_catalog(keyword) to find table names, or list_tables(schema) to browse a schema."
            elif "not found" in err.lower():
                hint = " Use describe_table(schema, table) to check column names."
            elif "permission" in err.lower() or "read-only" in err.lower():
                hint = " Only SELECT queries are allowed. Use sql_query() for reads."
            raise ToolError(f"SQL error: {e}{hint}")
```

- [ ] **Step 3: Improve `sql_admin` error message**

Find line 2553 (`raise ToolError(f"DDL error: {e}")`). Add a hint:

```python
            raise ToolError(f"DDL error: {e}. Only CREATE OR REPLACE VIEW is allowed.")
```

- [ ] **Step 4: Improve `shell_detector` error message**

Find line 9980 (`raise ToolError(f"Shell detection failed: {e}")`). Add:

```python
            raise ToolError(f"Shell detection failed: {e}. The DuckPGQ graph may not be ready. Try corporate_web(name) instead.")
```

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: add LLM recovery hints to error messages

SQL errors now suggest data_catalog(), list_tables(), or describe_table()
based on error type. Helps LLMs self-correct without human intervention."
```

---

## Task 5: Fix `_session_clients` memory leak (MEDIUM)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:9558-9583`

The `_session_clients` dict accumulates session data forever. With stateless HTTP, sessions are short-lived but never pruned.

- [ ] **Step 1: Replace dict with LRU cache**

Find `_session_clients: dict[str, dict] = {}` (line 9558). Replace with:

```python
_session_clients: dict[str, dict] = {}
_SESSION_MAX = 1000  # prevents unbounded memory growth
```

Then in the `on_initialize` method where sessions are stored (line 9577), add pruning:

```python
            _session_clients[session_id] = {
                "client_name": getattr(client_info, "name", "unknown"),
                "client_version": getattr(client_info, "version", "unknown"),
                "client_ip": client_ip,
            }
            # Prune old sessions if over limit
            if len(_session_clients) > _SESSION_MAX:
                # Remove oldest entries (first inserted)
                excess = len(_session_clients) - _SESSION_MAX
                for key in list(_session_clients.keys())[:excess]:
                    del _session_clients[key]
```

- [ ] **Step 2: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "fix: prune _session_clients dict to prevent memory leak

Caps at 1000 sessions, removes oldest when exceeded."
```

---

## Task 6: Deploy and validate all fixes

- [ ] **Step 1: Deploy**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' --exclude '*.md' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Test healthcheck**

```bash
# Wait for startup
sleep 30

# Test health endpoint
curl -s https://mcp.common-ground.nyc/health
# Expected: {"status":"ok"}

# Check Docker reports healthy
ssh hetzner "docker ps --filter name=duckdb-server --format '{{.Names}} {{.Status}}'"
# Expected: "Up X seconds (healthy)" eventually
```

- [ ] **Step 3: Test SQL injection fix**

```bash
# shell_detector with borough parameter — should work normally
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"shell_detector","arguments":{"borough":"NEW YORK","min_corps":10}}}' | head -2
```

- [ ] **Step 4: Test error messages**

```bash
# Trigger a "table not found" error — should include recovery hint
curl -s https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sql_query","arguments":{"sql":"SELECT * FROM lake.fake.nonexistent"}}}' | grep -o '"text":"[^"]*"'
# Expected: Contains "data_catalog" or "list_tables" hint
```

- [ ] **Step 5: Verify container stability**

```bash
ssh hetzner "docker ps --filter name=duckdb-server --format '{{.Names}} {{.Status}}'"
# Expected: Up and healthy
```

---

## Execution Order

```
Task 1 (shell_detector injection)      ← Independent
Task 2 (gentrification_tracker inject) ← Independent
Task 3 (healthcheck)                   ← Independent
Task 4 (error messages)                ← Independent
Task 5 (memory leak)                   ← Independent
Task 6 (deploy + validate)             ← After all code changes
```

Tasks 1-5 are all independent — can run in any order. Task 6 deploys everything.

---

## Verification Checklist

- [ ] `grep "borough.strip().replace" mcp_server.py` returns nothing (injection fix)
- [ ] `grep "CAST.*TIMESTAMP.*{r\[" mcp_server.py` returns nothing (injection fix)
- [ ] `curl https://mcp.common-ground.nyc/health` returns `{"status":"ok"}`
- [ ] Docker reports container as "healthy"
- [ ] SQL error messages include recovery hints ("Use data_catalog..." etc.)
- [ ] `shell_detector(borough="Brooklyn")` works without errors
- [ ] Container stable for 5+ minutes after all tests
