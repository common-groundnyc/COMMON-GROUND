# MCP Server Hardening — Design Spec

**Date:** 2026-03-27
**Scope:** Security fixes applied in-place to the existing monolith. No file restructuring.
**Downtime:** Brief maintenance window during deploy (late night).

---

## Constraints

- No splitting of `mcp_server.py` (13K lines stays as-is)
- No rate limiting (website says "no rate limits, free forever")
- No shared catalog connection pool (current per-request approach is safe)
- No external URL removal from rent stabilization build
- No new test coverage (deferred)
- All changes must be backward-compatible with existing MCP clients (Claude Desktop, ChatGPT, Cursor)
- Website data health page (`common-ground.nyc/data`) must keep working

---

## Fix 1: SQL Injection in `entity.py`

### Current State

Four functions build SQL via f-string interpolation with only `'` → `''` escaping:
- `phonetic_search_sql(first_name, last_name, ...)`
- `fuzzy_name_sql(name, table, name_col, ...)`
- `phonetic_vital_search_sql(first_name, last_name, table, ...)`
- `fuzzy_money_search_sql(name, table, name_col, ...)`

The `table`, `name_col`, `first_col`, `last_col`, `extra_cols` parameters are injected as raw identifiers with zero escaping.

### Change

Each function returns a `tuple[str, list]` instead of `str`. User-supplied values become `?` placeholders. Identifier parameters (`table`, `name_col`, etc.) are validated against an allowlist of known table/column names.

**Before:**
```python
def phonetic_search_sql(first_name, last_name, ...):
    escaped_last = last_name.replace("'", "''")
    return f"... WHERE ... UPPER('{escaped_last}') ..."
```

**After:**
```python
def phonetic_search_sql(first_name, last_name, ...):
    sql = "... WHERE ... UPPER(?) ..."
    params = [last_name]
    return sql, params
```

### Callers to Update (in `mcp_server.py`)

Every tool that calls these functions must change from:
```python
sql = phonetic_search_sql(first, last)
cols, rows = _execute(db, sql)
```
to:
```python
sql, params = phonetic_search_sql(first, last)
cols, rows = _execute(db, sql, params)
```

**Affected tools (verified by grep):** `entity_xray`, `person_crossref`, `due_diligence`, `vital_records`, `money_trail`, `cop_sheet`, `top_crossrefs`. (7 tools, 8 call sites)

**Note:** `fuzzy_name_sql` is imported but has zero callers in `mcp_server.py`. Parameterize it for defensive future-proofing but no caller update needed.

### Identifier Validation

For `table`, `name_col`, `first_col`, `last_col` — these are currently always called with hardcoded string literals from `mcp_server.py`. Add a validation function:

```python
_VALID_IDENTIFIERS = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')

def _validate_identifier(name: str) -> str:
    if not _VALID_IDENTIFIERS.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name
```

Apply at the top of each function that receives identifier params.

### Downstream Impact

- **No external API changes.** Tool signatures, names, and behavior are identical.
- **Risk:** If any caller passes the result of these functions to `_execute()` differently than expected (e.g., string concatenation), it will break. Grep all callers.

---

## Fix 2: SQL Injection in `spatial.py`

### Current State

- `h3_heatmap_sql()` injects `filter_table` via f-string: `AND source_table = '{filter_table}'`
- `h3_zip_centroid_sql()` injects `zipcode` via f-string: `WHERE zipcode = '{zipcode}'`
- `h3_kring_sql()` injects `lat`, `lng`, `radius_rings` — these are floats/ints from validated tool parameters, low risk but should still use params.

### Change

Same pattern as Fix 1: return `(sql, params)` tuples. Validate `filter_table` against known table names.

### Callers to Update (verified by grep)

- `neighborhood_compare` calls `h3_zip_centroid_sql`, `h3_neighborhood_stats_sql`
- `neighborhood_portrait` calls `h3_zip_centroid_sql`, `h3_neighborhood_stats_sql`
- `safety_report` calls `h3_heatmap_sql`
- `hotspot_map` calls `h3_heatmap_sql`

4 tools need updating. (`area_snapshot` does NOT use spatial.py — it uses `ST_DWithin` directly with parameterized queries.)

### Downstream Impact

None — internal refactor only.

---

## Fix 3: SQL Injection in `quality.py`

### Current State

All 4 functions (`approx_distinct_sql`, `approx_quantiles_sql`, `frequent_items_sql`, `iqr_outliers_sql`) inject `table` and `column` as raw identifiers.

### Change

Validate identifiers against the regex allowlist.

**Note:** `quality.py` has zero current callers — it is imported nowhere in `mcp_server.py`. The `environmental_justice` tool uses hardcoded SQL constants with `?` parameterization, not these helper functions. Validation is added for defensive future-proofing only.

### Callers to Update

None — zero current callers.

### Downstream Impact

None.

---

## Fix 3b: SQL Injection in `lake_health` tool (mcp_server.py)

### Current State

The `lake_health` tool has a `schema` parameter that is user-supplied and directly interpolated:
```python
where = f"WHERE schema_name = '{schema}'" if schema else ""
```

This is a real, exploitable injection vector — unlike the helper files, this is in a tool that MCP clients invoke directly.

### Change

Use `?` parameterization:
```python
where = "WHERE schema_name = ?" if schema else ""
params = [schema] if schema else []
```

### Callers to Update

Self-contained — the interpolation is inside the tool function itself.

### Downstream Impact

None — same results, just safe.

---

## Fix 3c: Inline f-string SQL in `mcp_server.py` (opportunistic)

### Current State

Several tools in `mcp_server.py` use f-string interpolation for SQL values that are already validated by input constraints but violate the hardening principle:

- `safety_report`: `WHERE policeprct = '{precinct}'` — safe (precinct is typed `int`), but should use `?`
- `similar_buildings` / other BBL tools: `WHERE bbl = '{bbl}'` — safe (BBL is regex-validated to `\d{10}`), but should use `?`
- `semantic_search` / `text_search`: `WHERE source = '{source}'` — safe (validated against frozenset), but should use `?`

### Change

Convert to `?` parameterization opportunistically while touching these files. These are low-risk because the inputs are already validated, but consistency matters — a future developer shouldn't have to wonder "is this f-string safe?" for every query.

### Downstream Impact

None — identical behavior with parameterized queries.

---

## Fix 4: Credential Escaping

### Current State

Five locations interpolate `pg_pass` into SQL:
1. `server.py:14` — `password={pg_pass}` (no escape)
2. `mcp_server.py` lifespan — `password={pg_pass}` (with `.replace("'", "\\\\'")` — wrong escape)
3. `mcp_server.py` warm-up recovery — same variable, same wrong escape
4. `warmup.py:11` — `password={pg_pass}` (no escape)
5. `_catalog_connect()` in `mcp_server.py` — `password={pg_pass}` (with `.replace("'", "\\\\'")` — wrong escape)

The current escape `replace("'", "\\\\'")` produces a backslash before the quote. DuckDB's SQL parser doesn't treat `\'` as an escape — the correct SQL escape is `''`.

### Change

Replace all instances with:
```python
pg_pass_escaped = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
```

Also applies to MinIO credentials (`minio_user`, `minio_pass`) which are interpolated into `SET s3_access_key_id = '{minio_user}'` — same fix.

### Locations for MinIO credential fix

- `mcp_server.py:661-662` (lifespan)
- `mcp_server.py:704-705` (warm-up recovery)
- `_catalog_connect()` at `mcp_server.py:12742-12743`

### Downstream Impact

- If the production password is alphanumeric only: **no-op** — behavior identical.
- If the password contains `'`: **fixes a latent bug** that would crash on startup.
- **Test with actual production password** after deploy by checking container logs for "DuckLake catalog attached".

---

## Fix 5: SQL Blocklist Expansion

### Current State

`_UNSAFE_SQL` regex blocks: `INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY`.

Missing: `CALL, LOAD, INSTALL, ATTACH, DETACH, EXPORT, IMPORT`.

`CALL` is particularly dangerous — a user could run `CALL lake.set_option(...)` to change DuckLake catalog settings. `LOAD` could load arbitrary extensions. `ATTACH` could connect to external databases.

**Intentionally NOT blocked:** `SET` and `PRAGMA` are left off the blocklist. `SET` is mitigated by `lock_configuration = true` (set during lifespan), and `PRAGMA` is needed for legitimate read-only introspection (the error message at `_validate_sql` explicitly allows it). `EXPLAIN`, `DESCRIBE`, and `SHOW` remain allowed for the same reason.

### Change

Expand `_UNSAFE_SQL`:
```python
_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY"
    r"|CALL|LOAD|INSTALL|ATTACH|DETACH|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)
```

Keep the `_SAFE_DDL` exception for `CREATE OR REPLACE VIEW` (used by `sql_admin`).

### Downstream Impact

- Any MCP client whose LLM has learned to send `CALL`, `LOAD`, etc. through `sql_query()` will get an error. This is correct behavior — those were never intended.
- `sql_admin` still works because it has its own validation path that only allows `CREATE OR REPLACE VIEW`.
- **No legitimate use case is blocked.**

---

## Fix 6: PII Redaction in PostHog

### Current State

```python
for k, v in arguments.items():
    props[f"arg_{k}"] = str(v)[:200]
```

Tool arguments (BBLs, personal names, SQL queries, addresses) are sent to PostHog as-is.

### Change

Replace argument values with type/length indicators:
```python
for k, v in arguments.items():
    sv = str(v)
    props[f"arg_{k}"] = f"<{type(v).__name__}:{len(sv)}chars>"
```

This preserves the ability to see which arguments were passed (and their rough size) without leaking PII. Example: `arg_name` becomes `<str:18chars>` instead of `"BARTON PERLBINDER"`.

Keep `tool` name, `duration_ms`, `session_id`, `client_name`, `client_version` as-is — these are not PII.

### Downstream Impact

- PostHog events lose argument content. Dashboards filtering on argument values will stop matching. (User confirmed: no such dashboards exist.)

---

## Fix 7: Remove Default Password

### Current State

`server.py:29-30`:
```python
api_user = os.environ.get("DUCKDB_API_USER", "admin")
api_pass = os.environ.get("DUCKDB_API_PASS", "changeme")
```

### Change

`server.py` appears to be dead code — the Docker entrypoint runs `mcp_server.py` via `start.sh`, not `server.py`. The MCP server's lifespan starts the DuckDB UI directly on :8080.

**Verify** by grepping docker-compose and deploy scripts for `server.py`. If confirmed dead: remove the default, require env var, log a warning if missing.

### Downstream Impact

- If `server.py` is truly unused: none.
- If some manual debug workflow uses it: that workflow must now set `DUCKDB_API_PASS`. Document in debug file.

---

## Fix 8: CORS Restriction on `/api/catalog`

### Current State

```python
"Access-Control-Allow-Origin": "*"
```

### Change

Restrict to known origins:
```python
_ALLOWED_ORIGINS = {
    "https://common-ground.nyc",
    "https://www.common-ground.nyc",
    "http://localhost:3002",  # local dev
    "http://localhost:3000",  # local dev
}

origin = request.headers.get("origin", "")
cors_origin = origin if origin in _ALLOWED_ORIGINS else ""
```

Return the matched origin in `Access-Control-Allow-Origin` header (not `*`). Include `"Vary": "Origin"` header to prevent CDN/browser caching issues when multiple origins hit the same endpoint.

### Downstream Impact

- **Website data health page:** Works — `common-ground.nyc` is in the allowlist.
- **Local dev:** Works — `localhost:3002` (Next.js dev) and `localhost:3000` are in the allowlist.
- **Browser-based third-party consumers:** Blocked. Non-browser consumers (curl, scripts) unaffected.
- **MCP clients:** Unaffected — MCP uses the `/mcp` endpoint, not `/api/catalog`, and MCP clients don't send CORS headers.

---

## Fix 9: Remove `allow_unsigned_extensions` Where Unnecessary

### Current State

5 DuckDB connections use `config={"allow_unsigned_extensions": "true"}`:
- `mcp_server.py` lifespan (main connection)
- `mcp_server.py` warm-up recovery (reconnect after compactor crash)
- `warmup.py` (disposable warm-up script)
- `_catalog_connect()` (per-request catalog endpoint)
- `mcp_server.py` background embedding thread (if it creates a separate connection)

### Change

**Keep** it in the main lifespan connection — community extensions need it.
**Remove** from `warmup.py` and `_catalog_connect()` — they don't load community extensions, only core extensions (`ducklake`, `postgres`, `httpfs`).

Actually — `_catalog_connect()` calls `load_extensions(conn)` which loads ALL community extensions. So it needs the flag too.

**Revised:** Only remove from `warmup.py`, which only loads `ducklake`, `postgres`, `httpfs`.

### Downstream Impact

- `warmup.py` will fail if any of its 3 extensions are unsigned. Core extensions (`ducklake`, `postgres`, `httpfs`) are all signed. Safe.

---

## Fix 10: Downstream Breakage Log

Create a `DEBUG-POST-DEPLOY.md` file documenting everything that needs manual verification after deploying these fixes.

### Contents

1. **Container startup check** — verify "DuckLake catalog attached" in logs (credential fix validation)
2. **Website data health page** — load `common-ground.nyc/data`, confirm catalog loads (CORS fix validation)
3. **MCP tool smoke test** — from Claude Code, call `building_profile(bbl="1000670001")` and `entity_xray(name="BARTON PERLBINDER")` (parameterization fix validation)
4. **sql_query blocklist test** — try `sql_query("CALL lake.set_option('enable_compaction', 'true')")` — should return error (blocklist fix validation)
5. **lake_health test** — call `lake_health(schema="housing")` — should return results (injection fix validation)
6. **spatial tool test** — call `neighborhood_portrait(zipcode="10003")` — should return results (spatial parameterization validation)
7. **semantic_search test** — call `semantic_search(query="pest problems", source="restaurant")` — should return results (inline f-string parameterization validation)
8. **PostHog event check** — verify events still arrive, confirm `arg_*` values are redacted (PII fix validation)
9. **Graph build check** — verify "Property graph built: X owners, Y buildings" in logs (no regression from credential fix)
10. **Embedding pipeline check** — verify "Lance embedding pipeline complete" in logs (no regression)
11. **Deferred items** — list of what was NOT fixed and why

---

## Files Modified

| File | Changes |
|------|---------|
| `entity.py` | 4 functions → return `(sql, params)` tuples; add identifier validation |
| `spatial.py` | 3 functions → return `(sql, params)` tuples; validate `filter_table` |
| `quality.py` | 4 functions → add identifier validation |
| `mcp_server.py` | ~13 tool callers updated for tuple returns; `lake_health` injection fixed; inline f-strings parameterized; credential escaping fixed in 5 locations; SQL blocklist expanded; PostHog args redacted; CORS restricted with `Vary: Origin` |
| `warmup.py` | Remove `allow_unsigned_extensions` |
| `server.py` | Remove default password |
| `DEBUG-POST-DEPLOY.md` (new) | Post-deploy verification checklist |

## Files NOT Modified

| File | Reason |
|------|--------|
| `Dockerfile` | No changes needed — same files, same entrypoint |
| `start.sh` | No changes needed |
| `hetzner-docker-compose.yml` | No changes needed |
| `deploy.sh` | No changes needed |
| `common-ground-website/*` | No changes needed — CORS allowlist includes their domain |
| `formatters.py` | No security issues |
| `response_middleware.py` | No security issues |
| `csv_export.py` | No security issues |
| `xlsx_export.py` | No security issues |
| `percentile_middleware.py` | No security issues |
| `percentiles.py` | No security issues (uses parameterized queries already) |
| `source_links.py` | No security issues |
| `embedder.py` | No security issues |
| `extensions.py` | No security issues |
| `download_model.py` | No security issues |
