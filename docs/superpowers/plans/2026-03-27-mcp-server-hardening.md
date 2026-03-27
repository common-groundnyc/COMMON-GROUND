# MCP Server Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all SQL injection, credential escaping, PII leakage, and access control issues in the MCP server without breaking production consumers.

**Architecture:** In-place fixes to existing files. No file splitting. Each task targets one module or concern. Every change preserves tool signatures and response shapes. A DEBUG-POST-DEPLOY.md file tracks what to verify after deploy.

**Tech Stack:** Python 3.12, DuckDB 1.5, FastMCP 3.1.1, Docker

**Spec:** `docs/superpowers/specs/2026-03-27-mcp-server-hardening-design.md`

**Working directory:** `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server`

---

### Task 1: Parameterize `entity.py`

**Files:**
- Modify: `entity.py`
- Create: `tests/test_entity_params.py`

- [ ] **Step 1: Write failing tests for parameterized returns**

```python
# tests/test_entity_params.py
from entity import (
    phonetic_search_sql,
    fuzzy_name_sql,
    phonetic_vital_search_sql,
    fuzzy_money_search_sql,
    _validate_identifier,
)
import pytest


def test_phonetic_search_returns_tuple_with_params():
    sql, params = phonetic_search_sql(first_name="John", last_name="Smith")
    assert isinstance(sql, str)
    assert isinstance(params, list)
    assert "?" in sql
    assert "Smith" in params
    assert "John" in params
    # Must NOT contain the raw name in SQL
    assert "'Smith'" not in sql
    assert "'John'" not in sql


def test_phonetic_search_last_only():
    sql, params = phonetic_search_sql(first_name=None, last_name="O'Brien")
    assert "?" in sql
    assert "O'Brien" in params
    assert "'O'Brien'" not in sql


def test_fuzzy_name_returns_tuple():
    sql, params = fuzzy_name_sql(name="BLACKSTONE")
    assert isinstance(params, list)
    assert "BLACKSTONE" in params
    assert "'BLACKSTONE'" not in sql


def test_phonetic_vital_returns_tuple():
    sql, params = phonetic_vital_search_sql(
        first_name="Maria", last_name="Garcia",
        table="lake.housing.nys_death_index",
        first_col="first_name", last_col="last_name",
    )
    assert "?" in sql
    assert "Garcia" in params


def test_fuzzy_money_returns_tuple():
    sql, params = fuzzy_money_search_sql(
        name="KUSHNER",
        table="lake.city_government.campaign_contributions",
        name_col="name",
    )
    assert "?" in sql
    assert "KUSHNER" in params


def test_validate_identifier_good():
    assert _validate_identifier("lake.housing.hpd_violations") == "lake.housing.hpd_violations"
    assert _validate_identifier("first_name") == "first_name"


def test_validate_identifier_bad():
    with pytest.raises(ValueError):
        _validate_identifier("'; DROP TABLE --")
    with pytest.raises(ValueError):
        _validate_identifier("name; DELETE")
    with pytest.raises(ValueError):
        _validate_identifier("")
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
python -m pytest tests/test_entity_params.py -v
```

Expected: FAIL — functions return `str` not `tuple`.

- [ ] **Step 3: Implement parameterized `entity.py`**

Rewrite all 4 functions to return `(sql, params)`. Add `_validate_identifier()` at the top. For each function:
- Replace `'{escaped_var}'` with `?` and add to params list
- Keep identifier params (`table`, `name_col`, etc.) as f-string interpolation but validate them first
- Remove the manual `replace("'", "''")`  escaping

Key changes per function:
- `phonetic_search_sql`: 2 branches (with/without first_name). Replace `'{escaped_last}'` and `'{escaped_first}'` with `?`. Return `(sql, [last_name])` or `(sql, [last_name, first_name, ...])`.
- `fuzzy_name_sql`: Replace `'{escaped}'` with `?`. Validate `table` and `name_col`. Return `(sql, [name])`.
- `phonetic_vital_search_sql`: Replace `'{escaped_last}'` and `'{escaped_first}'` with `?`. Validate `table`, `first_col`, `last_col`. Return `(sql, [last_name, ...])`.
- `fuzzy_money_search_sql`: Replace `'{escaped}'` with `?`. Validate `table`, `name_col`. Return `(sql, [name])`.

**Important DuckDB note:** DuckDB uses `?` for positional params. Each `?` in the SQL maps to the corresponding index in the params list. If the same value appears multiple times in the SQL (e.g., `last_name` used in both WHERE and SELECT), it needs multiple `?` and multiple entries in params.

- [ ] **Step 4: Run tests — expect PASS**

```bash
python -m pytest tests/test_entity_params.py -v
```

- [ ] **Step 5: Commit**

```bash
git add entity.py tests/test_entity_params.py
git commit -m "fix: parameterize entity.py SQL builders to prevent injection"
```

---

### Task 2: Update `mcp_server.py` callers of `entity.py`

**Files:**
- Modify: `mcp_server.py` at lines 6665-6666, 7048-7049, 7189-7190, 10425-10429, 10586-10587, 12320-12322

**Important:** Tasks 2, 4, 5, 6, 7, 9, 10 all modify `mcp_server.py`. These MUST be applied sequentially, not branched in parallel.

- [ ] **Step 1: Update `due_diligence` + `entity_xray` (lines ~6665-6669)**

Read current code at line 6665. The `due_diligence` tool calls `phonetic_search_sql` and passes the result to `_execute(db, ph_sql)`. Change to tuple unpacking:
```python
ph_sql, ph_params = phonetic_search_sql(...)
# later: _execute(db, ph_sql, ph_params)
```

Also update `entity_xray` (line ~6665)**

Read current code at line 6665. Change from:
```python
ph_sql = phonetic_search_sql(...)
# later: _execute(db, ph_sql)
```
to:
```python
ph_sql, ph_params = phonetic_search_sql(...)
# later: _execute(db, ph_sql, ph_params)
```

- [ ] **Step 2: Update `vital_records` (line ~7048)**

Same pattern — `phonetic_vital_search_sql` returns tuple now.

- [ ] **Step 3: Update `money_trail` (line ~7189)**

Same pattern — `fuzzy_money_search_sql` returns tuple now.

- [ ] **Step 4: Update `person_crossref` (lines ~10425-10429)**

Two calls to `phonetic_search_sql` — one with first_name, one without. Both need tuple unpacking.

- [ ] **Step 5: Update `top_crossrefs` (line ~10586)**

Same pattern — `phonetic_vital_search_sql` returns tuple.

- [ ] **Step 6: Update `name_variants` (lines ~12320-12322)**

Two calls to `phonetic_search_sql` — both need tuple unpacking.

- [ ] **Step 7: Grep for any missed callers**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
grep -n "phonetic_search_sql\|fuzzy_name_sql\|phonetic_vital_search_sql\|fuzzy_money_search_sql" mcp_server.py
```

Verify every hit is updated. The import on line 27 stays as-is.

- [ ] **Step 8: Commit**

```bash
git add mcp_server.py
git commit -m "fix: update all entity.py callers for parameterized returns"
```

---

### Task 3: Parameterize `spatial.py`

**Files:**
- Modify: `spatial.py`
- Create: `tests/test_spatial_params.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_spatial_params.py
from spatial import (
    h3_kring_sql,
    h3_heatmap_sql,
    h3_zip_centroid_sql,
    h3_neighborhood_stats_sql,
)


def test_h3_kring_returns_tuple():
    sql, params = h3_kring_sql(40.7128, -74.0060, radius_rings=2)
    assert isinstance(params, list)
    assert 40.7128 in params
    assert "'40.7128'" not in sql


def test_h3_heatmap_returns_tuple():
    sql, params = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table="public_safety.nypd_complaints_historic",
        lat=40.7128, lng=-74.0060,
    )
    assert "?" in sql
    assert "public_safety.nypd_complaints_historic" in params
    # filter_table must NOT be in SQL as raw string
    assert "'public_safety.nypd_complaints_historic'" not in sql


def test_h3_zip_centroid_returns_tuple():
    sql, params = h3_zip_centroid_sql("10003")
    assert "?" in sql
    assert "10003" in params
    assert "'10003'" not in sql


def test_h3_neighborhood_stats_returns_tuple():
    sql, params = h3_neighborhood_stats_sql(40.7128, -74.0060, radius_rings=8)
    assert isinstance(params, list)
    assert 40.7128 in params
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
python -m pytest tests/test_spatial_params.py -v
```

- [ ] **Step 3: Implement parameterized `spatial.py`**

All 5 functions return `(sql, params)`:
- `h3_kring_sql`: params = `[lat, lng, H3_RES, radius_rings]`
- `h3_aggregate_sql`: composes `h3_kring_sql` internally — merge params
- `h3_heatmap_sql`: `filter_table` becomes `?` param, lat/lng/radius as params
- `h3_zip_centroid_sql`: `zipcode` becomes `?` param
- `h3_neighborhood_stats_sql`: composes `h3_kring_sql` — merge params

**Composition pattern:** When `h3_aggregate_sql` calls `h3_kring_sql` internally, it needs to inline the SQL fragment and merge parameter lists:
```python
def h3_aggregate_sql(...):
    kring_sql, kring_params = h3_kring_sql(lat, lng, radius_rings)
    sql = f"""
        WITH target_cells AS ({kring_sql})
        SELECT ... WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table IN (?)
        ...
    """
    return sql, kring_params + [filter_table]
```

**Note on `source_table` identifier in `h3_aggregate_sql`:** The `filter_tables` list contains values like `'public_safety.nypd_complaints_historic'` — these are table identifiers used as filter values against the `source_table` column in the h3_index, NOT SQL identifiers. They should be parameterized as `?` values, not validated as identifiers.

- [ ] **Step 4: Run tests — expect PASS**

```bash
python -m pytest tests/test_spatial_params.py -v
```

- [ ] **Step 5: Commit**

```bash
git add spatial.py tests/test_spatial_params.py
git commit -m "fix: parameterize spatial.py SQL builders"
```

---

### Task 4: Update `mcp_server.py` callers of `spatial.py`

**Files:**
- Modify: `mcp_server.py` at lines 3724-3732, 4728-4737, 8327-8331, 12285-12291

- [ ] **Step 1: Update `neighborhood_compare` (lines ~3724-3732)**

```python
# Before:
centroid_cols, centroid_rows = _execute(db, h3_zip_centroid_sql(z))
# After:
cent_sql, cent_params = h3_zip_centroid_sql(z)
centroid_cols, centroid_rows = _execute(db, cent_sql, cent_params)
```

Same for `h3_neighborhood_stats_sql` call on line ~3732.

- [ ] **Step 2: Update `safety_report` (lines ~4728-4737)**

```python
# h3_heatmap_sql call
hm_sql, hm_params = h3_heatmap_sql(...)
hm_cols, hm_rows = _execute(db, hm_sql, hm_params)
```

- [ ] **Step 3: Update `neighborhood_portrait` (lines ~8327-8331)**

Same pattern as neighborhood_compare.

- [ ] **Step 4: Update `hotspot_map` (lines ~12285-12291)**

```python
sql, params = h3_heatmap_sql(...)
cols, raw_rows = _execute(db, sql, params)
```

- [ ] **Step 5: Grep for missed callers**

```bash
grep -n "h3_kring_sql\|h3_aggregate_sql\|h3_heatmap_sql\|h3_zip_centroid_sql\|h3_neighborhood_stats_sql" mcp_server.py
```

- [ ] **Step 6: Commit**

```bash
git add mcp_server.py
git commit -m "fix: update all spatial.py callers for parameterized returns"
```

---

### Task 5: Harden `quality.py` + Fix `lake_health` injection

**Files:**
- Modify: `quality.py`
- Modify: `mcp_server.py` at line ~12352

- [ ] **Step 1: Add identifier validation to `quality.py`**

Add `_validate_identifier` (same regex as entity.py) and call it at the top of all 4 functions. No return type change needed since there are zero callers — just defensive hardening.

```python
import re

_VALID_IDENTIFIERS = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')

def _validate_identifier(name: str) -> str:
    if not _VALID_IDENTIFIERS.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name
```

- [ ] **Step 2: Fix `lake_health` injection (mcp_server.py line ~12352)**

Read line 12352. Change from:
```python
where = f"WHERE schema_name = '{schema}'" if schema else ""
cols, raw_rows = _execute(db, f"""
    SELECT ... FROM lake.foundation.data_health
    {where}
    ORDER BY row_count DESC
""")
```
to:
```python
if schema:
    where = "WHERE schema_name = ?"
    params = [schema]
else:
    where = ""
    params = []
cols, raw_rows = _execute(db, f"""
    SELECT ... FROM lake.foundation.data_health
    {where}
    ORDER BY row_count DESC
""", params)
```

- [ ] **Step 3: Commit**

```bash
git add quality.py mcp_server.py
git commit -m "fix: add identifier validation to quality.py, parameterize lake_health"
```

---

### Task 6: Fix inline f-string SQL in `mcp_server.py`

**Files:**
- Modify: `mcp_server.py` at multiple locations

These are already safe due to input validation but should use `?` for consistency.

- [ ] **Step 1: Fix `safety_report` precinct injection (line ~4732)**

Read surrounding code. Change `WHERE policeprct = '{precinct}'` to `WHERE policeprct = ?` and add `precinct` to the params list of the `_execute` call.

- [ ] **Step 2: Fix `semantic_search` source filter (line ~3433)**

Change `f"WHERE source = '{source}'"` to `"WHERE source = ?"` with params.

Read line 3433 context — the `source_filter` is interpolated into a Lance vector search SQL. Check if DuckDB supports `?` params in lance_vector_search queries. If not, validate `source` against `VALID_DESC_SOURCES` frozenset (already done on line 3359) and keep the f-string with a comment explaining why.

- [ ] **Step 3: Fix `text_search` source filter (line ~10523)**

Same pattern as step 2 — check if Lance SQL supports params.

- [ ] **Step 4: Fix BBL-based Lance queries (line ~8021)**

`building_vectors.lance` query uses `WHERE bbl = '{bbl}'`. BBL is regex-validated to `\d{10}`. If Lance doesn't support `?`, keep f-string with comment. Otherwise parameterize.

- [ ] **Step 5: Fix graph PGQ MATCH queries (lines ~8982, 9244, 11325, 11447, 11587, 11822)**

DuckPGQ `MATCH` syntax uses `WHERE` inside the pattern:
```sql
MATCH (b1:Building WHERE b1.bbl = '{bbl}')
```

Check DuckPGQ docs — if `?` params work inside MATCH patterns, parameterize. If not (likely — PGQ MATCH is a special syntax), keep f-string with `replace("'", "''")` escaping and add comment:
```python
# DuckPGQ MATCH patterns don't support ? params — BBL is regex-validated to \d{10}
```

- [ ] **Step 6: Fix `_resolve_name_variants` (lines ~10347, 10356)**

These use `UPPER(last_name) = '{words[-1]}'`. These come from splitting a user-supplied name string. Change to `?` params:
```python
WHERE UPPER(last_name) = UPPER(?) AND UPPER(first_name) LIKE UPPER(?) || '%'
```

- [ ] **Step 7: Commit**

```bash
git add mcp_server.py
git commit -m "fix: parameterize remaining inline f-string SQL where possible"
```

---

### Task 7: Fix credential escaping

**Files:**
- Modify: `mcp_server.py` at lines 720, 755, 698-699, 741-742, 12597-12598, 12602
- Modify: `warmup.py` at line 5
- Modify: `server.py` at line 12-14

- [ ] **Step 1: Fix `mcp_server.py` lifespan credential escape (line ~720)**

Change:
```python
pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "\\'")
```
to:
```python
pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
```

This one change fixes both line 722 (primary attach) and line 755 (warm-up recovery attach) since they use the same variable.

- [ ] **Step 2: Fix MinIO credential interpolation (lines ~698-699 and ~741-742)**

Add escaping for minio credentials:
```python
minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin").replace("'", "''")
minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "").replace("'", "''")
```

Apply at line ~694 (before first use). The same variables are reused at line 741-742 (warm-up recovery), so no additional change needed there.

- [ ] **Step 3: Fix `_catalog_connect()` credentials (lines ~12597-12602)**

Same two changes:
```python
minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin").replace("'", "''")
minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "").replace("'", "''")
# ...
pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "").replace("'", "''")
```

- [ ] **Step 4: Fix `warmup.py` (line ~5)**

```python
pg_pass = os.environ.get('DAGSTER_PG_PASSWORD', '').replace("'", "''")
```

- [ ] **Step 5: Fix `server.py` default password + credential escape**

Change line 12 (pg_pass) and lines 29-30 (api_user/api_pass):
```python
pg_pass = os.environ["DAGSTER_PG_PASSWORD"].replace("'", "''")
```

Remove the default — require the env var. If it's not set, the server crashes at startup with a clear `KeyError`, which is correct behavior.

Also fix the API password default:
```python
api_user = os.environ.get("DUCKDB_API_USER", "")
api_pass = os.environ.get("DUCKDB_API_PASS", "")
if not api_user or not api_pass:
    print("Warning: DUCKDB_API_USER/DUCKDB_API_PASS not set, HTTP API disabled", flush=True)
else:
    conn.execute(f"SELECT httpserve_start('0.0.0.0', 9999, '{api_user}:{api_pass}')")
```

- [ ] **Step 6: Commit**

```bash
git add mcp_server.py warmup.py server.py
git commit -m "fix: correct credential escaping ('→'' not '→\\'), remove default passwords"
```

---

### Task 8: Expand SQL blocklist

**Files:**
- Modify: `mcp_server.py` at line ~49

- [ ] **Step 1: Write failing test**

**Note:** Importing `_validate_sql` from `mcp_server.py` may trigger full module initialization (FastMCP, DuckDB, etc.) which will fail outside Docker. To work around this, either: (a) run tests inside the Docker container (`docker run --rm cg-mcp-test python -m pytest tests/test_sql_validation.py -v`), or (b) extract `_validate_sql` and `_UNSAFE_SQL` into a separate lightweight file. Option (a) is simpler and avoids restructuring.

```python
# Add to an existing or new test file
# tests/test_sql_validation.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

def test_blocklist_blocks_call():
    from mcp_server import _validate_sql
    from fastmcp.exceptions import ToolError
    import pytest
    with pytest.raises(ToolError):
        _validate_sql("CALL lake.set_option('enable_compaction', 'true')")

def test_blocklist_blocks_load():
    from mcp_server import _validate_sql
    from fastmcp.exceptions import ToolError
    import pytest
    with pytest.raises(ToolError):
        _validate_sql("LOAD httpserver")

def test_blocklist_blocks_attach():
    from mcp_server import _validate_sql
    from fastmcp.exceptions import ToolError
    import pytest
    with pytest.raises(ToolError):
        _validate_sql("ATTACH ':memory:' AS evil")

def test_blocklist_allows_select():
    from mcp_server import _validate_sql
    _validate_sql("SELECT * FROM lake.housing.hpd_violations LIMIT 10")

def test_blocklist_allows_explain():
    from mcp_server import _validate_sql
    _validate_sql("EXPLAIN SELECT 1")

def test_blocklist_allows_pragma():
    from mcp_server import _validate_sql
    _validate_sql("PRAGMA database_list")
```

- [ ] **Step 2: Run tests — expect mixed (some FAIL for CALL/LOAD/ATTACH)**

```bash
python -m pytest tests/test_sql_validation.py -v
```

- [ ] **Step 3: Expand the blocklist (line ~49)**

Change:
```python
_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)
```
to:
```python
_UNSAFE_SQL = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY"
    r"|CALL|LOAD|INSTALL|ATTACH|DETACH|EXPORT|IMPORT)\b",
    re.IGNORECASE,
)
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
python -m pytest tests/test_sql_validation.py -v
```

- [ ] **Step 5: Commit**

```bash
git add mcp_server.py tests/test_sql_validation.py
git commit -m "fix: expand SQL blocklist to block CALL, LOAD, INSTALL, ATTACH, DETACH, EXPORT, IMPORT"
```

---

### Task 9: Redact PII in PostHog

**Files:**
- Modify: `mcp_server.py` at line ~12538

- [ ] **Step 1: Change argument capture (line ~12538)**

Read lines 12535-12540. Change:
```python
for k, v in arguments.items():
    props[f"arg_{k}"] = str(v)[:200]
```
to:
```python
for k, v in arguments.items():
    sv = str(v)
    props[f"arg_{k}"] = f"<{type(v).__name__}:{len(sv)}chars>"
```

- [ ] **Step 2: Commit**

```bash
git add mcp_server.py
git commit -m "fix: redact PII from PostHog tool argument capture"
```

---

### Task 10: Restrict CORS on `/api/catalog`

**Files:**
- Modify: `mcp_server.py` at lines ~12620 and ~12760

- [ ] **Step 1: Add CORS origin allowlist (near line ~12610)**

Add above the `catalog_json` function:
```python
_ALLOWED_ORIGINS = frozenset({
    "https://common-ground.nyc",
    "https://www.common-ground.nyc",
    "http://localhost:3002",
    "http://localhost:3000",
})


def _cors_origin(request) -> str:
    origin = request.headers.get("origin", "")
    return origin if origin in _ALLOWED_ORIGINS else ""
```

- [ ] **Step 2: Update OPTIONS handler (line ~12620)**

Change:
```python
"Access-Control-Allow-Origin": "*",
```
to:
```python
"Access-Control-Allow-Origin": _cors_origin(request),
"Vary": "Origin",
```

- [ ] **Step 3: Update GET response (line ~12760)**

Change:
```python
"Access-Control-Allow-Origin": "*",
```
to:
```python
"Access-Control-Allow-Origin": _cors_origin(request),
"Vary": "Origin",
```

- [ ] **Step 4: Commit**

```bash
git add mcp_server.py
git commit -m "fix: restrict CORS on /api/catalog to known origins"
```

---

### Task 11: Remove `allow_unsigned_extensions` from `warmup.py`

**Files:**
- Modify: `warmup.py` at line ~6

- [ ] **Step 1: Change connection config**

Change:
```python
conn = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
```
to:
```python
conn = duckdb.connect()
```

The warmup script only loads `ducklake`, `postgres`, `httpfs` — all signed core extensions.

- [ ] **Step 2: Commit**

```bash
git add warmup.py
git commit -m "fix: remove allow_unsigned_extensions from warmup (core exts only)"
```

---

### Task 12: Create DEBUG-POST-DEPLOY.md

**Files:**
- Create: `DEBUG-POST-DEPLOY.md`

- [ ] **Step 1: Write the post-deploy verification checklist**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add DEBUG-POST-DEPLOY.md
git commit -m "docs: add post-deploy verification checklist for hardening pass"
```

---

### Task 13: Docker rebuild smoke test

- [ ] **Step 1: Build the Docker image locally**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server
docker build -t cg-mcp-test .
```

**Verify:** Build succeeds with no import errors.

- [ ] **Step 2: Run existing tests inside container**

```bash
docker run --rm cg-mcp-test python -m pytest tests/ -v --tb=short
```

**Verify:** All tests pass including the new ones from Tasks 1, 3, 8.

- [ ] **Step 3: Quick import smoke test**

```bash
docker run --rm cg-mcp-test python -c "
from entity import phonetic_search_sql, _validate_identifier
from spatial import h3_zip_centroid_sql
from quality import _validate_identifier as qi
sql, params = phonetic_search_sql(first_name='John', last_name=\"O'Brien\")
assert isinstance(params, list), 'entity.py not returning tuple'
sql2, params2 = h3_zip_centroid_sql('10003')
assert isinstance(params2, list), 'spatial.py not returning tuple'
print('All imports and return types OK')
"
```

- [ ] **Step 4: Tag and push**

If all checks pass, this is ready for `deploy.sh`.
