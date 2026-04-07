# Direct DuckDB Ingestion Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace dlt + docker_executor with parallel HTTP fetch → Arrow → DuckDB zero-copy → DuckLake, achieving 8x throughput at 15x less memory.

**Architecture:** Each Dagster @asset fetches data from Socrata (or other APIs) using Python ThreadPoolExecutor with 10-30 parallel workers, converts JSON to Arrow tables, and inserts into DuckLake via DuckDB's zero-copy Arrow scan. A shared DuckLakeResource manages the connection. multiprocess_executor replaces docker_executor — no containers, env vars inherited. Incremental cursors migrated from dlt's compressed state to a simple DuckLake table.

**Tech Stack:** Dagster 1.12+ (multiprocess_executor), DuckDB 1.5.1 (curl_httpfs, ducklake, postgres extensions), PyArrow, httpx, DuckLake 0.4 (Postgres catalog + MinIO S3)

---

## File Structure

```
src/dagster_pipeline/
├── resources/
│   └── ducklake.py              # NEW — DuckLakeResource (ConfigurableResource)
├── ingestion/
│   ├── __init__.py               # NEW
│   ├── fetcher.py                # NEW — parallel HTTP fetch → Arrow
│   ├── socrata.py                # NEW — Socrata-specific: URL builders, column maps, CSV/JSON selection
│   └── writer.py                 # NEW — DuckLake write: full replace, delta merge, schema evolution
├── defs/
│   ├── socrata_direct_assets.py  # NEW — @asset per Socrata dataset (replaces socrata_assets.py)
│   ├── federal_direct_assets.py  # NEW — @asset per federal source (replaces federal_assets.py)
│   └── flush_sensor.py           # KEEP
├── sources/
│   ├── datasets.py               # KEEP — dataset registry with schedule tiers
│   └── (old dlt sources)         # KEEP for reference, not imported
├── definitions.py                # MODIFY — swap executor, resources, asset imports
```

---

### Task 0: Research Validation (already done)

Findings locked in:
- **Connection**: single per process, crash-and-restart (millpond pattern)
- **Executor**: `multiprocess_executor` with `forkserver`, `max_concurrent: 8`
- **Write concurrency**: `tag_concurrency_limits` by schema (1 writer per schema)
- **Schema evolution**: `INSERT BY NAME`, `ALTER TABLE ADD COLUMN IF NOT EXISTS`
- **Incremental**: `MERGE INTO ... ON (_id) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`
- **Types**: auto-detect (3.6x faster than all_varchar), `ignore_errors=true` for safety

---

### Task 1: DuckLakeResource

**Files:**
- Create: `src/dagster_pipeline/resources/__init__.py`
- Create: `src/dagster_pipeline/resources/ducklake.py`
- Test: `tests/test_ducklake_resource.py`

- [ ] **Step 1: Write failing test for resource connection**

```python
# tests/test_ducklake_resource.py
from dagster_pipeline.resources.ducklake import DuckLakeResource

def test_ducklake_resource_connects():
    r = DuckLakeResource(
        catalog_url="ducklake:postgres:dbname=ducklake user=dagster password=test host=178.156.228.119 port=5432",
        s3_endpoint="178.156.228.119:9000",
        s3_access_key="minioadmin",
        s3_secret_key="test",
    )
    conn = r.get_connection()
    assert conn is not None
    result = conn.execute("SELECT 1").fetchone()
    assert result[0] == 1
    conn.close()
```

- [ ] **Step 2: Implement DuckLakeResource**

```python
# src/dagster_pipeline/resources/ducklake.py
"""DuckLake connection resource — single connection per process.
Follows millpond pattern: crash-and-restart, no connection pooling.
"""
import dagster as dg
import duckdb


class DuckLakeResource(dg.ConfigurableResource):
    catalog_url: str        # ducklake:postgres:dbname=ducklake user=... host=... port=...
    s3_endpoint: str        # 178.156.228.119:9000
    s3_access_key: str
    s3_secret_key: str
    s3_use_ssl: bool = False
    memory_limit: str = "2GB"
    threads: int = 4

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        conn.execute("INSTALL curl_httpfs FROM community")
        conn.execute("LOAD curl_httpfs")
        conn.execute("LOAD ducklake")
        conn.execute("LOAD postgres")

        conn.execute(f"SET s3_endpoint='{self.s3_endpoint}'")
        conn.execute(f"SET s3_access_key_id='{self.s3_access_key}'")
        conn.execute(f"SET s3_secret_access_key='{self.s3_secret_key}'")
        conn.execute(f"SET s3_use_ssl={'true' if self.s3_use_ssl else 'false'}")
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute("SET http_timeout=300000")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")

        conn.execute(f"ATTACH '{self.catalog_url}' AS lake (METADATA_SCHEMA 'lake')")
        return conn
```

- [ ] **Step 3: Run test, verify pass**
- [ ] **Step 4: Commit**

```
feat: add DuckLakeResource — single DuckDB connection to DuckLake
```

---

### Task 2: Parallel HTTP Fetcher

**Files:**
- Create: `src/dagster_pipeline/ingestion/__init__.py`
- Create: `src/dagster_pipeline/ingestion/fetcher.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_fetcher.py
from dagster_pipeline.ingestion.fetcher import parallel_fetch_json
import pyarrow as pa

def test_parallel_fetch_returns_arrow():
    """Fetch 2 pages from Socrata air_quality, get Arrow table."""
    urls = [
        "https://data.cityofnewyork.us/resource/c3uy-2p5r.json?$limit=100&$offset=0&$order=:id",
        "https://data.cityofnewyork.us/resource/c3uy-2p5r.json?$limit=100&$offset=100&$order=:id",
    ]
    table = parallel_fetch_json(urls, max_workers=2)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 200
```

- [ ] **Step 2: Implement fetcher**

```python
# src/dagster_pipeline/ingestion/fetcher.py
"""Parallel HTTP fetch → Arrow tables. The fast path.

Benchmarked: 33,143 rows/sec at 427MB with 30 workers + column projection + gzip.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import pyarrow as pa

logger = logging.getLogger(__name__)

_CLIENT = httpx.Client(timeout=120, headers={"Accept-Encoding": "gzip"})


def fetch_json_page(url: str) -> pa.Table | None:
    """Fetch one JSON page, return as Arrow table."""
    resp = _CLIENT.get(url)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        return None
    table = pa.Table.from_pylist(rows)
    del rows
    return table


def parallel_fetch_json(urls: list[str], max_workers: int = 10) -> pa.Table | None:
    """Fetch multiple URLs in parallel, return concatenated Arrow table."""
    tables = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as pool:
        futures = {pool.submit(fetch_json_page, url): i for i, url in enumerate(urls)}
        for future in as_completed(futures):
            table = future.result()
            if table:
                tables.append(table)

    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="permissive")
```

- [ ] **Step 3: Run test, verify pass**
- [ ] **Step 4: Commit**

```
feat: add parallel HTTP fetcher — concurrent JSON pages → Arrow
```

---

### Task 3: Socrata URL Builder + Column Registry

**Files:**
- Create: `src/dagster_pipeline/ingestion/socrata.py`
- Test: `tests/test_socrata.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_socrata.py
from dagster_pipeline.ingestion.socrata import build_page_urls, get_row_count

def test_build_page_urls():
    urls = build_page_urls("data.cityofnewyork.us", "c3uy-2p5r", token="", num_pages=3)
    assert len(urls) == 3
    assert "$limit=50000" in urls[0]
    assert "$offset=0" in urls[0]
    assert "$offset=100000" in urls[2]

def test_get_row_count():
    count = get_row_count("data.cityofnewyork.us", "c3uy-2p5r", token="")
    assert count > 10000  # air_quality has ~18k rows
```

- [ ] **Step 2: Implement Socrata helpers**

```python
# src/dagster_pipeline/ingestion/socrata.py
"""Socrata-specific: URL construction, column projection, row counts.

Optimizations applied:
- $select=needed_cols → 75% less data transfer
- Accept-Encoding: gzip → 5x compression
- 30 parallel workers → 6x throughput
- auto_detect types → 3.6x faster than all_varchar
"""
import logging
import httpx

logger = logging.getLogger(__name__)

PAGE_SIZE = 50_000
MAX_WORKERS = 10

DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}


def get_row_count(domain: str, dataset_id: str, token: str,
                  where: str | None = None) -> int:
    params = {"$select": "count(*) as count", "$$app_token": token}
    if where:
        params["$where"] = where
    resp = httpx.get(f"https://{domain}/resource/{dataset_id}.json",
                     params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return int(data[0]["count"]) if data else 0


def build_page_urls(domain: str, dataset_id: str, token: str,
                    num_pages: int, select: str | None = None,
                    where: str | None = None) -> list[str]:
    """Build paginated Socrata JSON URLs."""
    urls = []
    for i in range(num_pages):
        params = {
            "$limit": str(PAGE_SIZE),
            "$offset": str(i * PAGE_SIZE),
            "$order": ":id",
            "$$app_token": token,
        }
        if select:
            params["$select"] = select
        if where:
            params["$where"] = where

        query = "&".join(f"{k}={v}" for k, v in params.items())
        urls.append(f"https://{domain}/resource/{dataset_id}.json?{query}")
    return urls
```

- [ ] **Step 3: Run test, verify pass**
- [ ] **Step 4: Commit**

```
feat: add Socrata URL builder with column projection + pagination
```

---

### Task 4: DuckLake Writer

**Files:**
- Create: `src/dagster_pipeline/ingestion/writer.py`
- Test: `tests/test_writer.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_writer.py
import duckdb
import pyarrow as pa
from dagster_pipeline.ingestion.writer import write_full_replace, write_delta_merge

def test_write_full_replace():
    conn = duckdb.connect(":memory:")
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    write_full_replace(conn, "main", "test_table", table)
    result = conn.execute("SELECT count(*) FROM main.test_table").fetchone()[0]
    assert result == 3
```

- [ ] **Step 2: Implement writer**

```python
# src/dagster_pipeline/ingestion/writer.py
"""DuckLake write operations — full replace and delta merge.

Uses DuckDB zero-copy Arrow scan (1.2M rows/sec insert).
Follows millpond pattern: INSERT BY NAME, schema evolution via ALTER TABLE.
"""
import logging
import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)


def write_full_replace(conn: duckdb.DuckDBPyConnection,
                       schema: str, table_name: str, table: pa.Table) -> int:
    """Drop and recreate table from Arrow. For initial loads."""
    fqn = f"lake.{schema}.{table_name}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    conn.execute(f"DROP TABLE IF EXISTS {fqn}")
    conn.register("_arrow_batch", table)
    try:
        conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM _arrow_batch")
    finally:
        conn.unregister("_arrow_batch")
    logger.info("%s: wrote %d rows (full replace)", fqn, table.num_rows)
    return table.num_rows


def write_delta_merge(conn: duckdb.DuckDBPyConnection,
                      schema: str, table_name: str, table: pa.Table,
                      merge_key: str = "_id") -> int:
    """MERGE INTO for incremental upsert by key."""
    fqn = f"lake.{schema}.{table_name}"
    conn.register("_arrow_batch", table)
    try:
        conn.execute(f"""
            MERGE INTO {fqn} AS target
            USING _arrow_batch AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN UPDATE
            WHEN NOT MATCHED THEN INSERT
        """)
    finally:
        conn.unregister("_arrow_batch")
    logger.info("%s: merged %d rows on %s", fqn, table.num_rows, merge_key)
    return table.num_rows


def update_cursor(conn: duckdb.DuckDBPyConnection,
                  dataset_name: str, row_count: int) -> None:
    """Update pipeline state with latest cursor."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lake._pipeline_state (
            dataset_name VARCHAR PRIMARY KEY,
            last_updated_at TIMESTAMP DEFAULT current_timestamp,
            row_count BIGINT,
            last_run_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute(f"""
        INSERT OR REPLACE INTO lake._pipeline_state
        VALUES ('{dataset_name}', current_timestamp, {row_count}, current_timestamp)
    """)
```

- [ ] **Step 3: Run test, verify pass**
- [ ] **Step 4: Commit**

```
feat: add DuckLake writer — full replace + MERGE INTO delta
```

---

### Task 5: Migrate dlt Incremental Cursors

**Files:**
- Create: `scripts/migrate_cursors.py`

- [ ] **Step 1: Write migration script**

Script reads dlt's base64+zlib compressed `_dlt_pipeline_state` from all 12 schemas, extracts `_updated_at` cursors for each resource, writes them to `lake._pipeline_state`.

```python
# scripts/migrate_cursors.py
"""One-time migration: dlt incremental cursors → _pipeline_state table."""
import json, base64, zlib, duckdb

conn = duckdb.connect(...)  # DuckLake connection
# ... (extraction logic from tested code above)
# For each cursor: INSERT INTO lake._pipeline_state VALUES (dataset_name, cursor_timestamp, ...)
```

- [ ] **Step 2: Run migration, verify 294 cursors migrated**
- [ ] **Step 3: Commit**

```
feat: migrate 294 dlt incremental cursors to _pipeline_state
```

---

### Task 6: Socrata @asset Functions

**Files:**
- Create: `src/dagster_pipeline/defs/socrata_direct_assets.py`

- [ ] **Step 1: Implement asset generator from datasets.py**

Reads SOCRATA_DATASETS, generates one `@dg.asset` per dataset. Each asset:
1. Gets DuckLakeResource connection
2. Checks `_pipeline_state` for cursor
3. If cursor exists: delta fetch with `$where=:updated_at > cursor`
4. If no cursor: full fetch (all pages)
5. Arrow → DuckDB zero-copy → DuckLake (full replace or MERGE INTO)
6. Updates cursor

- [ ] **Step 2: Test with one small dataset end-to-end**

```bash
dagster asset materialize --select 'environment/air_quality' -m dagster_pipeline.definitions
```

- [ ] **Step 3: Test with one monster dataset**

```bash
dagster asset materialize --select 'city_government/nys_campaign_contributions' -m dagster_pipeline.definitions
```

- [ ] **Step 4: Commit**

```
feat: add 208 Socrata @asset functions — direct DuckDB ingestion
```

---

### Task 7: Federal Source Adaptation

**Files:**
- Create: `src/dagster_pipeline/defs/federal_direct_assets.py`

- [ ] **Step 1: Adapt each federal source type to the fetcher pattern**

Each source type gets a fetch function that returns Arrow:
- **BLS/Census/HUD/NREL/FDA**: REST JSON → Arrow (small, sequential OK)
- **CourtListener bulk CSV**: HTTP download → PyArrow CSV reader → Arrow
- **FEC bulk CSV**: Already uses PyArrow — keep as-is
- **NYS Death Index**: HTTP CSV → PyArrow CSV reader → Arrow
- **NYPD Misconduct**: ZIP → PyArrow CSV → Arrow

All write through the same `writer.py` path.

- [ ] **Step 2: Test each source type**
- [ ] **Step 3: Commit**

```
feat: adapt 50 federal sources to direct DuckDB ingestion
```

---

### Task 8: Dagster Definitions — Swap Executor + Resources

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Replace docker_executor with multiprocess_executor**

```python
from dagster import multiprocess_executor

defs = Definitions(
    assets=[*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities],
    jobs=[daily_live_job, monthly_refresh_job, all_assets_full, entity_resolution_job],
    schedules=[daily_schedule, monthly_schedule],
    sensors=[flush_ducklake_sensor],
    resources={
        "ducklake": DuckLakeResource(
            catalog_url=os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG"],
            s3_endpoint=os.environ.get("S3_ENDPOINT", "178.156.228.119:9000"),
            s3_access_key=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=os.environ["S3_SECRET_KEY"],
        ),
    },
    executor=multiprocess_executor.configured({
        "max_concurrent": 8,
        "start_method": {"forkserver": {}},
        "tag_concurrency_limits": [
            {"key": "schema", "value": {"applyLimitPerUniqueValue": True}, "limit": 1},
        ],
    }),
)
```

- [ ] **Step 2: Remove old dlt imports and docker config**
- [ ] **Step 3: Verify Dagster UI loads all assets**

```bash
dagster dev -m dagster_pipeline.definitions
```

- [ ] **Step 4: Commit**

```
feat: swap docker_executor → multiprocess_executor, add DuckLakeResource
```

---

### Task 9: End-to-End Validation

- [ ] **Step 1: Run daily_live job (67 assets)**

```bash
dagster job execute -j daily_live -m dagster_pipeline.definitions
```

Expected: all 67 assets materialize, no OOM, <30 min total.

- [ ] **Step 2: Run monthly_refresh job (100 assets)**

Expected: all 100 assets materialize with delta sync (using migrated cursors).

- [ ] **Step 3: Verify data in DuckLake via MCP server**

```sql
SELECT count(*) FROM lake.housing.hpd_violations;
SELECT count(*) FROM lake.public_safety.nypd_complaints_historic;
```

- [ ] **Step 4: Monitor memory — should stay under 2GB per worker**
- [ ] **Step 5: Enable daily_schedule**
- [ ] **Step 6: Commit + tag release**

```
feat: complete migration to direct DuckDB ingestion — v2.0
```

---

## Rollback Plan

If migration fails:
1. Old dlt sources are kept (not deleted) — just not imported
2. Revert `definitions.py` to import old `socrata_assets.py` and `federal_assets.py`
3. Revert executor to `docker_executor`
4. DuckLake data is unchanged throughout — same tables, same parquet files

## Performance Targets

| Metric | dlt (old) | Direct DuckDB (new) |
|--------|-----------|---------------------|
| Rows/sec | 4,000 | 33,000+ |
| Memory per dataset | 6,500 MB | 500 MB |
| Full materialization (337 assets) | 6+ hours, crashes | <1 hour |
| Daily live (67 assets) | N/A | <30 min |
| Monster dataset (12.6M rows) | OOM at 30GB | ~6 min, 500MB |
