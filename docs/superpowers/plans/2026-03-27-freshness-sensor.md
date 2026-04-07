# Data Freshness Sensor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Dagster sensor that polls Socrata endpoints hourly, compares source row counts vs lake row counts, writes results to `data_health`, and triggers asset materializations when source data has grown significantly.

**Architecture:** A `@sensor` polls `$select=count(*)` on each Socrata dataset every hour (~260 requests, well within the 1,000/hr rate limit). It compares source counts against lake counts, writes the diff to `lake.foundation.data_health` (extending the existing table with `source_rows`, `source_checked_at`, `sync_status` columns), and yields `RunRequest` for any dataset where source has >5% more rows. The cursor persists `{dataset_id: last_source_count}` between ticks.

**Tech Stack:** Dagster 1.12 sensors, Socrata SODA API, DuckDB/DuckLake, Python 3.12

---

## What Changes

| Before | After |
|--------|-------|
| Website fetches source row counts client-side (slow, 260 requests per page load) | Server precomputes source counts hourly, website reads from `data_health` |
| `data_health` has: schema, table, row_count, column_count, null_cols | Adds: `source_rows`, `source_checked_at`, `sync_status`, `dataset_id` |
| No auto-materialization on data changes | Sensor triggers materialization when source has >5% more rows |
| SOURCE / SYNC / UPDATED columns show `—` | Shows real values from precomputed data |

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `src/dagster_pipeline/defs/freshness_sensor.py` | `@sensor` that polls Socrata, compares counts, writes to data_health, triggers runs |
| `tests/test_freshness_sensor.py` | Unit tests for the count-checking and diff logic |

### Modified files

| File | Change |
|------|--------|
| `src/dagster_pipeline/defs/quality_assets.py` | Extend `data_health` table schema with source columns |
| `src/dagster_pipeline/definitions.py` | Register the new sensor |

---

## Task 1: Build the freshness checker (pure functions)

**Files:**
- Create: `src/dagster_pipeline/defs/freshness_sensor.py`
- Create: `tests/test_freshness_sensor.py`

- [ ] **Step 1: Research — Socrata count API and error handling**

Search with Exa:
1. `socrata "$select=count(*)" response format json 2026` — exact response shape
2. `socrata API "404" "dataset not found" response` — how to handle removed datasets

Socrata returns: `[{"count": "1234567"}]` for `$select=count(*)`.

- [ ] **Step 2: Write failing tests for pure functions**

```python
# tests/test_freshness_sensor.py
import pytest
from dagster_pipeline.defs.freshness_sensor import (
    check_socrata_count,
    compute_sync_status,
    build_dataset_manifest,
    SYNC_THRESHOLD,
)

class TestBuildDatasetManifest:
    def test_returns_list_of_dicts(self):
        manifest = build_dataset_manifest()
        assert isinstance(manifest, list)
        assert len(manifest) > 200  # ~260 datasets

    def test_each_entry_has_required_fields(self):
        manifest = build_dataset_manifest()
        for entry in manifest[:5]:
            assert "table_name" in entry
            assert "dataset_id" in entry
            assert "domain" in entry
            assert "schema" in entry

    def test_includes_hpd_violations(self):
        manifest = build_dataset_manifest()
        tables = {e["table_name"] for e in manifest}
        assert "hpd_violations" in tables


class TestComputeSyncStatus:
    def test_synced_when_equal(self):
        assert compute_sync_status(1000, 1000) == "synced"

    def test_synced_when_within_threshold(self):
        assert compute_sync_status(1000, 1040) == "synced"  # 4% diff < 5%

    def test_stale_when_source_much_larger(self):
        assert compute_sync_status(1000, 1100) == "stale"  # 10% diff > 5%

    def test_stale_when_lake_is_zero(self):
        assert compute_sync_status(0, 1000) == "stale"

    def test_synced_when_both_zero(self):
        assert compute_sync_status(0, 0) == "synced"

    def test_synced_when_source_unknown(self):
        assert compute_sync_status(1000, None) == "unknown"

    def test_over_synced_when_lake_larger(self):
        """Lake has more rows than source — possible but shouldn't trigger."""
        assert compute_sync_status(1100, 1000) == "synced"
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cd ~/Desktop/dagster-pipeline
uv run python -m pytest tests/test_freshness_sensor.py -v
```

- [ ] **Step 4: Implement pure functions**

```python
# src/dagster_pipeline/defs/freshness_sensor.py
"""Data freshness sensor — polls Socrata endpoints hourly.

Checks source row counts vs lake row counts, writes diff to data_health,
and triggers materializations when sources have significantly more data.
"""
import json
import logging
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

logger = logging.getLogger(__name__)

SYNC_THRESHOLD = 0.05  # 5% growth triggers materialization

DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}


def build_dataset_manifest() -> list[dict]:
    """Flatten SOCRATA_DATASETS into a list of {schema, table_name, dataset_id, domain}."""
    manifest = []
    for schema, datasets in SOCRATA_DATASETS.items():
        for entry in datasets:
            table_name, dataset_id, domain_key = entry[0], entry[1], entry[2]
            manifest.append({
                "schema": schema,
                "table_name": table_name,
                "dataset_id": dataset_id,
                "domain": DOMAINS.get(domain_key, domain_key),
            })
    return manifest


def check_socrata_count(domain: str, dataset_id: str, app_token: str = "", timeout: int = 10) -> int | None:
    """Query Socrata $select=count(*) for a dataset. Returns row count or None on error."""
    url = f"https://{domain}/resource/{dataset_id}.json?$select=count(*)"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
            if data and isinstance(data, list) and "count" in data[0]:
                return int(data[0]["count"])
    except (URLError, HTTPError, ValueError, KeyError, IndexError) as e:
        logger.debug("Socrata count failed for %s/%s: %s", domain, dataset_id, e)
    return None


def compute_sync_status(lake_rows: int, source_rows: int | None) -> str:
    """Compare lake vs source row counts. Returns 'synced', 'stale', or 'unknown'."""
    if source_rows is None:
        return "unknown"
    if source_rows == 0 and lake_rows == 0:
        return "synced"
    if lake_rows == 0 and source_rows > 0:
        return "stale"
    if source_rows <= lake_rows:
        return "synced"
    diff_pct = (source_rows - lake_rows) / max(lake_rows, 1)
    return "stale" if diff_pct > SYNC_THRESHOLD else "synced"
```

- [ ] **Step 5: Run tests**

```bash
uv run python -m pytest tests/test_freshness_sensor.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/defs/freshness_sensor.py tests/test_freshness_sensor.py
git commit -m "feat: add freshness checker pure functions

build_dataset_manifest flattens SOCRATA_DATASETS.
check_socrata_count polls $select=count(*).
compute_sync_status compares lake vs source with 5% threshold."
```

---

## Task 2: Build the Dagster sensor

**Files:**
- Modify: `src/dagster_pipeline/defs/freshness_sensor.py` (add sensor function)

- [ ] **Step 1: Research — sensor asset_selection patterns**

Search with Exa:
1. `dagster sensor "AssetSelection" "RunRequest" dynamic asset trigger 2026` — dynamic asset selection in sensors
2. `dagster sensor "context.cursor" json state persist example` — cursor pattern

- [ ] **Step 2: Add the sensor function**

Append to `freshness_sensor.py`:

```python
import os

import duckdb
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from dagster_pipeline.defs.name_index_asset import _connect_ducklake, _read_ducklake_creds


@sensor(
    name="data_freshness_monitor",
    description="Polls Socrata endpoints hourly, compares source vs lake row counts, updates data_health, triggers materializations for stale datasets.",
    minimum_interval_seconds=3600,  # hourly
    asset_selection=AssetSelection.all(),
    default_status=DefaultSensorStatus.STOPPED,  # start manually until verified
)
def data_freshness_sensor(context):
    """Check all Socrata datasets for new data, update health table, trigger materializations."""
    app_token = os.environ.get("SOURCES__SOCRATA__APP_TOKEN", "")
    manifest = build_dataset_manifest()

    # Load previous state from cursor
    prev_counts = json.loads(context.cursor) if context.cursor else {}

    # Connect to DuckLake for lake row counts
    try:
        conn = _connect_ducklake()
    except Exception as e:
        return SkipReason(f"DuckLake connection failed: {e}")

    try:
        # Get lake row counts in one query
        lake_counts = {}
        try:
            rows = conn.execute("""
                SELECT table_schema, table_name, estimated_row_count
                FROM duckdb_tables()
                WHERE database_name = 'lake'
            """).fetchall()
            for schema, table, count in rows:
                lake_counts[f"{schema}.{table}"] = count or 0
        except Exception as e:
            context.log.warning("Failed to get lake counts: %s", e)

        # Poll each Socrata dataset
        results = []
        run_requests = []
        checked = 0
        stale = 0

        for entry in manifest:
            key = f"{entry['schema']}.{entry['table_name']}"
            lake_rows = lake_counts.get(key, 0)

            # Check Socrata
            source_rows = check_socrata_count(
                entry["domain"], entry["dataset_id"], app_token, timeout=10
            )
            checked += 1

            status = compute_sync_status(lake_rows, source_rows)

            results.append({
                "schema_name": entry["schema"],
                "table_name": entry["table_name"],
                "dataset_id": entry["dataset_id"],
                "lake_rows": lake_rows,
                "source_rows": source_rows,
                "sync_status": status,
            })

            # Trigger materialization if stale and source count changed since last check
            prev_count = prev_counts.get(entry["dataset_id"])
            if (
                status == "stale"
                and source_rows is not None
                and source_rows != prev_count
            ):
                stale += 1
                asset_key = AssetKey([entry["schema"], entry["table_name"]])
                run_requests.append(
                    RunRequest(
                        run_key=f"{entry['dataset_id']}_{source_rows}",
                        asset_selection=[asset_key],
                    )
                )

            # Rate limit — 10ms pause between requests
            time.sleep(0.01)

        # Write results to data_health
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lake.foundation.source_freshness (
                    schema_name VARCHAR,
                    table_name VARCHAR,
                    dataset_id VARCHAR,
                    lake_rows BIGINT,
                    source_rows BIGINT,
                    sync_status VARCHAR,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("DELETE FROM lake.foundation.source_freshness")
            for r in results:
                conn.execute("""
                    INSERT INTO lake.foundation.source_freshness
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [r["schema_name"], r["table_name"], r["dataset_id"],
                      r["lake_rows"], r["source_rows"], r["sync_status"]])
            context.log.info(
                "Freshness check: %d datasets checked, %d stale, %d materializations triggered",
                checked, stale, len(run_requests),
            )
        except Exception as e:
            context.log.warning("Failed to write freshness results: %s", e)

        # Update cursor with new source counts
        new_counts = {
            r["dataset_id"]: r["source_rows"]
            for r in results
            if r["source_rows"] is not None
        }

        if run_requests:
            return SensorResult(
                run_requests=run_requests,
                cursor=json.dumps(new_counts),
            )
        else:
            context.update_cursor(json.dumps(new_counts))
            return SkipReason(
                f"All {checked} datasets synced (no materializations needed)"
            )

    finally:
        conn.close()
```

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/defs/freshness_sensor.py
git commit -m "feat: add data_freshness_monitor sensor

Polls Socrata hourly, compares source vs lake row counts,
writes to source_freshness table, triggers materializations
for datasets with >5% growth."
```

---

## Task 3: Register sensor and add MCP endpoint

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`
- Modify: `infra/duckdb-server/mcp_server.py` (optional — add `/api/catalog` freshness data)

- [ ] **Step 1: Register the sensor in definitions.py**

Find the sensors list in `definitions.py` (search for `sensors=`). Add the import and sensor:

```python
from dagster_pipeline.defs.freshness_sensor import data_freshness_sensor
```

Add to the sensors list:
```python
sensors=[flush_ducklake_sensor, data_freshness_sensor],
```

- [ ] **Step 2: Verify the sensor loads**

```bash
cd ~/Desktop/dagster-pipeline
uv run dagster definitions validate -m dagster_pipeline.definitions
```

Expected: No errors. The sensor should appear in the definitions.

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register data_freshness_monitor sensor

Hourly Socrata polling sensor. Default status STOPPED — enable
manually in Dagster UI after verification."
```

---

## Task 4: Update website to read precomputed freshness

**Files:**
- Modify: `src/components/data-health-table.tsx` (in common-ground-website)

This is optional and can be done separately. The key change: instead of fetching `$select=count(*)` client-side for each table on page load, the website reads `source_freshness` from the `/api/catalog` endpoint which the MCP server already serves.

- [ ] **Step 1: Check if `/api/catalog` already includes freshness data**

The website fetches from `https://mcp.common-ground.nyc/api/catalog`. Check if the MCP server's catalog endpoint includes source_freshness data. If not, the freshness sensor's `source_freshness` table can be queried by the MCP server's catalog builder.

- [ ] **Step 2: Plan the website update**

The data-health-table component currently:
1. Loads the catalog from `/api/catalog`
2. For each table, fires a client-side `$select=count(*)` request to Socrata
3. Displays the result in the SOURCE column

After this change:
1. Loads the catalog from `/api/catalog` (which now includes `source_rows` and `sync_status` from the sensor)
2. Displays directly — no client-side Socrata requests needed
3. Faster page load, no CORS issues, no rate limit concerns

This is a website change, not a pipeline change. Mark as TODO for a separate session.

- [ ] **Step 3: Commit any changes**

```bash
# Only if MCP server changes were made
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: include source_freshness in /api/catalog response"
```

---

## Task 5: Test end-to-end

- [ ] **Step 1: Run the sensor locally (dry run)**

```bash
cd ~/Desktop/dagster-pipeline
DAGSTER_HOME=/tmp/dagster-test uv run python -c "
from dagster_pipeline.defs.freshness_sensor import (
    build_dataset_manifest, check_socrata_count, compute_sync_status
)

manifest = build_dataset_manifest()
print(f'{len(manifest)} datasets in manifest')

# Test one dataset
entry = next(e for e in manifest if e['table_name'] == 'hpd_violations')
count = check_socrata_count(entry['domain'], entry['dataset_id'])
print(f'HPD violations source count: {count}')

status = compute_sync_status(10_000_000, count)
print(f'Sync status (assuming 10M lake rows): {status}')
"
```

- [ ] **Step 2: Test in Dagster UI**

```bash
cd ~/Desktop/dagster-pipeline
uv run dagster dev -m dagster_pipeline.definitions
```

Navigate to Sensors in the UI. Find `data_freshness_monitor`. Click "Test" to run a single tick. Verify:
- Tick completes without errors
- Log shows "Freshness check: N datasets checked, N stale"
- `source_freshness` table populated in DuckLake

- [ ] **Step 3: Enable the sensor**

Toggle `data_freshness_monitor` to RUNNING in the Dagster UI. Wait for the first automated tick (up to 1 hour). Verify it runs and writes results.

---

## Execution Order

```
Task 1 (pure functions + tests)   ← No dependencies
  ↓
Task 2 (sensor function)          ← Imports from Task 1
  ↓
Task 3 (register in definitions)  ← Imports sensor
  ↓
Task 4 (website update)           ← Independent, can do later
  ↓
Task 5 (end-to-end test)          ← After all code
```

Tasks 1-3 are sequential. Task 4 is independent (website, separate repo). Task 5 validates everything.

---

## Verification Checklist

- [ ] `uv run python -m pytest tests/test_freshness_sensor.py -v` — all pass
- [ ] `uv run dagster definitions validate -m dagster_pipeline.definitions` — no errors
- [ ] Sensor appears in Dagster UI under Sensors tab
- [ ] Single tick test completes without errors
- [ ] `lake.foundation.source_freshness` table created with columns: schema_name, table_name, dataset_id, lake_rows, source_rows, sync_status, checked_at
- [ ] Stale datasets (>5% growth) trigger RunRequest with correct asset_selection
- [ ] Cursor persists between ticks (JSON with dataset_id → source_count)
- [ ] Socrata rate limit respected (~260 requests per tick, under 1,000/hr)
- [ ] Sensor default_status is STOPPED (manual enable after verification)
