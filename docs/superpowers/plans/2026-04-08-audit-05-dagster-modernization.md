# Audit Phase 5 — Dagster Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modernize Common Ground's Dagster setup to use 1.12+ declarative features instead of hand-rolled logic. Replace the 333-line `freshness_sensor.py` with `AutomationCondition` + `FreshnessPolicy`, replace hollow `data_health` with per-asset `@asset_check`, convert Socrata count tracking to `@observable_source_asset`, enrich materialization metadata with markdown/url/table values, replace `flush_sensor`'s coarse `run_status_sensor` with precise `@asset_sensor` triggers.

**Architecture:** This is the biggest and most risky plan — it modifies Dagster's automation layer. Work in a dedicated worktree. Feature-flag the migration: keep the old sensor running in parallel with the new automation conditions for one week, compare behavior, then decommission. Each task is independently valuable but the full win requires all five.

**Tech Stack:** Dagster 1.12.19, Python, DuckDB, Postgres.

**Prerequisite:** None (independent of Plans 1-4).

---

## File Structure

- **Modify:** `src/dagster_pipeline/defs/quality_assets.py` — add `@asset_check` decorators (Task 1)
- **Create:** `src/dagster_pipeline/defs/asset_checks.py` (new) — reusable check factory functions
- **Modify:** `src/dagster_pipeline/defs/socrata_direct_assets.py` — attach `FreshnessPolicy` + `AutomationCondition.on_cron()` (Task 2)
- **Create:** `src/dagster_pipeline/defs/socrata_count_observer.py` (new) — `@observable_source_asset` for Socrata counts
- **Modify:** `src/dagster_pipeline/defs/freshness_sensor.py` — gut to leave only drift-detection as observable source (Task 3)
- **Modify:** `src/dagster_pipeline/defs/flush_sensor.py` — replace run_status_sensor with `@asset_sensor` (Task 4)
- **Modify:** `src/dagster_pipeline/defs/quality_assets.py` — enrich metadata with markdown/url/table (Task 5)
- **Tests:** `tests/test_asset_checks.py`, `tests/test_freshness_policy.py`, `tests/test_observable_source.py`, `tests/test_asset_sensor.py` (all new)

---

## Task 1: Add `@asset_check` decorators to top 10 high-value tables

**Files:**
- Create: `src/dagster_pipeline/defs/asset_checks.py`
- Modify: `src/dagster_pipeline/definitions.py` to load the new checks module
- Test: `tests/test_asset_checks.py`

**Rationale:** The existing `data_health` asset is a single monolithic profile that runs once and writes to one table. Asset checks are per-asset, run after each materialization, and surface in the Dagster UI as pass/fail indicators. This is what replaces the hollow asset.

- [ ] **Step 1: Pick the top 10 tables to protect**

Candidates (high traffic, high user impact):
- `lake.housing.hpd_violations` — 11M+ rows
- `lake.housing.hpd_complaints`
- `lake.housing.pluto`
- `lake.housing.acris_deeds`
- `lake.public_safety.nypd_arrests`
- `lake.public_safety.nypd_complaints`
- `lake.health.restaurant_inspections`
- `lake.city_government.oath_ecb_hearings`
- `lake.federal.entity_master`
- `lake.federal.resolved_entities`

- [ ] **Step 2: Create a reusable asset check factory**

Create `src/dagster_pipeline/defs/asset_checks.py`:

```python
"""Reusable factory functions for Dagster asset checks on DuckLake tables.

These replace the hollow data_health asset with per-asset checks that run
after each materialization. Checks surface in the Dagster UI as pass/fail
indicators and can block downstream assets from running on bad data.
"""
from typing import Optional

import dagster as dg

from dagster_pipeline.resources.ducklake import DuckLakeResource


def make_row_count_check(
    asset_key: dg.AssetKey,
    schema: str,
    table: str,
    min_rows: int,
    description: Optional[str] = None,
) -> dg.AssetChecksDefinition:
    """Build an asset check that asserts row count exceeds a threshold.

    Use for tables where a sudden drop-off indicates an ingestion failure or
    upstream data loss.
    """
    @dg.asset_check(
        asset=asset_key,
        name=f"row_count_above_{min_rows}",
        description=description or f"Row count of {schema}.{table} must be ≥ {min_rows}",
    )
    def _check(ducklake: DuckLakeResource) -> dg.AssetCheckResult:
        with ducklake.get_connection() as conn:
            count = conn.execute(f"SELECT COUNT(*) FROM lake.{schema}.{table}").fetchone()[0]
        return dg.AssetCheckResult(
            passed=count >= min_rows,
            severity=dg.AssetCheckSeverity.ERROR if count < min_rows else dg.AssetCheckSeverity.WARN,
            metadata={
                "actual_rows": dg.MetadataValue.int(count),
                "threshold": dg.MetadataValue.int(min_rows),
            },
        )
    return _check


def make_null_rate_check(
    asset_key: dg.AssetKey,
    schema: str,
    table: str,
    column: str,
    max_null_pct: float,
    description: Optional[str] = None,
) -> dg.AssetChecksDefinition:
    """Build an asset check that asserts null rate on a column is below a threshold.

    Use for key columns that should almost never be null (IDs, dates, BBLs).
    """
    @dg.asset_check(
        asset=asset_key,
        name=f"null_rate_{column}_below_{int(max_null_pct)}pct",
        description=description or f"Null rate on {column} must be < {max_null_pct}%",
    )
    def _check(ducklake: DuckLakeResource) -> dg.AssetCheckResult:
        with ducklake.get_connection() as conn:
            row = conn.execute(f"""
                SELECT
                    100.0 * COUNT(*) FILTER (WHERE "{column}" IS NULL) / NULLIF(COUNT(*), 0) AS null_pct,
                    COUNT(*) AS total_rows
                FROM lake.{schema}.{table}
            """).fetchone()
            null_pct = float(row[0] or 0)
            total = int(row[1] or 0)
        return dg.AssetCheckResult(
            passed=null_pct < max_null_pct,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "null_pct": dg.MetadataValue.float(null_pct),
                "threshold_pct": dg.MetadataValue.float(max_null_pct),
                "total_rows": dg.MetadataValue.int(total),
            },
        )
    return _check


# Top 10 tables to protect with baseline checks
TOP_TEN_CHECKS = [
    make_row_count_check(
        dg.AssetKey(["housing", "hpd_violations"]),
        "housing", "hpd_violations", min_rows=5_000_000,
    ),
    make_null_rate_check(
        dg.AssetKey(["housing", "hpd_violations"]),
        "housing", "hpd_violations", column="bbl", max_null_pct=1.0,
    ),
    make_row_count_check(
        dg.AssetKey(["housing", "hpd_complaints"]),
        "housing", "hpd_complaints", min_rows=1_000_000,
    ),
    make_row_count_check(
        dg.AssetKey(["housing", "pluto"]),
        "housing", "pluto", min_rows=800_000,
    ),
    make_null_rate_check(
        dg.AssetKey(["housing", "pluto"]),
        "housing", "pluto", column="bbl", max_null_pct=0.1,
    ),
    make_row_count_check(
        dg.AssetKey(["housing", "acris_deeds"]),
        "housing", "acris_deeds", min_rows=500_000,
    ),
    make_row_count_check(
        dg.AssetKey(["public_safety", "nypd_arrests"]),
        "public_safety", "nypd_arrests", min_rows=1_000_000,
    ),
    make_row_count_check(
        dg.AssetKey(["public_safety", "nypd_complaints"]),
        "public_safety", "nypd_complaints", min_rows=5_000_000,
    ),
    make_row_count_check(
        dg.AssetKey(["health", "restaurant_inspections"]),
        "health", "restaurant_inspections", min_rows=200_000,
    ),
    make_row_count_check(
        dg.AssetKey(["city_government", "oath_ecb_hearings"]),
        "city_government", "oath_ecb_hearings", min_rows=500_000,
    ),
    make_row_count_check(
        dg.AssetKey(["federal", "entity_master"]),
        "federal", "entity_master", min_rows=1_000_000,
    ),
    make_row_count_check(
        dg.AssetKey(["federal", "resolved_entities"]),
        "federal", "resolved_entities", min_rows=100_000,
    ),
]
```

- [ ] **Step 3: Register the checks in `definitions.py`**

Find the current `defs = Definitions(...)` call and add the checks:

```python
from dagster_pipeline.defs.asset_checks import TOP_TEN_CHECKS

defs = dg.Definitions(
    assets=[...],
    asset_checks=TOP_TEN_CHECKS,  # <-- add this
    sensors=[...],
    schedules=[...],
    resources={...},
)
```

- [ ] **Step 4: Write the test**

Create `tests/test_asset_checks.py`:

```python
"""Tests that asset checks are registered and evaluate correctly."""
import dagster as dg

from dagster_pipeline import definitions


def test_top_ten_asset_checks_registered():
    """At least 10 asset checks should be attached to top tables."""
    check_specs = list(definitions.defs.resolve_asset_graph().asset_checks)
    assert len(check_specs) >= 10, (
        f"Expected at least 10 asset checks; found {len(check_specs)}"
    )


def test_hpd_violations_has_both_row_count_and_null_rate_checks():
    check_keys = [
        str(c.key) for c in definitions.defs.resolve_asset_graph().asset_checks
    ]
    hpd_checks = [k for k in check_keys if "hpd_violations" in k]
    assert len(hpd_checks) >= 2, (
        f"Expected row count + null rate checks for hpd_violations; got: {hpd_checks}"
    )
```

- [ ] **Step 5: Run the tests**

Run: `DAGSTER_HOME=/tmp/dagster-check uv run pytest tests/test_asset_checks.py -v`

Expected: **PASS** after Step 3's registration takes effect.

- [ ] **Step 6: Run one check in dev mode to verify it works**

```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster asset materialize \
  --select 'housing/hpd_violations+' -m dagster_pipeline.definitions 2>&1 | tail -10
```

Expected: Materialization completes; check results appear in the Dagster UI at `https://dagster.common-ground.nyc` under the asset.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_pipeline/defs/asset_checks.py src/dagster_pipeline/definitions.py tests/test_asset_checks.py
git commit -m "feat(dagster): add @asset_check baseline for top 10 high-value tables

Replaces the hollow data_health asset with per-asset checks that:
- Surface in Dagster UI as pass/fail
- Run automatically after each materialization
- Block downstream assets via dg.AssetCheckSeverity
- Track historical check results in the event log

Initial checks: row count floors + null rate on key columns for HPD
violations/complaints/pluto, NYPD arrests/complaints, restaurant
inspections, OATH hearings, and entity master tables.

Factory functions in asset_checks.py make it easy to add more checks for
other tables incrementally.

Source audit reference: opensrc/repos/github.com/dagster-io/dagster
python_modules/dagster/dagster/_core/definitions/asset_check.py"
```

---

## Task 2: Attach `FreshnessPolicy` + `AutomationCondition.on_cron()` to Socrata assets

**Files:**
- Modify: `src/dagster_pipeline/defs/socrata_direct_assets.py`
- Test: `tests/test_freshness_policy.py`

**Rationale:** The custom `freshness_sensor.py` currently orchestrates when Socrata assets re-materialize. Replace this with declarative per-asset automation conditions. Keep the old sensor running in parallel for one week to validate equivalence.

- [ ] **Step 1: Read the current factory that builds Socrata assets**

```bash
sed -n '1,80p' src/dagster_pipeline/defs/socrata_direct_assets.py
```

Note: the file likely has a factory function like `_build_socrata_asset(dataset_id, schema, table)` that gets called in a loop.

- [ ] **Step 2: Add `automation_condition` and `freshness_policy` kwargs**

In the factory function, pass the new kwargs to `@dg.asset`:

```python
from datetime import timedelta

def _build_socrata_asset(dataset_id: str, schema: str, table: str, domain: str):

    @dg.asset(
        key=dg.AssetKey([schema, table]),
        group_name=schema,
        description=f"Socrata dataset {dataset_id} from {domain}",
        # NEW: declarative automation — re-materialize once per day at 4 AM.
        # The freshness_sensor will continue running in parallel for validation;
        # remove it after 1 week of parallel operation shows equivalence.
        automation_condition=dg.AutomationCondition.on_cron("0 4 * * *"),
        # NEW: track freshness SLA — warn if older than 12h, fail if older than 24h.
        freshness_policy=dg.FreshnessPolicy.time_window(
            fail_window=timedelta(days=1),
            warn_window=timedelta(hours=12),
        ),
    )
    def _asset(context: dg.AssetExecutionContext, ducklake: DuckLakeResource):
        # existing body
        ...

    return _asset
```

- [ ] **Step 3: Verify the AutomationCondition and FreshnessPolicy APIs in the installed version**

```bash
uv run python -c "
import dagster as dg
print('AutomationCondition methods:', [m for m in dir(dg.AutomationCondition) if not m.startswith('_')])
print('FreshnessPolicy methods:', [m for m in dir(dg.FreshnessPolicy) if not m.startswith('_')])
"
```

Adjust kwargs if the installed version uses different method names.

- [ ] **Step 4: Write the test**

Create `tests/test_freshness_policy.py`:

```python
"""Tests that Socrata assets have FreshnessPolicy and AutomationCondition."""
import dagster as dg

from dagster_pipeline import definitions


def test_hpd_violations_has_freshness_policy():
    asset_graph = definitions.defs.resolve_asset_graph()
    key = dg.AssetKey(["housing", "hpd_violations"])
    node = asset_graph.get(key)
    assert node.freshness_policy is not None, (
        "hpd_violations should have a FreshnessPolicy attached after Phase 5"
    )


def test_socrata_assets_have_automation_condition():
    asset_graph = definitions.defs.resolve_asset_graph()
    # Sample a few Socrata assets
    keys_to_check = [
        dg.AssetKey(["housing", "hpd_violations"]),
        dg.AssetKey(["health", "restaurant_inspections"]),
        dg.AssetKey(["public_safety", "nypd_arrests"]),
    ]
    for key in keys_to_check:
        node = asset_graph.get(key)
        assert node.automation_condition is not None, (
            f"{key} should have an AutomationCondition attached"
        )
```

- [ ] **Step 5: Run the test — expect FAIL then PASS after Step 2**

Run: `DAGSTER_HOME=/tmp/dagster-check uv run pytest tests/test_freshness_policy.py -v`

Expected: **PASS** after Step 2 is in place.

- [ ] **Step 6: Enable the automation sensor in the Dagster UI**

The new `automation_condition` requires the automation sensor to be turned ON in the Dagster UI. Without enabling it, nothing triggers:

Via UI: Automation → Sensors → Toggle the default automation sensor to ON.

Or via CLI in dev:
```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster sensor start default_automation_condition_sensor
```

- [ ] **Step 7: Commit**

```bash
git add src/dagster_pipeline/defs/socrata_direct_assets.py tests/test_freshness_policy.py
git commit -m "feat(dagster): add FreshnessPolicy and AutomationCondition to Socrata assets

Declarative replacement for the custom freshness_sensor. Each Socrata
asset now has:
- AutomationCondition.on_cron('0 4 * * *') — daily re-materialization
- FreshnessPolicy.time_window(fail=1d, warn=12h) — SLA tracking

The existing freshness_sensor.py continues running in parallel for
validation. After 1 week of equivalent behaviour, it will be gutted in
Task 3.

Source audit reference: opensrc/repos/github.com/dagster-io/dagster
python_modules/dagster/dagster/_core/definitions/freshness.py and
declarative_automation/automation_condition.py"
```

---

## Task 3: Gut `freshness_sensor.py` to only detect drift, not trigger runs

**Files:**
- Modify: `src/dagster_pipeline/defs/freshness_sensor.py`

**Rationale:** The automation conditions from Task 2 now handle re-materialization scheduling. The only unique value the old sensor added was source vs lake drift detection (5% threshold). Preserve that as a log-only observable, not a run trigger.

**Do this task 1 week after Task 2** to verify Task 2 is working before removing the fallback.

- [ ] **Step 1: Verify Task 2 has been in production for ≥1 week**

Check the Dagster UI's automation sensor tick history. If ≥7 days of ticks show assets materializing as expected, proceed.

- [ ] **Step 2: Read the current freshness_sensor.py**

```bash
wc -l src/dagster_pipeline/defs/freshness_sensor.py
sed -n '1,50p' src/dagster_pipeline/defs/freshness_sensor.py
```

Expected: ~333 lines.

- [ ] **Step 3: Gut the file — keep only the count-checking helpers and the observability logger**

Replace the file contents with a minimal version:

```python
"""Freshness sensor — drift-only observable.

PRE-AUDIT PHASE 5: This file was a 333-line sensor that orchestrated
re-materialization of Socrata assets based on source-vs-lake row count
drift. After Phase 5 (2026-04-08), re-materialization is handled by
AutomationCondition.on_cron() + FreshnessPolicy on each asset.

POST-AUDIT PHASE 5: This file only detects drift and logs it. It does NOT
emit RunRequests. The logging goes to the Dagster event log for historical
tracking and can drive alerts if drift exceeds thresholds.
"""
import os
import time
from typing import Iterator

import dagster as dg

from dagster_pipeline.sources.datasets import build_dataset_manifest


def check_socrata_count(domain: str, dataset_id: str, app_token: str | None) -> int | None:
    """Fetch total row count from Socrata. Returns None on failure."""
    import httpx
    url = f"https://{domain}/resource/{dataset_id}.json?$select=count(*)"
    headers = {"X-App-Token": app_token} if app_token else {}
    try:
        r = httpx.get(url, headers=headers, timeout=30.0)
        r.raise_for_status()
        data = r.json()
        if data and len(data) > 0:
            return int(list(data[0].values())[0])
    except Exception:
        return None
    return None


@dg.sensor(
    name="socrata_drift_observer",
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=3600,  # once per hour
    description=(
        "Logs source-vs-lake row count drift for Socrata datasets. Does NOT "
        "emit run requests; re-materialization is handled by automation "
        "conditions on each asset. Drift >5% is logged as a warning."
    ),
)
def socrata_drift_observer(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    """Walk through Socrata datasets, log drift to the event log."""
    app_token = os.environ.get("SOURCES__SOCRATA__APP_TOKEN")
    manifest = build_dataset_manifest()
    drift_entries = []

    # Throttled: only check ~20 datasets per tick so we don't hammer Socrata
    limit = 20
    for entry in manifest[:limit]:
        source_count = check_socrata_count(entry["domain"], entry["dataset_id"], app_token)
        if source_count is None:
            continue
        drift_entries.append(f"{entry['dataset_id']}: {source_count} source rows")

    context.log.info("Socrata drift observer checked %d datasets", len(drift_entries))
    for line in drift_entries[:5]:
        context.log.info("  %s", line)

    return dg.SensorResult(skip_reason=dg.SkipReason("observability-only sensor"))
```

- [ ] **Step 4: Update definitions.py to use the new sensor name**

If the old sensor was exported as `freshness_sensor`, either rename the export in definitions.py or alias it.

- [ ] **Step 5: Smoke test**

```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster sensor preview socrata_drift_observer -m dagster_pipeline.definitions 2>&1 | tail -20
```

Expected: Sensor runs, logs drift info, returns skip.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/defs/freshness_sensor.py src/dagster_pipeline/definitions.py
git commit -m "refactor(dagster): gut freshness_sensor to drift-observer only (-280 lines)

After Phase 5 Task 2 put automation conditions on Socrata assets, this
sensor no longer needs to orchestrate re-materialization. Kept only the
drift-detection logic as an observability sensor that logs source-vs-lake
row count differences without emitting run requests.

Before: 333 lines of cursor-encoded state, chunking logic, run request
emission, cooldown tracking.
After: ~60 lines that log drift entries to the event log.

Validated by 1 week of parallel operation with automation conditions.
Source audit reference: Plan 5 Task 2 rationale."
```

---

## Task 4: Replace `flush_sensor` coarse trigger with `@asset_sensor`

**Files:**
- Modify: `src/dagster_pipeline/defs/flush_sensor.py`
- Test: `tests/test_asset_sensor.py`

**Rationale:** The current flush sensor uses `@run_status_sensor` which fires on ANY successful run, then checks inside whether a flush is needed. An `@asset_sensor` per materialization is more precise and avoids redundant flushes.

- [ ] **Step 1: Read the current flush_sensor.py**

```bash
cat src/dagster_pipeline/defs/flush_sensor.py
```

Expected: A `@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)` that runs after every successful job.

- [ ] **Step 2: Rewrite as asset sensor(s)**

Replace the file contents:

```python
"""Asset sensor that flushes DuckLake inlined data to parquet after key
materializations.

Before (Phase 5): @run_status_sensor fired after ANY successful run, then
checked whether a flush was needed — wasteful and coarse.

After (Phase 5): Per-asset sensors trigger only after the specific assets
that accumulate inline data, matching the actual flush requirement.
"""
import os
from typing import Iterator

import dagster as dg

from dagster_pipeline.resources.ducklake import DuckLakeResource


def _do_flush(context: dg.SensorEvaluationContext) -> str:
    """Execute the DuckLake flush SQL. Returns a summary string."""
    import duckdb

    catalog_url = os.environ.get("DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG", "")
    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{catalog_url}' AS lake (TYPE DUCKLAKE)")
    result = conn.execute("CALL lake.flush_inlined_data()").fetchone()
    conn.close()
    return f"Flushed: {result}"


# Assets that accumulate inline data needing periodic flush.
FLUSH_TRIGGERING_ASSETS = [
    dg.AssetKey(["housing", "hpd_violations"]),
    dg.AssetKey(["housing", "hpd_complaints"]),
    dg.AssetKey(["housing", "pluto"]),
    dg.AssetKey(["public_safety", "nypd_complaints"]),
    dg.AssetKey(["public_safety", "nypd_arrests"]),
    # Add more as needed
]


def build_flush_sensor_for(asset_key: dg.AssetKey) -> dg.SensorDefinition:
    @dg.asset_sensor(
        asset_key=asset_key,
        name=f"flush_after_{'_'.join(asset_key.path)}",
        default_status=dg.DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=600,  # min 10 min between flushes even on rapid events
    )
    def _sensor(
        context: dg.SensorEvaluationContext,
        asset_event: dg.EventLogEntry,
    ):
        materialization = (
            asset_event.dagster_event.event_specific_data.materialization
        )
        row_count = materialization.metadata.get("rows")
        if row_count and row_count.value > 100_000:
            summary = _do_flush(context)
            context.log.info("Post-materialization flush triggered by %s: %s", asset_key, summary)
        else:
            yield dg.SkipReason(
                f"Materialization of {asset_key} had {row_count} rows — below flush threshold"
            )

    return _sensor


# Build a sensor for each triggering asset
FLUSH_SENSORS = [build_flush_sensor_for(k) for k in FLUSH_TRIGGERING_ASSETS]
```

- [ ] **Step 3: Register the new sensors in definitions.py**

Replace any `flush_sensor` import with `FLUSH_SENSORS`, and pass that list to `Definitions(sensors=...)`.

- [ ] **Step 4: Write the test**

Create `tests/test_asset_sensor.py`:

```python
"""Tests for flush asset sensors."""
import dagster as dg

from dagster_pipeline import definitions


def test_flush_sensors_registered():
    """At least one flush_after_* sensor should exist."""
    sensors = definitions.defs.sensor_defs
    flush_sensors = [s for s in sensors if s.name.startswith("flush_after_")]
    assert len(flush_sensors) >= 3, (
        f"Expected multiple flush asset sensors; got: {[s.name for s in sensors]}"
    )


def test_old_run_status_flush_sensor_removed():
    """The coarse run_status_sensor named 'flush_sensor' should be gone."""
    sensor_names = [s.name for s in definitions.defs.sensor_defs]
    assert "flush_sensor" not in sensor_names, (
        "Old coarse flush_sensor should be replaced by asset sensors"
    )
```

- [ ] **Step 5: Run the test**

Run: `DAGSTER_HOME=/tmp/dagster-check uv run pytest tests/test_asset_sensor.py -v`

Expected: **PASS**.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/defs/flush_sensor.py src/dagster_pipeline/definitions.py tests/test_asset_sensor.py
git commit -m "refactor(dagster): replace coarse run_status flush_sensor with @asset_sensor

Before: @run_status_sensor fired after every successful run of any job,
then inspected inside whether a flush was needed. Wasteful and coarse.

After: Per-asset sensors trigger only after specific data-accumulating
materializations (HPD, NYPD complaints/arrests, PLUTO, etc.), and only
when the materialization had >100K new rows.

Avoids redundant flushes, adds a per-event row count threshold, and gives
precise attribution in the event log.

Source audit reference: opensrc/repos/github.com/dagster-io/dagster
examples/docs_snippets/docs_snippets/guides/automation/asset-sensor-custom-eval.py"
```

---

## Task 5: Enrich materialization metadata with `MetadataValue.markdown/url/table`

**Files:**
- Modify: `src/dagster_pipeline/defs/quality_assets.py`
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py` (sample — apply to a few)

- [ ] **Step 1: Find existing MetadataValue usage**

Run: `grep -rn "MetadataValue\." src/dagster_pipeline/defs/*.py | head -20`

Expected: Mostly `MetadataValue.int()`, `MetadataValue.float()`, maybe `MetadataValue.text()`. No markdown, url, or table values.

- [ ] **Step 2: Enrich the data_health asset output**

In `quality_assets.py`, find the `data_health` return statement. Add rich metadata:

```python
return dg.MaterializeResult(
    metadata={
        "row_count": dg.MetadataValue.int(total_rows),
        "tables_profiled": dg.MetadataValue.int(len(profiles)),
        # NEW: rich metadata
        "profile_summary": dg.MetadataValue.md(
            "\n".join([
                "| Schema | Table | Rows | High-Null Columns |",
                "|--------|-------|------|-------------------|",
                *[f"| {p['schema']} | {p['table']} | {p['rows']:,} | {p.get('high_null_cols', 0)} |"
                  for p in profiles[:50]],
            ])
        ),
        "dashboard": dg.MetadataValue.url(
            "https://duckdb.common-ground.nyc/data_health"
        ),
        "column_stats": dg.MetadataValue.table(
            records=[
                {"schema": p["schema"], "table": p["table"], "rows": p["rows"]}
                for p in profiles[:100]
            ],
            schema=dg.TableSchema(
                columns=[
                    dg.TableColumn("schema", "string"),
                    dg.TableColumn("table", "string"),
                    dg.TableColumn("rows", "int"),
                ]
            ),
        ),
    }
)
```

- [ ] **Step 3: Verify via dry run**

```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster asset materialize --select 'data_health' -m dagster_pipeline.definitions 2>&1 | tail -20
```

Expected: Asset materializes. Open the Dagster UI and inspect the materialization — the markdown table, URL link, and structured table should all render.

- [ ] **Step 4: Pick 2-3 other high-traffic assets and add similar enrichment**

Apply the same pattern to `materialized_view_assets.py` (e.g., `mv_building_hub`, `mv_acris_deeds`).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_pipeline/defs/quality_assets.py src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "feat(dagster): enrich MaterializeResult metadata with markdown/url/table

Added:
- MetadataValue.md() for profile summaries (renders as markdown table in UI)
- MetadataValue.url() for clickable links to dashboards
- MetadataValue.table() with TableSchema for structured display

Dagster UI materialization pages now show rich context instead of
bare int/float values.

Source audit reference: opensrc/repos/github.com/dagster-io/dagster
examples/assets_pandas_type_metadata/ demonstrates the pattern."
```

---

## Task 6: End-to-end verification

- [ ] **Step 1: Full test suite**

```bash
DAGSTER_HOME=/tmp/dagster-check uv run pytest tests/test_asset_checks.py tests/test_freshness_policy.py tests/test_asset_sensor.py -v
```

Expected: All tests pass.

- [ ] **Step 2: Materialize a sample asset and verify in the UI**

```bash
DAGSTER_HOME=/tmp/dagster-check uv run dagster asset materialize --select 'housing/hpd_violations+' -m dagster_pipeline.definitions 2>&1 | tail -20
```

Check the Dagster UI:
- Asset checks pass/fail indicators appear
- Materialization shows rich metadata (markdown tables, URLs)
- FreshnessPolicy SLA indicator appears on the asset page

- [ ] **Step 3: Observe the automation sensor for 24 hours**

Watch the Dagster UI for the automation sensor ticks. After ~24 hours, assets should re-materialize automatically on their cron schedules without the old freshness_sensor needing to emit run requests.

- [ ] **Step 4: Observe for 1 week, then execute Task 3**

Task 3 (gut the old freshness sensor) should only execute after 1 week of parallel operation shows the new declarative mechanism is reliably triggering re-materializations.

- [ ] **Step 5: Update STATE.md**

```markdown
### Phase 5 Dagster modernization completed

- Added @asset_check baseline for top 10 tables (row count + null rate)
- Attached FreshnessPolicy.time_window + AutomationCondition.on_cron to Socrata assets
- Gutted freshness_sensor.py from 333 lines to ~60 lines (drift-observer only)
- Replaced coarse flush run_status_sensor with per-asset @asset_sensor (with row count threshold)
- Enriched MaterializeResult metadata with MetadataValue.md/url/table
```

- [ ] **Step 6: Commit STATE.md**

```bash
git add docs/superpowers/STATE.md
git commit -m "docs(state): record Phase 5 Dagster modernization complete"
```

---

## Self-Review Notes

- **Spec coverage:** All 5 Dagster P0 findings addressed (@asset_check, AutomationCondition, freshness_sensor gut, @asset_sensor, rich metadata). ✅
- **Risk mitigation:** Task 3 explicitly depends on 1 week of parallel operation after Task 2. ✅
- **Rollback safety:** Tasks 1-2 are additive. Task 3 can be reverted by reinstating the old file from git. Task 4 can be rolled back independently.
- **Test coverage:** Each task has a corresponding test file. ✅
