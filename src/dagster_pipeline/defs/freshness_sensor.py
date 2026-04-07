"""Data freshness sensor — compares Socrata source row counts against DuckLake lake counts.

Runs hourly (STOPPED by default). For each Socrata dataset:
1. Fetches live count(*) from the Socrata API
2. Compares against DuckLake estimated_row_count
3. Emits a RunRequest if the dataset is stale (>5% drift OR >1000 missing rows)

Results are written to lake._pipeline_state for dashboarding.
"""
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
from dagster import (
    AssetKey,
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

logger = logging.getLogger(__name__)

SYNC_THRESHOLD = 0.05  # 5% drift triggers stale
SYNC_MIN_MISSING = 1000  # absolute row gap also triggers stale
RETRIGGER_COOLDOWN = 6 * 3600  # 6 hours — don't re-trigger while a run is likely still ingesting
MAX_RUNS_PER_TICK = 10  # cap queued runs per sensor tick to avoid flooding
CHUNK_SIZE = 20  # datasets to check per tick — keeps each tick under 60s grpc deadline
COUNT_CHECK_WORKERS = 10  # parallel HTTP threads inside one tick

DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def build_dataset_manifest() -> list[dict]:
    """Flatten SOCRATA_DATASETS into a list of dicts with schema/table/dataset_id/domain."""
    manifest = []
    for schema, datasets in SOCRATA_DATASETS.items():
        for table_name, dataset_id, domain_key in datasets:
            manifest.append({
                "schema": schema,
                "table_name": table_name,
                "dataset_id": dataset_id,
                "domain": DOMAINS.get(domain_key, DOMAINS["nyc"]),
            })
    return manifest


def check_socrata_count(
    domain: str,
    dataset_id: str,
    app_token: str | None,
    timeout: int = 5,
) -> int | None:
    """Fetch SELECT count(*) from a Socrata dataset. Returns None on error.

    Socrata returns the value under either ``count(*)`` (legacy) or ``count_1``
    (post-SoQL change). Fall back to the first value in the row if neither key
    is present.
    """
    url = f"https://{domain}/resource/{dataset_id}.json?$select=count(*)&$limit=1"
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if not data:
            return None
        row = data[0]
        val = row.get("count(*)") or row.get("count_1")
        if val is None and row:
            val = next(iter(row.values()), None)
        return int(val) if val is not None else None
    except (HTTPError, URLError, OSError, KeyError, IndexError, ValueError) as exc:
        logger.warning("count check failed for %s/%s: %s", domain, dataset_id, exc)
        return None


def compute_sync_status(lake_rows: int, source_rows: int | None) -> str:
    """Compare lake vs source count using percentage drift AND absolute gap.

    Returns 'synced', 'stale', or 'unknown'.
    A dataset is stale if source has more rows AND either:
      - drift exceeds SYNC_THRESHOLD (5%), OR
      - absolute missing rows exceed SYNC_MIN_MISSING (1000)
    """
    if source_rows is None:
        return "unknown"
    if source_rows == 0 and lake_rows == 0:
        return "synced"
    if source_rows == 0:
        return "synced"  # source reports 0 but lake has rows — trust lake
    if lake_rows == 0 and source_rows > 0:
        return "stale"
    if source_rows <= lake_rows:
        return "synced"
    missing = source_rows - lake_rows
    drift_pct = missing / lake_rows
    if drift_pct > SYNC_THRESHOLD or missing > SYNC_MIN_MISSING:
        return "stale"
    return "synced"


# ---------------------------------------------------------------------------
# Dagster sensor
# ---------------------------------------------------------------------------

@sensor(
    name="data_freshness_monitor",
    minimum_interval_seconds=60,
    asset_selection=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
)
def data_freshness_sensor(context):
    """Hourly freshness check: compare Socrata source counts vs DuckLake lake counts."""
    app_token = os.environ.get("SOURCES__SOCRATA__APP_TOKEN")

    manifest = build_dataset_manifest()

    # Load cursor: dataset_id → {"source_rows": int, "triggered_at": float|None}
    # plus a special "__index" key tracking the chunk offset across ticks
    raw_cursor = context.cursor or "{}"
    try:
        cursor = json.loads(raw_cursor)
    except (json.JSONDecodeError, TypeError):
        cursor = {}

    # Migrate old flat cursor format (dataset_id → int) to new format
    for k, v in list(cursor.items()):
        if k == "__index":
            continue
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            cursor[k] = {"source_rows": v, "triggered_at": None}

    # Chunked processing: pick CHUNK_SIZE datasets starting at __index, wrap around
    start_idx = int(cursor.get("__index", 0)) % max(len(manifest), 1)
    end_idx = start_idx + CHUNK_SIZE
    chunk = manifest[start_idx:end_idx]
    next_idx = end_idx if end_idx < len(manifest) else 0
    context.log.info(
        "Checking freshness for %d/%d datasets (offset %d-%d)",
        len(chunk), len(manifest), start_idx, start_idx + len(chunk),
    )

    # Connect to DuckLake and get lake row counts
    from dagster_pipeline.defs.name_index_asset import _connect_ducklake
    try:
        conn = _connect_ducklake()
    except Exception as exc:
        return SkipReason(f"Could not connect to DuckLake: {exc}")

    try:
        lake_rows_result = conn.execute("""
            SELECT schema_name || '.' || table_name, COALESCE(estimated_size, 0)
            FROM duckdb_tables()
            WHERE database_name = 'lake'
              AND schema_name NOT IN ('information_schema', 'pg_catalog')
        """).fetchall()
    except Exception as exc:
        conn.close()
        return SkipReason(f"Could not query lake row counts: {exc}")

    # Build lookup: "schema.table_name" → actual row count from duckdb_tables
    lake_counts_by_key: dict[str, int] = {
        row[0]: row[1]
        for row in lake_rows_result
    }

    run_requests = []
    freshness_rows = []
    new_cursor = dict(cursor)

    # Parallel-fetch source row counts for the whole chunk — serial loop blew the
    # 60s gRPC deadline because Socrata latency dominates each tick.
    def _fetch(entry):
        return entry, check_socrata_count(entry["domain"], entry["dataset_id"], app_token)

    with ThreadPoolExecutor(max_workers=COUNT_CHECK_WORKERS) as pool:
        fetched = list(pool.map(_fetch, chunk))

    for entry, source_rows in fetched:
        schema = entry["schema"]
        table_name = entry["table_name"]
        dataset_id = entry["dataset_id"]

        dataset_key = f"{schema}.{table_name}"
        lake_rows = lake_counts_by_key.get(dataset_key, 0)

        if source_rows is None:
            freshness_rows.append({
                "schema": schema,
                "table_name": table_name,
                "dataset_id": dataset_id,
                "lake_rows": lake_rows,
                "source_rows": None,
                "sync_status": "unknown",
                "checked_at": time.time(),
                "error": "count_fetch_failed",
            })
            continue

        sync_status = compute_sync_status(lake_rows, source_rows)
        is_stale = sync_status == "stale"
        prev = cursor.get(dataset_id, {})
        prev_source_rows = prev.get("source_rows") if isinstance(prev, dict) else prev
        prev_triggered_at = prev.get("triggered_at") if isinstance(prev, dict) else None

        freshness_rows.append({
            "schema": schema,
            "table_name": table_name,
            "dataset_id": dataset_id,
            "lake_rows": lake_rows,
            "source_rows": source_rows,
            "sync_status": sync_status,
            "checked_at": time.time(),
            "error": None,
        })

        if is_stale:
            source_changed = source_rows != prev_source_rows
            still_stale = prev_triggered_at is not None
            # Don't re-trigger if we fired recently — the run is likely still ingesting
            cooldown_active = (
                prev_triggered_at is not None
                and (time.time() - prev_triggered_at) < RETRIGGER_COOLDOWN
            )

            if cooldown_active and not source_changed:
                hours_ago = (time.time() - prev_triggered_at) / 3600
                context.log.info(
                    "Stale but cooling down: %s/%s — triggered %.1fh ago, skipping",
                    schema, table_name, hours_ago,
                )
                new_cursor[dataset_id] = {"source_rows": source_rows, "triggered_at": prev_triggered_at}
            elif source_changed or still_stale:
                # Include timestamp so run_key is unique per sensor tick — allows retries
                run_key = f"{dataset_id}_{source_rows}_{int(time.time())}"
                run_requests.append(
                    RunRequest(
                        run_key=run_key,
                        asset_selection=AssetSelection.assets(
                            AssetKey([schema, table_name])
                        ),
                        tags={"triggered_by": "freshness_sensor", "dataset_id": dataset_id},
                    )
                )
                drift_pct = (source_rows - lake_rows) / max(lake_rows, 1)
                context.log.info(
                    "Stale: %s/%s — lake=%d source=%d drift=%.1f%% (retry=%s)",
                    schema, table_name, lake_rows, source_rows, drift_pct * 100,
                    "yes" if still_stale and not source_changed else "no",
                )
                new_cursor[dataset_id] = {"source_rows": source_rows, "triggered_at": time.time()}
            else:
                new_cursor[dataset_id] = {"source_rows": source_rows, "triggered_at": None}
        else:
            # Synced — clear any previous triggered_at
            new_cursor[dataset_id] = {"source_rows": source_rows, "triggered_at": None}

    # Update _pipeline_state with source_rows, sync_status, and corrected row_count
    try:
        # Ensure freshness columns exist
        existing_cols = {r[0] for r in conn.execute("DESCRIBE lake._pipeline_state").fetchall()}
        for col, typ in [("source_rows", "BIGINT"), ("sync_status", "VARCHAR"), ("source_checked_at", "TIMESTAMP")]:
            if col not in existing_cols:
                try:
                    conn.execute(f"ALTER TABLE lake._pipeline_state ADD COLUMN {col} {typ}")
                    context.log.info("Added column %s to _pipeline_state", col)
                except Exception as e:
                    context.log.warning("Could not add column %s: %s", col, e)

        updated = 0
        for row in freshness_rows:
            dataset_key = f"{row['schema']}.{row['table_name']}"
            lake_rows = row["lake_rows"]  # actual count from duckdb_tables
            try:
                conn.execute("""
                    UPDATE lake._pipeline_state
                    SET source_rows = ?, sync_status = ?, source_checked_at = current_timestamp,
                        row_count = ?
                    WHERE dataset_name = ?
                """, [row["source_rows"], row["sync_status"], lake_rows, dataset_key])
                updated += 1
            except Exception:
                pass  # row may not exist yet (never ingested)

        context.log.info("Updated %d/%d rows in lake._pipeline_state with source freshness",
                         updated, len(freshness_rows))
    except Exception as exc:
        context.log.warning("Could not write freshness table: %s", exc)
    finally:
        conn.close()

    stale_count = sum(1 for r in freshness_rows if r.get("sync_status") == "stale")

    # Cap runs per tick to avoid flooding the queue
    deferred = 0
    if len(run_requests) > MAX_RUNS_PER_TICK:
        deferred = len(run_requests) - MAX_RUNS_PER_TICK
        # Clear triggered_at for deferred datasets so they get picked up next tick
        deferred_requests = run_requests[MAX_RUNS_PER_TICK:]
        for req in deferred_requests:
            dataset_id = req.tags.get("dataset_id", "")
            if dataset_id in new_cursor:
                new_cursor[dataset_id]["triggered_at"] = None
        run_requests = run_requests[:MAX_RUNS_PER_TICK]

    context.log.info(
        "Freshness check complete: %d stale, %d run requests emitted, %d deferred to next tick",
        stale_count, len(run_requests), deferred,
    )

    new_cursor["__index"] = next_idx
    return SensorResult(run_requests=run_requests, cursor=json.dumps(new_cursor))
