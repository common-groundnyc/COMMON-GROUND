"""Sensor that flushes DuckLake inlined data after every successful run.

DuckLake 0.4 inlines small tables into the Postgres catalog metadata.
When a different DuckDB client (Docker step container) tries to read that
inlined data, it fails with "Cannot open file ducklake_inlined_data_XXX".

This sensor runs after every completed run and flushes all inlined data
to parquet files on local NVMe, preventing the error on subsequent runs.

Remove this when DuckLake 1.0 ships (April 2026) — they're fixing inlining
with Postgres catalogs.
"""
import logging

from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

logger = logging.getLogger(__name__)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="flush_ducklake_inlined_data",
    description="Flushes DuckLake inlined data to parquet after successful runs",
    minimum_interval_seconds=120,  # don't fire more than once per 2 min
)
def flush_ducklake_sensor(context: RunStatusSensorContext):
    """Flush all inlined data to parquet after a successful run."""
    import os

    from dagster_pipeline.resources.ducklake import DuckLakeResource

    context.log.info("Flushing DuckLake inlined data after run %s", context.dagster_run.run_id)

    resource = DuckLakeResource(
        catalog_url=os.environ.get(
            "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
            "ducklake:postgres:dbname=ducklake user=dagster password=test host=178.156.228.119 port=5432",
        ),
    )
    conn = resource.get_connection()

    tables = conn.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT LIKE '%staging%'
        AND table_schema != 'information_schema'
    """).fetchall()

    flushed = 0
    for schema, table in tables:
        try:
            conn.execute(
                f"CALL ducklake_flush_inlined_data('lake', "
                f"schema_name := '{schema}', table_name := '{table}')"
            )
            flushed += 1
        except Exception:
            pass

    conn.close()
    context.log.info("Flushed inlined data for %d tables", flushed)
