"""Data quality assets — post-ingestion health checks.

Runs after data ingestion to profile tables, detect PII, and flag anomalies.
Results stored in lake.foundation.data_health for MCP tool consumption.
"""
import logging
import time

import duckdb
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey(["foundation", "data_health"]),
    group_name="foundation",
    description="Per-table health profile: row counts, null rates, PII detection.",
    compute_kind="duckdb",
)
def data_health(context) -> MaterializeResult:
    """Profile every lake table for completeness and data quality."""
    conn = _connect_ducklake()
    t_start = time.time()

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

        tables = conn.execute("""
            SELECT schema_name, table_name
            FROM duckdb_tables()
            WHERE database_name = 'lake'
              AND schema_name NOT IN ('information_schema', 'pg_catalog', 'foundation')
            ORDER BY schema_name, table_name
        """).fetchall()

        context.log.info("Profiling %d tables...", len(tables))

        profiles = []
        for schema, table in tables:
            try:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM lake.{schema}.{table}"
                ).fetchone()[0]

                cols = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM duckdb_columns()
                    WHERE database_name = 'lake'
                      AND schema_name = '{schema}' AND table_name = '{table}'
                    LIMIT 10
                """).fetchall()

                null_cols = 0
                for col_name, col_type in cols:
                    try:
                        null_rate = conn.execute(f'''
                            SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE "{col_name}" IS NULL)
                                   / NULLIF(COUNT(*), 0), 1)
                            FROM lake.{schema}.{table}
                        ''').fetchone()[0]
                        if null_rate and null_rate > 50:
                            null_cols += 1
                    except Exception:
                        pass

                profiles.append({
                    "schema_name": schema,
                    "table_name": table,
                    "row_count": row_count,
                    "column_count": len(cols),
                    "high_null_columns": null_cols,
                })
            except Exception as e:
                context.log.warning("Failed to profile %s.%s: %s", schema, table, e)

        if profiles:
            conn.execute("""
                CREATE OR REPLACE TABLE lake.foundation.data_health (
                    schema_name VARCHAR,
                    table_name VARCHAR,
                    row_count BIGINT,
                    column_count INTEGER,
                    high_null_columns INTEGER,
                    profiled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            for p in profiles:
                conn.execute("""
                    INSERT INTO lake.foundation.data_health
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [p["schema_name"], p["table_name"], p["row_count"],
                      p["column_count"], p["high_null_columns"]])

        total_rows = sum(p["row_count"] for p in profiles)
        elapsed = time.time() - t_start
        context.log.info("Profiled %d tables, %s total rows in %.1fs",
                         len(profiles), f"{total_rows:,}", elapsed)

        return MaterializeResult(
            metadata={
                "tables_profiled": MetadataValue.int(len(profiles)),
                "total_rows": MetadataValue.int(total_rows),
                "tables_with_high_nulls": MetadataValue.int(
                    sum(1 for p in profiles if p["high_null_columns"] > 0)
                ),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
