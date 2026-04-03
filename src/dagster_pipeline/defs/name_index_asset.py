"""Dagster asset for the unified name index — extracts person names from all 47
lake tables and materializes a cross-reference index to DuckLake.

This is a SQL transformation asset (plain @asset, not @dlt_assets) that:
1. Connects to DuckLake via DuckDB
2. Runs UNION ALL across all name-bearing tables (53 queries from name_registry)
3. Filters to names appearing in 2+ source tables (cross-references)
4. Persists as lake.federal.name_index
"""
import logging
import re

import duckdb
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.sources.name_registry import NAME_REGISTRY, get_extraction_sql

logger = logging.getLogger(__name__)


def _connect_ducklake() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection attached to the DuckLake catalog."""
    import os

    from dagster_pipeline.resources.ducklake import DuckLakeResource
    resource = DuckLakeResource(
        catalog_url=os.environ.get("DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG", ""),
    )
    return resource.get_connection()


def _read_ducklake_creds() -> dict:
    """Return Postgres catalog credentials parsed from the catalog env var."""
    import os
    import re

    catalog = os.environ.get(
        "DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG",
        "ducklake:postgres:dbname=ducklake user=dagster password=test host=178.156.228.119 port=5432",
    )
    # Strip ducklake:postgres: prefix if present
    raw = re.sub(r"^ducklake:postgres:", "", catalog).strip()
    pairs = dict(kv.split("=", 1) for kv in raw.split() if "=" in kv)
    return {
        "pg_db": pairs.get("dbname", "ducklake"),
        "pg_user": pairs.get("user", "dagster"),
        "pg_pass": pairs.get("password", ""),
        "pg_host": pairs.get("host", "localhost"),
        "pg_port": pairs.get("port", "5432"),
    }


def _build_union_sql(conn=None) -> tuple[str, int]:
    """Generate the full UNION ALL SQL from the name registry.
    Skips tables that don't exist yet. Returns (sql, skipped_count).
    """
    # Get existing tables if connection provided
    existing_tables = set()
    if conn:
        try:
            rows = conn.execute(
                "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake'"
            ).fetchall()
            existing_tables = {(s, t) for s, t in rows}
        except Exception:
            pass

    unions = []
    skipped = 0
    for source in NAME_REGISTRY:
        if existing_tables and (source.schema, source.table) not in existing_tables:
            skipped += 1
            continue
        sqls = get_extraction_sql(source)
        unions.extend(sqls)

    if not unions:
        return "SELECT NULL AS last_name, NULL AS first_name, NULL AS source_table WHERE false", skipped
    return " UNION ALL ".join(unions), skipped


@asset(
    key=AssetKey(["federal", "name_index"]),
    group_name="federal",
    description="Unified name index: person names from 47 lake tables, filtered to cross-references (2+ source tables).",
    compute_kind="duckdb",
)
def name_index(context) -> MaterializeResult:
    """Materialize the name index to lake.federal.name_index."""
    conn = _connect_ducklake()

    try:
        conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
        has_phonetic = True
    except Exception:
        has_phonetic = False

    try:
        union_sql, skipped = _build_union_sql(conn)
        query_count = union_sql.count("UNION ALL") + 1 if "UNION ALL" in union_sql else 0
        context.log.info("Built UNION ALL from %d sources (%d SQL queries, %d skipped — not yet loaded)",
                         len(NAME_REGISTRY), query_count, skipped)

        # Step 1: Raw index (all rows with valid last_name)
        context.log.info("Step 1: Creating raw name index...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE lake.federal.name_index_raw AS
            SELECT * FROM (
                {union_sql}
            )
            WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
        """)
        raw_count = conn.execute(
            "SELECT COUNT(*) FROM lake.federal.name_index_raw"
        ).fetchone()[0]
        context.log.info("Raw name index: %s rows", f"{raw_count:,}")

        # Step 2: Filtered index (names in 2+ source tables)
        context.log.info("Step 2: Filtering to cross-references (2+ tables)...")
        if has_phonetic:
            conn.execute("""
                CREATE OR REPLACE TABLE lake.federal.name_index AS
                SELECT ROW_NUMBER() OVER () AS unique_id, r.*,
                       double_metaphone(UPPER(r.last_name)) AS dm_last,
                       double_metaphone(UPPER(r.first_name)) AS dm_first
                FROM lake.federal.name_index_raw r
                WHERE (r.last_name, r.first_name) IN (
                    SELECT last_name, first_name
                    FROM lake.federal.name_index_raw
                    WHERE first_name IS NOT NULL AND LENGTH(first_name) >= 2
                    GROUP BY last_name, first_name
                    HAVING COUNT(DISTINCT source_table) >= 2
                )
            """)
        else:
            conn.execute("""
                CREATE OR REPLACE TABLE lake.federal.name_index AS
                SELECT ROW_NUMBER() OVER () AS unique_id, r.*
                FROM lake.federal.name_index_raw r
                WHERE (r.last_name, r.first_name) IN (
                    SELECT last_name, first_name
                    FROM lake.federal.name_index_raw
                    WHERE first_name IS NOT NULL AND LENGTH(first_name) >= 2
                    GROUP BY last_name, first_name
                    HAVING COUNT(DISTINCT source_table) >= 2
                )
            """)
        filtered_count = conn.execute(
            "SELECT COUNT(*) FROM lake.federal.name_index"
        ).fetchone()[0]
        context.log.info("Filtered name index: %s rows", f"{filtered_count:,}")

        # Step 3: Count contributing source tables
        source_tables = conn.execute(
            "SELECT COUNT(DISTINCT source_table) FROM lake.federal.name_index"
        ).fetchone()[0]
        context.log.info("Contributing source tables: %d", source_tables)

        # Step 4: Drop raw table
        conn.execute("DROP TABLE IF EXISTS lake.federal.name_index_raw")
        context.log.info("Dropped raw table")

        return MaterializeResult(
            metadata={
                "raw_count": MetadataValue.int(raw_count),
                "filtered_count": MetadataValue.int(filtered_count),
                "source_tables": MetadataValue.int(source_tables),
            }
        )
    finally:
        conn.close()
