"""Foundation assets — materialized columns for H3, phonetics, fingerprints.

These assets add computed columns to existing lake tables WITHOUT modifying
the source tables. Each creates a new table in the 'foundation' schema:
  - foundation.h3_index — H3 cell at res 9 for every lat/lng row
  - foundation.phonetic_index — double_metaphone for every name row
  - foundation.row_fingerprints — MurmurHash3 fingerprint per row
"""
import logging
import time

import duckdb
from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake
from dagster_pipeline.sources.datasets import LAT_LNG_TABLES
from dagster_pipeline.sources.name_registry import NAME_REGISTRY

logger = logging.getLogger(__name__)

H3_RESOLUTION = 9  # ~100m hexagons


@asset(
    key=AssetKey(["foundation", "h3_index"]),
    group_name="foundation",
    description="H3 hex cell index (res 9, ~100m) for all lat/lng tables in the lake.",
    compute_kind="duckdb",
)
def h3_index(context) -> MaterializeResult:
    """Materialize H3 spatial index across all lat/lng tables."""
    conn = _connect_ducklake()
    t_start = time.time()

    try:
        conn.execute("INSTALL h3 FROM community; LOAD h3")

        unions = []
        skipped = []
        for schema, table, lat_col, lng_col in LAT_LNG_TABLES:
            try:
                conn.execute(f"""
                    SELECT {lat_col}, {lng_col}
                    FROM lake.{schema}.{table} LIMIT 1
                """)
                unions.append(f"""
                    SELECT
                        '{schema}.{table}' AS source_table,
                        h3_latlng_to_cell(
                            TRY_CAST({lat_col} AS DOUBLE),
                            TRY_CAST({lng_col} AS DOUBLE),
                            {H3_RESOLUTION}
                        ) AS h3_res9,
                        TRY_CAST({lat_col} AS DOUBLE) AS lat,
                        TRY_CAST({lng_col} AS DOUBLE) AS lng,
                        ROW_NUMBER() OVER () AS source_rowid
                    FROM lake.{schema}.{table}
                    WHERE TRY_CAST({lat_col} AS DOUBLE) IS NOT NULL
                      AND TRY_CAST({lng_col} AS DOUBLE) IS NOT NULL
                      AND TRY_CAST({lat_col} AS DOUBLE) BETWEEN 40.4 AND 41.0
                      AND TRY_CAST({lng_col} AS DOUBLE) BETWEEN -74.3 AND -73.6
                """)
                context.log.info("Added %s.%s (%s, %s)", schema, table, lat_col, lng_col)
            except Exception as e:
                skipped.append(f"{schema}.{table}")
                context.log.warning("Skipped %s.%s: %s", schema, table, e)

        if not unions:
            raise RuntimeError("No lat/lng tables found — check LAT_LNG_TABLES")

        full_sql = " UNION ALL ".join(unions)
        context.log.info("Building H3 index from %d tables (%d skipped)...",
                         len(unions), len(skipped))

        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute(f"""
            CREATE OR REPLACE TABLE lake.foundation.h3_index AS
            SELECT * FROM ({full_sql})
            WHERE h3_res9 IS NOT NULL
        """)

        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.h3_index"
        ).fetchone()[0]
        table_count = conn.execute(
            "SELECT COUNT(DISTINCT source_table) FROM lake.foundation.h3_index"
        ).fetchone()[0]

        elapsed = time.time() - t_start
        context.log.info("H3 index: %s rows from %d tables in %.1fs",
                         f"{row_count:,}", table_count, elapsed)

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "table_count": MetadataValue.int(table_count),
                "skipped_tables": MetadataValue.text(", ".join(skipped) if skipped else "none"),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@asset(
    key=AssetKey(["foundation", "phonetic_index"]),
    group_name="foundation",
    deps=[AssetKey(["federal", "name_index"])],
    description="Double metaphone phonetic encoding for all names in name_index.",
    compute_kind="duckdb",
)
def phonetic_index(context) -> MaterializeResult:
    """Add phonetic columns to the name index for fast blocking."""
    conn = _connect_ducklake()
    t_start = time.time()

    try:
        conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")

        context.log.info("Building phonetic index from name_index...")
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("""
            CREATE OR REPLACE TABLE lake.foundation.phonetic_index AS
            SELECT
                unique_id,
                last_name,
                first_name,
                source_table,
                double_metaphone(UPPER(last_name)) AS dm_last,
                double_metaphone(UPPER(first_name)) AS dm_first,
                soundex(UPPER(last_name)) AS sx_last,
                soundex(UPPER(first_name)) AS sx_first
            FROM lake.federal.name_index
            WHERE last_name IS NOT NULL AND first_name IS NOT NULL
        """)

        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.phonetic_index"
        ).fetchone()[0]

        dm_distinct = conn.execute(
            "SELECT COUNT(DISTINCT dm_last) FROM lake.foundation.phonetic_index"
        ).fetchone()[0]

        elapsed = time.time() - t_start
        context.log.info("Phonetic index: %s rows, %s distinct dm_last in %.1fs",
                         f"{row_count:,}", f"{dm_distinct:,}", elapsed)

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "distinct_phonetic_lastnames": MetadataValue.int(dm_distinct),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()


@asset(
    key=AssetKey(["foundation", "row_fingerprints"]),
    group_name="foundation",
    description="MurmurHash3 fingerprints for dedup detection across high-volume tables.",
    compute_kind="duckdb",
)
def row_fingerprints(context) -> MaterializeResult:
    """Hash key columns of high-volume tables for dedup detection."""
    conn = _connect_ducklake()
    t_start = time.time()

    DEDUP_TABLES = [
        ("housing", "hpd_violations", "violationid"),
        ("housing", "hpd_complaints", "complaint_id"),
        ("housing", "dob_ecb_violations", "ecb_violation_number"),
        ("social_services", "n311_service_requests", "unique_key"),
        ("public_safety", "nypd_complaints_historic", "cmplnt_num"),
        ("public_safety", "nypd_arrests_historic", "arrest_key"),
        ("health", "restaurant_inspections", "camis"),
    ]

    try:
        conn.execute("INSTALL hashfuncs FROM community; LOAD hashfuncs")
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

        unions = []
        for schema, table, key_col in DEDUP_TABLES:
            try:
                conn.execute(f"SELECT {key_col} FROM lake.{schema}.{table} LIMIT 1")
                unions.append(f"""
                    SELECT
                        '{schema}.{table}' AS source_table,
                        CAST({key_col} AS VARCHAR) AS record_key,
                        murmurhash3_128(CAST({key_col} AS VARCHAR))::VARCHAR AS fingerprint
                    FROM lake.{schema}.{table}
                    WHERE {key_col} IS NOT NULL
                """)
            except Exception as e:
                context.log.warning("Skipped %s.%s: %s", schema, table, e)

        if unions:
            conn.execute(f"""
                CREATE OR REPLACE TABLE lake.foundation.row_fingerprints AS
                {' UNION ALL '.join(unions)}
            """)
            row_count = conn.execute(
                "SELECT COUNT(*) FROM lake.foundation.row_fingerprints"
            ).fetchone()[0]
        else:
            row_count = 0

        elapsed = time.time() - t_start
        context.log.info("Row fingerprints: %s rows in %.1fs", f"{row_count:,}", elapsed)

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "tables_fingerprinted": MetadataValue.int(len(unions)),
                "duration_seconds": MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
