"""One-shot script to materialize the name index to DuckLake.

Connects to DuckLake, runs the UNION ALL from name_registry, filters to
cross-references (2+ source tables), and persists as lake.federal.name_index.

Usage: uv run python scripts/materialize_name_index.py
"""
import logging
import sys
import time

sys.path.insert(0, "src")

from dagster_pipeline.defs.name_index_asset import _connect_ducklake, _build_union_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Connecting to DuckLake...")
    conn = _connect_ducklake()

    union_sql = _build_union_sql()
    query_count = union_sql.count("UNION ALL") + 1
    logger.info("Built UNION ALL: %d queries, %d chars", query_count, len(union_sql))

    # Step 1: Raw index
    logger.info("Step 1: Creating raw name index...")
    t0 = time.time()
    conn.execute(f"""
        CREATE OR REPLACE TABLE lake.federal.name_index_raw AS
        SELECT * FROM (
            {union_sql}
        )
        WHERE last_name IS NOT NULL AND LENGTH(last_name) >= 2
    """)
    raw_count = conn.execute("SELECT COUNT(*) FROM lake.federal.name_index_raw").fetchone()[0]
    logger.info("Raw name index: %s rows (%.1fs)", f"{raw_count:,}", time.time() - t0)

    # Step 2: Filtered index
    logger.info("Step 2: Filtering to cross-references (2+ tables)...")
    t1 = time.time()
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
    filtered_count = conn.execute("SELECT COUNT(*) FROM lake.federal.name_index").fetchone()[0]
    logger.info("Filtered name index: %s rows (%.1fs)", f"{filtered_count:,}", time.time() - t1)

    # Step 3: Source table distribution
    logger.info("Source table distribution (top 10):")
    rows = conn.execute("""
        SELECT source_table, COUNT(*) as cnt
        FROM lake.federal.name_index
        GROUP BY source_table ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    for table, cnt in rows:
        logger.info("  %-35s %s", table, f"{cnt:,}")

    source_tables = conn.execute(
        "SELECT COUNT(DISTINCT source_table) FROM lake.federal.name_index"
    ).fetchone()[0]
    logger.info("Contributing source tables: %d", source_tables)

    # Step 4: Quality checks
    null_count = conn.execute("""
        SELECT COUNT(*) FROM lake.federal.name_index
        WHERE last_name IS NULL OR LENGTH(last_name) < 2
    """).fetchone()[0]
    logger.info("NULL/short last_name rows: %d", null_count)

    # Spot check
    smith_rows = conn.execute("""
        SELECT source_table, first_name, city, zip
        FROM lake.federal.name_index
        WHERE last_name = 'SMITH' AND first_name = 'JOHN'
        LIMIT 10
    """).fetchall()
    logger.info("Spot check SMITH/JOHN: %d rows", len(smith_rows))
    for r in smith_rows:
        logger.info("  %s", r)

    # Step 5: Drop raw table
    conn.execute("DROP TABLE IF EXISTS lake.federal.name_index_raw")
    logger.info("Dropped raw table")

    conn.close()
    logger.info("Done. raw_count=%s filtered_count=%s source_tables=%d",
                f"{raw_count:,}", f"{filtered_count:,}", source_tables)


if __name__ == "__main__":
    main()
