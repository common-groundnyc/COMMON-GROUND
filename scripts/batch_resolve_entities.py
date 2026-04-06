"""Batch-process all name index records through Splink predict+cluster.

Reads 55.5M records from lake.federal.name_index in ~111 batches of ~500K,
grouped by last_name (safe because both blocking rules require last_name match).
Writes resolved entities to lake.federal.resolved_entities.

Usage:
    cd ~/Desktop/dagster-pipeline
    python scripts/batch_resolve_entities.py           # full run
    python scripts/batch_resolve_entities.py --dry-run  # test first 3 batches
"""
import argparse
import gc
import logging
import sys
import time

sys.path.insert(0, "src")

import duckdb
from splink import DuckDBAPI, Linker

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500_000
MODEL_PATH = "models/splink_model.json"
PREDICT_THRESHOLD = 0.8
CLUSTER_THRESHOLD = 0.85


def get_last_name_counts(conn):
    """Get last_name frequency distribution from name_index."""
    rows = conn.execute("""
        SELECT last_name, COUNT(*) as cnt
        FROM lake.federal.name_index
        WHERE last_name IS NOT NULL AND first_name IS NOT NULL
          AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
        GROUP BY last_name
        ORDER BY cnt DESC
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def pack_batches(name_counts):
    """Pack last names into batches of ~BATCH_SIZE using greedy bin-packing."""
    batches = []
    current_batch = []
    current_count = 0

    for last_name, cnt in name_counts:
        if current_count + cnt > BATCH_SIZE and current_batch:
            batches.append((current_batch, current_count))
            current_batch = []
            current_count = 0
        current_batch.append(last_name)
        current_count += cnt

    if current_batch:
        batches.append((current_batch, current_count))

    return batches


def process_batch(conn, last_names, batch_num, total_batches, first_batch):
    """Process a single batch: read records, predict, cluster, save to batch_results.

    Returns (batch_count, cluster_count). Saves clustered output to the
    'all_results' table (created on first batch, appended thereafter).
    """
    t0 = time.time()

    # Build a values table for the IN clause (handles large lists)
    conn.execute("DROP TABLE IF EXISTS __batch_names")
    conn.execute("CREATE TABLE __batch_names (last_name VARCHAR)")
    # Insert in chunks of 1000
    for i in range(0, len(last_names), 1000):
        chunk = last_names[i:i + 1000]
        values = ", ".join(f"('{n.replace(chr(39), chr(39)+chr(39))}')" for n in chunk)
        conn.execute(f"INSERT INTO __batch_names VALUES {values}")

    # Read batch data into local table
    conn.execute("""
        CREATE OR REPLACE TABLE batch_data AS
        SELECT * FROM lake.federal.name_index
        WHERE last_name IN (SELECT last_name FROM __batch_names)
          AND last_name IS NOT NULL AND first_name IS NOT NULL
          AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
    """)

    batch_count = conn.execute("SELECT COUNT(*) FROM batch_data").fetchone()[0]
    t_read = time.time() - t0

    if batch_count == 0:
        logger.warning("Batch %d/%d: 0 records, skipping", batch_num, total_batches)
        conn.execute("DROP TABLE IF EXISTS batch_data")
        conn.execute("DROP TABLE IF EXISTS __batch_names")
        return 0, 0

    # Create Linker with pre-trained model
    db_api = DuckDBAPI(connection=conn)
    linker = Linker("batch_data", settings=MODEL_PATH, db_api=db_api)

    # Predict
    t_pred = time.time()
    results = linker.inference.predict(threshold_match_probability=PREDICT_THRESHOLD)
    pred_time = time.time() - t_pred

    # Cluster
    t_clust = time.time()
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        results, threshold_match_probability=CLUSTER_THRESHOLD
    )
    clust_time = time.time() - t_clust

    # Save results to batch_results table, then append to all_results
    clusters_rel = clusters.as_duckdbpyrelation()
    conn.execute("CREATE OR REPLACE TABLE batch_results AS SELECT * FROM clusters_rel")
    cluster_count = conn.execute(
        "SELECT COUNT(DISTINCT cluster_id) FROM batch_results"
    ).fetchone()[0]

    if first_batch:
        conn.execute("CREATE TABLE all_results AS SELECT * FROM batch_results")
    else:
        conn.execute("INSERT INTO all_results SELECT * FROM batch_results")
    conn.execute("DROP TABLE IF EXISTS batch_results")

    total_time = time.time() - t0
    logger.info(
        "Batch %d/%d: %s records, %s clusters | read=%.1fs pred=%.1fs clust=%.1fs total=%.1fs",
        batch_num, total_batches,
        f"{batch_count:,}", f"{cluster_count:,}",
        t_read, pred_time, clust_time, total_time,
    )

    return batch_count, cluster_count


def main():
    parser = argparse.ArgumentParser(description="Batch resolve entities via Splink")
    parser.add_argument("--dry-run", action="store_true", help="Process only first 3 batches, skip lake write")
    args = parser.parse_args()

    t_start = time.time()

    logger.info("Connecting to DuckLake...")
    conn = _connect_ducklake()

    # Get last_name frequency distribution
    logger.info("Getting last_name frequency distribution...")
    name_counts = get_last_name_counts(conn)
    total_names = len(name_counts)
    total_records = sum(cnt for _, cnt in name_counts)
    logger.info("Found %s distinct last names, %s total records",
                f"{total_names:,}", f"{total_records:,}")

    # Pack into batches
    batches = pack_batches(name_counts)
    total_batches = len(batches)
    logger.info("Packed into %d batches (target size: %s)", total_batches, f"{BATCH_SIZE:,}")

    if args.dry_run:
        batches = batches[:3]
        total_batches_to_run = 3
        logger.info("DRY RUN: processing only first 3 batches")
    else:
        total_batches_to_run = total_batches

    # Process batches
    cumulative_records = 0
    cumulative_clusters = 0
    failed_batches = []
    first_batch = True

    for i, (last_names, expected_count) in enumerate(batches, 1):
        try:
            batch_records, batch_clusters = process_batch(
                conn, last_names, i, total_batches_to_run, first_batch
            )

            if batch_records > 0:
                first_batch = False

            cumulative_records += batch_records
            cumulative_clusters += batch_clusters

        except Exception as e:
            logger.error("Batch %d/%d FAILED: %s", i, total_batches_to_run, e)
            failed_batches.append((i, last_names[:3], str(e)))

        # Clean up batch tables
        for tbl in ["batch_data", "__batch_names", "batch_results",
                     "__splink__df_concat", "__splink__df_predict",
                     "__splink__df_clustered"]:
            conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        # Also drop any splink temp views/tables
        try:
            tables = conn.execute("SHOW TABLES").fetchall()
            for (tbl,) in tables:
                if tbl.startswith("__splink__"):
                    conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                    conn.execute(f"DROP VIEW IF EXISTS {tbl}")
        except Exception:
            pass
        gc.collect()

        # Progress update every 10 batches
        if i % 10 == 0:
            elapsed = time.time() - t_start
            rate = cumulative_records / elapsed if elapsed > 0 else 0
            logger.info(
                "Progress: %d/%d batches, %s records, %.0f rec/sec, elapsed %.0fs",
                i, total_batches_to_run, f"{cumulative_records:,}", rate, elapsed,
            )

    # Write to DuckLake (skip in dry-run)
    if not args.dry_run and not first_batch:
        logger.info("Writing resolved_entities to DuckLake...")
        t_write = time.time()
        conn.execute("""
            CREATE OR REPLACE TABLE lake.federal.resolved_entities AS
            SELECT * FROM all_results
        """)
        write_time = time.time() - t_write
        logger.info("DuckLake write completed in %.1fs", write_time)

    # Final metrics
    elapsed = time.time() - t_start
    rate = cumulative_records / elapsed if elapsed > 0 else 0

    # Count multi-record clusters
    multi_record = 0
    if not first_batch:
        multi_record = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT cluster_id FROM all_results
                GROUP BY cluster_id HAVING COUNT(*) >= 2
            )
        """).fetchone()[0]

    logger.info("=" * 70)
    logger.info("BATCH PROCESSING SUMMARY")
    logger.info("=" * 70)
    logger.info("Mode:                 %s", "DRY RUN" if args.dry_run else "FULL RUN")
    logger.info("Total batches:        %d / %d", total_batches_to_run - len(failed_batches), total_batches_to_run)
    logger.info("Total records:        %s", f"{cumulative_records:,}")
    logger.info("Total clusters:       %s", f"{cumulative_clusters:,}")
    logger.info("Multi-record matches: %s", f"{multi_record:,}")
    logger.info("Elapsed time:         %.1fs (%.1f min)", elapsed, elapsed / 60)
    logger.info("Throughput:           %.0f records/sec", rate)
    if failed_batches:
        logger.info("Failed batches:       %d", len(failed_batches))
        for batch_num, sample_names, err in failed_batches:
            logger.info("  Batch %d (e.g. %s): %s", batch_num, sample_names, err)
    logger.info("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
