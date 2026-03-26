"""Dagster asset for probabilistic entity resolution via Splink.

Wraps the batch processing logic from scripts/batch_resolve_entities.py as a
first-class Dagster asset with dependency on name_index. Processes ~55M records
in ~115 batches of ~500K, writing resolved entities to DuckLake.
"""
import gc
import time

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

# Import core logic from the batch script (avoid duplication)
# These are added to sys.path via PYTHONPATH=src or Docker /app/src
BATCH_SIZE = 500_000
MODEL_PATH = "models/splink_model.json"
PREDICT_THRESHOLD = 0.8
CLUSTER_THRESHOLD = 0.85

# Splink temp tables to clean up after each batch
_SPLINK_CLEANUP_TABLES = [
    "batch_data", "__batch_names", "batch_results",
    "__splink__df_concat", "__splink__df_predict",
    "__splink__df_clustered",
]


def _get_last_name_counts(conn):
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


def _pack_batches(name_counts):
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


def _process_batch(conn, last_names, batch_num, total_batches, first_batch, log):
    """Process a single batch: read records, predict, cluster, append to all_results.

    Returns (batch_count, cluster_count).
    """
    from splink import DuckDBAPI, Linker

    t0 = time.time()

    # Load phonetic UDFs for enhanced blocking
    try:
        conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
    except Exception:
        pass  # Fall back to standard blocking if unavailable

    # Build a values table for the IN clause
    conn.execute("DROP TABLE IF EXISTS __batch_names")
    conn.execute("CREATE TABLE __batch_names (last_name VARCHAR)")
    for i in range(0, len(last_names), 1000):
        chunk = last_names[i:i + 1000]
        values = ", ".join(f"('{n.replace(chr(39), chr(39)+chr(39))}')" for n in chunk)
        conn.execute(f"INSERT INTO __batch_names VALUES {values}")

    # Read batch data
    conn.execute("""
        CREATE OR REPLACE TABLE batch_data AS
        SELECT * FROM lake.federal.name_index
        WHERE last_name IN (SELECT last_name FROM __batch_names)
          AND last_name IS NOT NULL AND first_name IS NOT NULL
          AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
    """)

    batch_count = conn.execute("SELECT COUNT(*) FROM batch_data").fetchone()[0]
    t_read = time.time() - t0

    # Add phonetic columns for enhanced comparison
    try:
        conn.execute("""
            ALTER TABLE batch_data ADD COLUMN dm_last VARCHAR
        """)
        conn.execute("""
            UPDATE batch_data SET dm_last = double_metaphone(UPPER(last_name))
        """)
        conn.execute("""
            ALTER TABLE batch_data ADD COLUMN dm_first VARCHAR
        """)
        conn.execute("""
            UPDATE batch_data SET dm_first = double_metaphone(UPPER(first_name))
        """)
    except Exception:
        pass  # Non-critical enhancement

    if batch_count == 0:
        log.warning("Batch %d/%d: 0 records, skipping", batch_num, total_batches)
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

    # Save results
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
    log.info(
        "Batch %d/%d: %s records, %s clusters | read=%.1fs pred=%.1fs clust=%.1fs total=%.1fs",
        batch_num, total_batches,
        f"{batch_count:,}", f"{cluster_count:,}",
        t_read, pred_time, clust_time, total_time,
    )

    return batch_count, cluster_count


def _cleanup_batch_tables(conn):
    """Drop all temporary tables created during batch processing."""
    for tbl in _SPLINK_CLEANUP_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        for (tbl,) in tables:
            if tbl.startswith("__splink__"):
                conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                conn.execute(f"DROP VIEW IF EXISTS {tbl}")
    except Exception:
        pass


@asset(
    key=AssetKey(["federal", "resolved_entities"]),
    group_name="federal",
    deps=[AssetKey(["federal", "name_index"])],
    description="Probabilistic entity resolution across all lake tables using Splink",
    compute_kind="splink",
)
def resolved_entities(context) -> MaterializeResult:
    """Materialize resolved_entities to lake.federal.resolved_entities.

    Processes all name_index records through Splink predict+cluster in ~500K
    batches grouped by last_name. Takes ~51 min for 55M records.
    """
    t_start = time.time()

    context.log.info("Connecting to DuckLake...")
    conn = _connect_ducklake()

    try:
        # Get last_name frequency distribution
        context.log.info("Getting last_name frequency distribution...")
        name_counts = _get_last_name_counts(conn)
        total_names = len(name_counts)
        total_records = sum(cnt for _, cnt in name_counts)
        context.log.info(
            "Found %s distinct last names, %s total records",
            f"{total_names:,}", f"{total_records:,}",
        )

        # Pack into batches
        batches = _pack_batches(name_counts)
        total_batches = len(batches)
        context.log.info("Packed into %d batches (target size: %s)", total_batches, f"{BATCH_SIZE:,}")

        # Process batches
        cumulative_records = 0
        cumulative_clusters = 0
        failed_batches = 0
        first_batch = True

        for i, (last_names, expected_count) in enumerate(batches, 1):
            try:
                batch_records, batch_clusters = _process_batch(
                    conn, last_names, i, total_batches, first_batch, context.log,
                )
                if batch_records > 0:
                    first_batch = False
                cumulative_records += batch_records
                cumulative_clusters += batch_clusters
            except Exception as e:
                context.log.error("Batch %d/%d FAILED: %s", i, total_batches, e)
                failed_batches += 1

            _cleanup_batch_tables(conn)
            gc.collect()

            # Progress update every 10 batches
            if i % 10 == 0:
                elapsed = time.time() - t_start
                rate = cumulative_records / elapsed if elapsed > 0 else 0
                context.log.info(
                    "Progress: %d/%d batches, %s records, %.0f rec/sec, elapsed %.0fs",
                    i, total_batches, f"{cumulative_records:,}", rate, elapsed,
                )

        # Write to DuckLake
        if not first_batch:
            context.log.info("Writing resolved_entities to DuckLake...")
            t_write = time.time()
            conn.execute("""
                CREATE OR REPLACE TABLE lake.federal.resolved_entities AS
                SELECT * FROM all_results
            """)
            write_time = time.time() - t_write
            context.log.info("DuckLake write completed in %.1fs", write_time)

        # Final metrics
        elapsed = time.time() - t_start
        multi_record = 0
        if not first_batch:
            multi_record = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT cluster_id FROM all_results
                    GROUP BY cluster_id HAVING COUNT(*) >= 2
                )
            """).fetchone()[0]

        context.log.info(
            "DONE: %s records, %s clusters, %s multi-record matches in %.1f min",
            f"{cumulative_records:,}", f"{cumulative_clusters:,}",
            f"{multi_record:,}", elapsed / 60,
        )

        return MaterializeResult(
            metadata={
                "total_records": MetadataValue.int(cumulative_records),
                "total_clusters": MetadataValue.int(cumulative_clusters),
                "multi_record_matches": MetadataValue.int(multi_record),
                "failed_batches": MetadataValue.int(failed_batches),
                "duration_minutes": MetadataValue.float(elapsed / 60),
                "batches": MetadataValue.int(total_batches),
            }
        )
    finally:
        conn.close()
