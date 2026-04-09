"""Dagster asset for probabilistic entity resolution via Splink.

Wraps the batch processing logic from scripts/batch_resolve_entities.py as a
first-class Dagster asset with dependency on name_index. Processes ~55M records
in ~115 batches of ~500K, writing resolved entities to DuckLake.
"""
import gc
import os
import time

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

# Import core logic from the batch script (avoid duplication)
# These are added to sys.path via PYTHONPATH=src or Docker /app/src
BATCH_SIZE = 500_000
_MODEL_V2 = "models/splink_model_v2.json"
_MODEL_V1 = "models/splink_model.json"
MODEL_PATH = _MODEL_V2 if os.path.exists(_MODEL_V2) else _MODEL_V1
PREDICT_THRESHOLD = 0.9
CLUSTER_THRESHOLD = 0.92

# Splink temp tables to clean up after each batch
_SPLINK_CLEANUP_TABLES = [
    "batch_data", "__batch_names", "batch_results",
    "__splink__df_concat", "__splink__df_predict",
    "__splink__df_clustered",
]


def _get_phonetic_counts(conn):
    """Get dm_last (phonetic) frequency distribution from name_index.

    Groups by phonetic encoding so SMITH and SMYTH are in the same batch.
    Falls back to exact last_name if dm_last column doesn't exist.
    """
    try:
        # dm_last is a VARCHAR[] (from double_metaphone). Extract the first
        # element so the group key is a plain string, not a list — otherwise
        # the batch VALUES clause fails with 'list has no attribute replace'.
        rows = conn.execute("""
            SELECT dm_last[1] AS dm_last_primary, COUNT(*) as cnt
            FROM lake.federal.name_index
            WHERE dm_last IS NOT NULL AND dm_last[1] IS NOT NULL
              AND first_name IS NOT NULL
              AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 1
            GROUP BY dm_last_primary
            ORDER BY cnt DESC
        """).fetchall()
        return [(r[0], r[1]) for r in rows], "dm_last[1]"
    except Exception:
        rows = conn.execute("""
            SELECT last_name, COUNT(*) as cnt
            FROM lake.federal.name_index
            WHERE last_name IS NOT NULL AND first_name IS NOT NULL
              AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 1
            GROUP BY last_name
            ORDER BY cnt DESC
        """).fetchall()
        return [(r[0], r[1]) for r in rows], "last_name"


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


def _process_batch(conn, group_values, group_col, batch_num, total_batches, first_batch, log):
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

    # Build a values table for the IN clause.
    # group_col may be an expression like 'dm_last[1]' (not a bare column name).
    # The batch_names table always uses a plain 'group_key' column name.
    conn.execute("DROP TABLE IF EXISTS __batch_names")
    conn.execute("CREATE TABLE __batch_names (group_key VARCHAR)")
    for i in range(0, len(group_values), 1000):
        chunk = group_values[i:i + 1000]
        values = ", ".join(f"('{n.replace(chr(39), chr(39)+chr(39))}')" for n in chunk)
        conn.execute(f"INSERT INTO __batch_names VALUES {values}")

    # Read batch data
    conn.execute(f"""
        CREATE OR REPLACE TABLE batch_data AS
        SELECT * FROM lake.federal.name_index
        WHERE {group_col} IN (SELECT group_key FROM __batch_names)
          AND last_name IS NOT NULL AND first_name IS NOT NULL
          AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 1
    """)

    batch_count = conn.execute("SELECT COUNT(*) FROM batch_data").fetchone()[0]
    t_read = time.time() - t0

    if batch_count == 0:
        log.warning("Batch %d/%d: 0 records, skipping", batch_num, total_batches)
        conn.execute("DROP TABLE IF EXISTS batch_data")
        conn.execute("DROP TABLE IF EXISTS __batch_names")
        return 0, 0

    # Detach the lake catalog before creating the Splink Linker.
    # Splink's settings validation calls information_schema.columns which
    # scans every view in every attached catalog. The lake has a broken JSON
    # view (financial.v_nys_notaries references a non-existent 'georeference'
    # column) that bombs the scan. We don't need the lake during Splink
    # processing — batch_data is already a local table. We reattach after
    # Splink is done so we can write results back.
    try:
        conn.execute("DETACH lake")
    except Exception:
        pass  # lake wasn't attached (fallback connection style)

    # Create Linker with pre-trained model
    db_api = DuckDBAPI(connection=conn)
    linker = Linker("batch_data", settings=MODEL_PATH, db_api=db_api)

    # Predict
    t_pred = time.time()
    results = linker.inference.predict(threshold_match_probability=PREDICT_THRESHOLD)
    pred_time = time.time() - t_pred

    # Persist pairwise match probabilities before clustering discards them
    predict_rel = results.as_duckdbpyrelation()
    conn.execute("""
        INSERT INTO pairwise_probabilities_staging
        SELECT unique_id_l, unique_id_r, match_probability
        FROM predict_rel
    """)

    # Cluster
    t_clust = time.time()
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        results, threshold_match_probability=CLUSTER_THRESHOLD
    )
    clust_time = time.time() - t_clust

    # Reattach lake before writing results — we detached it before Splink
    # to avoid the broken v_nys_notaries view in information_schema.
    try:
        _connect_ducklake(conn)
    except Exception:
        pass  # may already be attached if detach failed earlier

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
        name_counts, group_col = _get_phonetic_counts(conn)
        context.log.info("Grouping by %s", group_col)
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

        # Create staging table for pairwise match probabilities
        conn.execute("""
            CREATE OR REPLACE TABLE pairwise_probabilities_staging (
                unique_id_l BIGINT,
                unique_id_r BIGINT,
                match_probability DOUBLE
            )
        """)

        # Process batches
        cumulative_records = 0
        cumulative_clusters = 0
        failed_batches = 0
        first_batch = True

        for i, (last_names, expected_count) in enumerate(batches, 1):
            try:
                batch_records, batch_clusters = _process_batch(
                    conn, last_names, group_col, i, total_batches, first_batch, context.log,
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

        # Persist pairwise probabilities to DuckLake
        prob_count = conn.execute(
            "SELECT COUNT(*) FROM pairwise_probabilities_staging"
        ).fetchone()[0]
        if prob_count > 0:
            context.log.info(
                "Writing %s pairwise probabilities to DuckLake...",
                f"{prob_count:,}",
            )
            conn.execute("""
                CREATE OR REPLACE TABLE lake.federal.pairwise_probabilities AS
                SELECT * FROM pairwise_probabilities_staging
            """)
            context.log.info("Pairwise probabilities persisted.")
        else:
            context.log.warning("No pairwise probabilities captured.")
        conn.execute("DROP TABLE IF EXISTS pairwise_probabilities_staging")

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
            "DONE: %s records, %s clusters, %s multi-record matches, %s probability pairs in %.1f min",
            f"{cumulative_records:,}", f"{cumulative_clusters:,}",
            f"{multi_record:,}", f"{prob_count:,}", elapsed / 60,
        )

        return MaterializeResult(
            metadata={
                "total_records": MetadataValue.int(cumulative_records),
                "total_clusters": MetadataValue.int(cumulative_clusters),
                "multi_record_matches": MetadataValue.int(multi_record),
                "pairwise_probabilities": MetadataValue.int(prob_count),
                "failed_batches": MetadataValue.int(failed_batches),
                "duration_minutes": MetadataValue.float(elapsed / 60),
                "batches": MetadataValue.int(total_batches),
            }
        )
    finally:
        conn.close()
