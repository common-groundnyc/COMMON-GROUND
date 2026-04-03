"""Train Splink model v2 with phonetic blocking + address comparisons.

Run inside Docker:
    docker exec common-ground-duckdb-server-1 python /app/scripts/train_splink_model.py
"""
import duckdb
import json
import os
import time

from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink import comparison_library as cl


def main():
    conn = duckdb.connect()
    conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")

    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "")

    if not pg_pass:
        raise RuntimeError("No DuckLake credentials — run inside Docker")

    conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres")
    conn.execute(f"ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres port=5432' AS lake (METADATA_SCHEMA 'lake')")

    print("Sampling 1M rows for training...", flush=True)
    conn.execute("""
        CREATE TABLE training_data AS
        SELECT *,
            double_metaphone(UPPER(last_name)) AS dm_last,
            double_metaphone(UPPER(first_name)) AS dm_first
        FROM lake.federal.name_index
        USING SAMPLE 1000000
    """)
    sample_count = conn.execute("SELECT COUNT(*) FROM training_data").fetchone()[0]
    print(f"Training sample: {sample_count:,} rows", flush=True)

    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        blocking_rules_to_generate_predictions=[
            block_on("dm_last", "dm_first"),
            block_on("dm_last", "zip"),
            block_on("last_name", "city"),
        ],
        comparisons=[
            cl.NameComparison("first_name"),
            cl.NameComparison("last_name"),
            cl.ExactMatch("address").configure(term_frequency_adjustments=True),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("zip").configure(term_frequency_adjustments=True),
        ],
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=False,
        max_iterations=15,
        em_convergence=0.0001,
    )

    db_api = DuckDBAPI(connection=conn)
    linker = Linker("training_data", settings, db_api=db_api)

    print("Training model (EM algorithm)...", flush=True)
    t0 = time.time()

    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

    for rule in [block_on("last_name", "first_name"), block_on("last_name", "zip")]:
        linker.training.estimate_parameters_using_expectation_maximisation(rule)

    elapsed = time.time() - t0
    print(f"Training complete in {elapsed:.1f}s", flush=True)

    linker.misc.save_model_to_json("models/splink_model_v2.json", overwrite=True)
    print("Model saved to models/splink_model_v2.json", flush=True)

    # Quick validation
    print("\nValidation — predicting on 10K sample...", flush=True)
    conn.execute("CREATE TABLE validation_data AS SELECT * FROM training_data LIMIT 10000")
    val_linker = Linker("validation_data", "models/splink_model_v2.json", db_api=db_api)
    results = val_linker.inference.predict(threshold_match_probability=0.9)
    clusters = val_linker.clustering.cluster_pairwise_predictions_at_threshold(
        results, threshold_match_probability=0.92)
    clusters_rel = clusters.as_duckdbpyrelation()
    conn.execute("CREATE TABLE val_clusters AS SELECT * FROM clusters_rel")
    multi = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT cluster_id FROM val_clusters GROUP BY cluster_id HAVING COUNT(*) >= 2
        )
    """).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM val_clusters").fetchone()[0]
    print(f"Validation: {total:,} records, {multi:,} multi-record clusters", flush=True)

    conn.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
