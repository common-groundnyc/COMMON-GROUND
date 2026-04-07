# Splink Model Rebuild — Phonetic Blocking + Address Comparisons

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Splink entity resolution model with phonetic blocking rules (double_metaphone), address comparison levels, higher thresholds, and phonetic-grouped batching — so SMITH/SMYTH and JOHN/JONATHAN actually get compared and matched.

**Architecture:** Three changes: (1) new model with phonetic blocking + address comparisons, (2) name_index_asset adds dm_last/dm_first/address columns at build time, (3) resolved_entities_asset batches by dm_last instead of exact last_name and uses higher thresholds.

**Tech Stack:** Splink 4.0, DuckDB 1.5.0, splink_udfs (double_metaphone, soundex), jaro_winkler_similarity.

**Current problems:**
1. Blocking requires exact last_name — SMITH never compared to SMYTH
2. dm_last/dm_first columns added but never used in blocking
3. Batching by exact last_name prevents cross-spelling matches
4. Only name+city+zip comparisons — no address
5. predict threshold 0.8 too loose for 55M records

---

## File Structure

### Modified files

| File | What changes |
|------|-------------|
| `src/dagster_pipeline/defs/name_index_asset.py` | Add dm_last, dm_first, address columns to name_index output |
| `src/dagster_pipeline/defs/resolved_entities_asset.py` | Phonetic batching, new model path, higher thresholds |
| `src/dagster_pipeline/sources/name_registry.py` | get_extraction_sql outputs dm_last, dm_first |
| `models/splink_model.json` | New model with phonetic blocking + address comparisons |

### New files

| File | What it does |
|------|-------------|
| `scripts/train_splink_model.py` | Script to train the new model on a sample |
| `tests/test_splink_model.py` | Validates model loads and blocking rules work |

---

## Task 1: Add phonetic + address columns to name_index

The name_index currently outputs: `source_table, last_name, first_name, address, city, zip`. We need to add `dm_last` and `dm_first` at build time so they're available for blocking without recomputing per batch.

**Files:**
- Modify: `src/dagster_pipeline/defs/name_index_asset.py`

- [ ] **Step 1: Add phonetic columns to the name_index build**

In `name_index_asset.py`, after the `_connect_ducklake()` call in the `name_index` asset function, add extension loading:

```python
    try:
        conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
    except Exception:
        pass
```

Then change the Step 2 filtered index query (the `CREATE OR REPLACE TABLE lake.federal.name_index AS` block) to add phonetic columns:

```python
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
```

If `splink_udfs` failed to load, the `double_metaphone` call will fail. Wrap in try/except and fall back to the original query (without dm columns).

- [ ] **Step 2: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/defs/name_index_asset.py
git commit -m "feat: add dm_last, dm_first phonetic columns to name_index at build time"
```

---

## Task 2: Train new Splink model with phonetic blocking

**Files:**
- Create: `scripts/train_splink_model.py`
- Create: `models/splink_model_v2.json`

- [ ] **Step 1: Create training script**

Create `scripts/train_splink_model.py`:

```python
"""Train Splink model v2 with phonetic blocking + address comparisons.

Run inside Docker or with DuckLake credentials available:
    docker exec common-ground-duckdb-server-1 python /app/scripts/train_splink_model.py

Uses a 1M row sample from name_index for training (full 55M is unnecessary).
"""
import duckdb
import json
import os
import time

from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink import comparison_library as cl

def main():
    conn = duckdb.connect()

    # Load extensions
    conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")

    # Connect to DuckLake (Docker env vars)
    pg_pass = os.environ.get("DAGSTER_PG_PASSWORD", "")
    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "")

    if pg_pass:
        conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs; INSTALL postgres; LOAD postgres")
        conn.execute(f"SET s3_region='us-east-1'")
        conn.execute(f"SET s3_endpoint='minio:9000'")
        conn.execute(f"SET s3_access_key_id='{minio_user}'")
        conn.execute(f"SET s3_secret_access_key='{minio_pass}'")
        conn.execute("SET s3_use_ssl=false; SET s3_url_style='path'")
        conn.execute(f"ATTACH 'ducklake:postgres:dbname=ducklake user=dagster password={pg_pass} host=postgres port=5432' AS lake (METADATA_SCHEMA 'lake')")
        source_table = "lake.federal.name_index"
    else:
        raise RuntimeError("No DuckLake credentials — run inside Docker")

    # Sample 1M rows for training (stratified by source_table)
    print("Sampling 1M rows for training...", flush=True)
    conn.execute(f"""
        CREATE TABLE training_data AS
        SELECT *,
            double_metaphone(UPPER(last_name)) AS dm_last,
            double_metaphone(UPPER(first_name)) AS dm_first
        FROM {source_table}
        USING SAMPLE 1000000
    """)
    sample_count = conn.execute("SELECT COUNT(*) FROM training_data").fetchone()[0]
    print(f"Training sample: {sample_count:,} rows", flush=True)

    # Define model settings
    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",

        # Phonetic blocking — enables fuzzy cross-spelling matches
        blocking_rules_to_generate_predictions=[
            # Phonetic last + phonetic first (SMITH=SMYTH, JOHN=JON)
            block_on("dm_last", "dm_first"),
            # Phonetic last + exact zip (same area, similar name)
            block_on("dm_last", "zip"),
            # Exact last + exact city (catches first-name-only typos)
            block_on("last_name", "city"),
        ],

        comparisons=[
            # Name comparisons with fuzzy levels
            cl.NameComparison("first_name"),
            cl.NameComparison("last_name"),
            # Address — exact match only (many nulls)
            cl.ExactMatch("address").configure(
                term_frequency_adjustments=True
            ),
            # City — exact match with TF adjustment
            cl.ExactMatch("city").configure(
                term_frequency_adjustments=True
            ),
            # ZIP — exact match with TF adjustment
            cl.ExactMatch("zip").configure(
                term_frequency_adjustments=True
            ),
        ],

        retain_matching_columns=True,
        retain_intermediate_calculation_columns=False,
        max_iterations=15,
        em_convergence=0.0001,
    )

    # Create linker and train
    db_api = DuckDBAPI(connection=conn)
    linker = Linker("training_data", settings, db_api=db_api)

    print("Training model (EM algorithm)...", flush=True)
    t0 = time.time()

    # Estimate u probabilities from random sample
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

    # Estimate m probabilities using training blocking rules
    training_rules = [
        block_on("last_name", "first_name"),
        block_on("last_name", "zip"),
    ]
    for rule in training_rules:
        linker.training.estimate_parameters_using_expectation_maximisation(rule)

    elapsed = time.time() - t0
    print(f"Training complete in {elapsed:.1f}s", flush=True)

    # Save model
    model = linker.misc.save_model_to_json(
        "models/splink_model_v2.json", overwrite=True
    )
    print(f"Model saved to models/splink_model_v2.json", flush=True)

    # Quick validation
    print("\nValidation — predicting on 10K sample...", flush=True)
    conn.execute("CREATE TABLE validation_data AS SELECT * FROM training_data LIMIT 10000")
    val_linker = Linker("validation_data", "models/splink_model_v2.json", db_api=db_api)
    results = val_linker.inference.predict(threshold_match_probability=0.9)
    clusters = val_linker.clustering.cluster_pairwise_predictions_at_threshold(
        results, threshold_match_probability=0.92
    )
    clusters_df = clusters.as_duckdbpyrelation()
    conn.execute("CREATE TABLE val_clusters AS SELECT * FROM clusters_df")
    multi = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT cluster_id FROM val_clusters
            GROUP BY cluster_id HAVING COUNT(*) >= 2
        )
    """).fetchone()[0]
    total_clustered = conn.execute("SELECT COUNT(*) FROM val_clusters").fetchone()[0]
    print(f"Validation: {total_clustered:,} records, {multi:,} multi-record clusters", flush=True)

    conn.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/train_splink_model.py
git commit -m "feat: add Splink v2 training script with phonetic blocking + address comparisons"
```

---

## Task 3: Run training on server

- [ ] **Step 1: Copy script and run**

```bash
scp -i ~/.ssh/id_ed25519_hetzner scripts/train_splink_model.py fattie@178.156.228.119:/tmp/
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    docker cp /tmp/train_splink_model.py common-ground-duckdb-server-1:/app/scripts/train_splink_model.py
    docker exec common-ground-duckdb-server-1 python /app/scripts/train_splink_model.py
"
```

- [ ] **Step 2: Download trained model**

```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    docker cp common-ground-duckdb-server-1:/app/models/splink_model_v2.json /tmp/splink_model_v2.json
"
scp -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119:/tmp/splink_model_v2.json models/splink_model_v2.json
```

- [ ] **Step 3: Inspect the model**

```bash
uv run python -c "
import json
with open('models/splink_model_v2.json') as f:
    m = json.load(f)
print('Blocking rules:')
for r in m['blocking_rules_to_generate_predictions']:
    print(f'  {r[\"blocking_rule\"]}')
print(f'Comparisons: {len(m[\"comparisons\"])}')
for c in m['comparisons']:
    print(f'  {c[\"output_column_name\"]}: {len(c[\"comparison_levels\"])} levels')
"
```

- [ ] **Step 4: Commit model**

```bash
git add models/splink_model_v2.json
git commit -m "feat: trained Splink v2 model with phonetic blocking (1M sample)"
```

---

## Task 4: Update resolved_entities to use v2 model + phonetic batching

**Files:**
- Modify: `src/dagster_pipeline/defs/resolved_entities_asset.py`

- [ ] **Step 1: Change model path and thresholds**

```python
MODEL_PATH = "models/splink_model_v2.json"
PREDICT_THRESHOLD = 0.9    # was 0.8 — tighter for 55M records
CLUSTER_THRESHOLD = 0.92   # was 0.85 — fewer false positives
```

- [ ] **Step 2: Change _get_last_name_counts to use dm_last**

Replace `_get_last_name_counts`:

```python
def _get_phonetic_counts(conn):
    """Get dm_last (phonetic) frequency distribution from name_index.

    Groups by phonetic encoding so SMITH and SMYTH are in the same batch.
    Falls back to exact last_name if dm_last column doesn't exist.
    """
    try:
        rows = conn.execute("""
            SELECT dm_last, COUNT(*) as cnt
            FROM lake.federal.name_index
            WHERE dm_last IS NOT NULL AND first_name IS NOT NULL
              AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
            GROUP BY dm_last
            ORDER BY cnt DESC
        """).fetchall()
        return [(r[0], r[1]) for r in rows], "dm_last"
    except Exception:
        # Fallback: dm_last column doesn't exist yet
        rows = conn.execute("""
            SELECT last_name, COUNT(*) as cnt
            FROM lake.federal.name_index
            WHERE last_name IS NOT NULL AND first_name IS NOT NULL
              AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
            GROUP BY last_name
            ORDER BY cnt DESC
        """).fetchall()
        return [(r[0], r[1]) for r in rows], "last_name"
```

- [ ] **Step 3: Update _pack_batches call site**

In the `resolved_entities` function, replace:

```python
        name_counts = _get_last_name_counts(conn)
```

with:

```python
        name_counts, group_col = _get_phonetic_counts(conn)
        context.log.info("Grouping by %s", group_col)
```

- [ ] **Step 4: Update _process_batch to use the group column**

In `_process_batch`, change the batch_data query to use the group column:

```python
def _process_batch(conn, group_values, group_col, batch_num, total_batches, first_batch, log):
```

And change the batch data query:

```python
    conn.execute(f"""
        CREATE OR REPLACE TABLE batch_data AS
        SELECT * FROM lake.federal.name_index
        WHERE {group_col} IN (SELECT {group_col} FROM __batch_names)
          AND last_name IS NOT NULL AND first_name IS NOT NULL
          AND LENGTH(last_name) >= 2 AND LENGTH(first_name) >= 2
    """)
```

And the __batch_names table:

```python
    conn.execute(f"CREATE TABLE __batch_names ({group_col} VARCHAR)")
    for i in range(0, len(group_values), 1000):
        chunk = group_values[i:i + 1000]
        values = ", ".join(f"('{n.replace(chr(39), chr(39)+chr(39))}')" for n in chunk)
        conn.execute(f"INSERT INTO __batch_names VALUES {values}")
```

Remove the separate dm_last/dm_first ALTER TABLE block (lines 97-112) since phonetic columns are now in name_index already.

- [ ] **Step 5: Update the batch loop call**

```python
                batch_records, batch_clusters = _process_batch(
                    conn, last_names, group_col, i, total_batches, first_batch, context.log,
                )
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/defs/resolved_entities_asset.py
git commit -m "feat: Splink v2 — phonetic batching, higher thresholds, dm_last grouping"
```

---

## Task 5: Write validation test

**Files:**
- Create: `tests/test_splink_model.py`

- [ ] **Step 1: Create test**

```python
"""Test that Splink model v2 loads and blocking rules reference correct columns."""
import json
import pytest


def test_model_v2_loads():
    with open("models/splink_model_v2.json") as f:
        m = json.load(f)
    assert m["link_type"] == "dedupe_only"
    assert m["unique_id_column_name"] == "unique_id"


def test_model_v2_has_phonetic_blocking():
    with open("models/splink_model_v2.json") as f:
        m = json.load(f)
    rules = [r["blocking_rule"] for r in m["blocking_rules_to_generate_predictions"]]
    # At least one rule uses dm_last (phonetic)
    assert any("dm_last" in r for r in rules), f"No phonetic blocking rule found: {rules}"


def test_model_v2_has_address_comparison():
    with open("models/splink_model_v2.json") as f:
        m = json.load(f)
    comparison_cols = [c["output_column_name"] for c in m["comparisons"]]
    assert "address" in comparison_cols, f"No address comparison: {comparison_cols}"


def test_model_v2_has_five_comparisons():
    with open("models/splink_model_v2.json") as f:
        m = json.load(f)
    assert len(m["comparisons"]) >= 5, f"Expected 5+ comparisons, got {len(m['comparisons'])}"


def test_model_v2_has_name_comparisons_with_fuzzy():
    with open("models/splink_model_v2.json") as f:
        m = json.load(f)
    for comp in m["comparisons"]:
        if comp["output_column_name"] in ("first_name", "last_name"):
            levels = [l.get("sql_condition", "") for l in comp["comparison_levels"]]
            assert any("jaro_winkler" in l for l in levels), \
                f"{comp['output_column_name']} missing jaro_winkler levels"
```

- [ ] **Step 2: Run test (will fail until model is trained)**

```bash
uv run pytest tests/test_splink_model.py -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_splink_model.py
git commit -m "test: add Splink model v2 validation tests"
```

---

## Task 6: Update phonetic_index to use name_index dm columns

Since name_index now has dm_last/dm_first built in, the foundation phonetic_index asset can be simplified to just copy those columns instead of recomputing.

**Files:**
- Modify: `src/dagster_pipeline/defs/foundation_assets.py`

- [ ] **Step 1: Simplify phonetic_index to use pre-computed columns**

Replace the phonetic_index SQL:

```python
        conn.execute("""
            CREATE OR REPLACE TABLE lake.foundation.phonetic_index AS
            SELECT
                unique_id,
                last_name,
                first_name,
                source_table,
                COALESCE(dm_last, double_metaphone(UPPER(last_name))) AS dm_last,
                COALESCE(dm_first, double_metaphone(UPPER(first_name))) AS dm_first,
                soundex(UPPER(last_name)) AS sx_last,
                soundex(UPPER(first_name)) AS sx_first
            FROM lake.federal.name_index
            WHERE last_name IS NOT NULL AND first_name IS NOT NULL
        """)
```

This uses the pre-computed dm columns from name_index but falls back to computing them if they're NULL (backwards compatible with old name_index builds).

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/foundation_assets.py
git commit -m "refactor: phonetic_index uses pre-computed dm columns from name_index"
```

---

## Task 7: Deploy and run full entity resolution

- [ ] **Step 1: Deploy updated code**

```bash
scp -i ~/.ssh/id_ed25519_hetzner \
    src/dagster_pipeline/defs/name_index_asset.py \
    src/dagster_pipeline/defs/resolved_entities_asset.py \
    src/dagster_pipeline/defs/foundation_assets.py \
    models/splink_model_v2.json \
    fattie@178.156.228.119:/tmp/

ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "
    docker cp /tmp/name_index_asset.py common-ground-duckdb-server-1:/app/src/dagster_pipeline/defs/
    docker cp /tmp/resolved_entities_asset.py common-ground-duckdb-server-1:/app/src/dagster_pipeline/defs/
    docker cp /tmp/foundation_assets.py common-ground-duckdb-server-1:/app/src/dagster_pipeline/defs/
    docker cp /tmp/splink_model_v2.json common-ground-duckdb-server-1:/app/models/
"
```

- [ ] **Step 2: Rebuild name_index with phonetic columns**

Run the name_index materialization (needs Docker execution).

- [ ] **Step 3: Run entity resolution with v2 model**

Run resolved_entities materialization. Expected: ~55M records, higher precision clusters, SMITH/SMYTH matched.

- [ ] **Step 4: Rebuild phonetic_index (uses new dm columns)**

Run phonetic_index materialization.

- [ ] **Step 5: Verify improvement**

```sql
-- Check: do SMITH and SMYTH now cluster together?
SELECT cluster_id, last_name, first_name, source_table
FROM lake.federal.resolved_entities
WHERE cluster_id IN (
    SELECT cluster_id FROM lake.federal.resolved_entities
    WHERE last_name = 'SMITH'
)
AND last_name IN ('SMITH', 'SMYTH', 'SMTH')
LIMIT 20;
```

- [ ] **Step 6: Commit state**

```bash
git add docs/superpowers/STATE.md
git commit -m "docs: Splink v2 deployed — phonetic blocking + address comparisons"
```
