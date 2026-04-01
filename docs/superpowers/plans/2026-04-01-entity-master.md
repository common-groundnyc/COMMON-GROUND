# Entity Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `lake.foundation.entity_master` — a canonical entity table with stable UUIDs, entity types, confidence scores, canonical names, and name variants that persists across Dagster rematerializations.

**Architecture:** A new Dagster asset downstream of `resolved_entities` that: (1) extracts per-cluster metadata (canonical name, type, confidence, sources), (2) generates deterministic content-addressed UUIDs via `md5(sorted_member_hashes)::UUID`, (3) persists match probabilities from the Splink predict step (currently discarded), and (4) is wired into the 3 direct `cluster_id` consumers in the MCP server. The entity_master table replaces cluster_id as the stable identity layer.

**Tech Stack:** DuckDB 1.5.1, Dagster 1.12, Splink 4.0, Python 3.12, pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/dagster_pipeline/defs/entity_master_asset.py` | Create | Dagster asset producing `lake.foundation.entity_master` |
| `src/dagster_pipeline/defs/resolved_entities_asset.py` | Modify | Persist pairwise match probabilities alongside clusters |
| `tests/test_entity_master.py` | Create | Unit tests for entity_master logic (UUID generation, type classification, confidence aggregation) |
| `infra/duckdb-server/tools/entity.py` | Modify | Replace `cluster_id` lookups with `entity_id` lookups in `_resolve_name_variants`, `_entity_xray`, `_top_crossrefs` |

---

### Task 1: Entity Type Classifier

**Files:**
- Create: `tests/test_entity_master.py`
- Create: `src/dagster_pipeline/defs/entity_master_asset.py`

- [ ] **Step 1: Write failing tests for entity type classification**

```python
# tests/test_entity_master.py
import pytest
from dagster_pipeline.defs.entity_master_asset import classify_entity_type


@pytest.mark.parametrize("name,expected", [
    ("JOHN SMITH", "PERSON"),
    ("SMITH REALTY LLC", "ORGANIZATION"),
    ("123 MAIN ST HOLDINGS", "ORGANIZATION"),
    ("CITY OF NEW YORK", "ORGANIZATION"),
    ("ABC CORP", "ORGANIZATION"),
    ("MARY O'BRIEN", "PERSON"),
    ("VAN DER BERG", "PERSON"),
    ("FIRST NATIONAL BANK", "ORGANIZATION"),
    ("GREENPOINT MANAGEMENT", "ORGANIZATION"),
    ("DEPARTMENT OF EDUCATION", "ORGANIZATION"),
    ("SMITH", "PERSON"),  # single name, assume person
    ("", "UNKNOWN"),
    (None, "UNKNOWN"),
])
def test_classify_entity_type(name, expected):
    assert classify_entity_type(name) == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py -v`
Expected: FAIL with `ModuleNotFoundError` or `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/dagster_pipeline/defs/entity_master_asset.py
"""Dagster asset producing lake.foundation.entity_master."""

ORG_INDICATORS = (
    "LLC", "L.L.C.", "L.L.C", "CORP", "CORPORATION", "INC", "INCORPORATED",
    "LTD", "LIMITED", "LP", "L.P.", "LLP", "L.L.P.",
    "TRUST", "BANK", "FUND", "FOUNDATION", "ASSOCIATION", "ASSOC",
    "HOLDINGS", "REALTY", "PROPERTIES", "MANAGEMENT", "MGMT",
    "HOUSING DEV", "DEVELOPMENT", "ENTERPRISES", "PARTNERS",
    "CITY OF", "STATE OF", "COUNTY OF", "DEPT OF", "DEPARTMENT",
    "AUTHORITY", "COMMISSION", "BOARD OF", "AGENCY",
)


def classify_entity_type(name: str | None) -> str:
    """Classify a name as PERSON, ORGANIZATION, or UNKNOWN."""
    if not name or not name.strip():
        return "UNKNOWN"
    upper = name.upper().strip()
    for indicator in ORG_INDICATORS:
        if indicator in upper:
            return "ORGANIZATION"
    return "PERSON"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py -v`
Expected: All 13 parametrized cases PASS

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add tests/test_entity_master.py src/dagster_pipeline/defs/entity_master_asset.py
git commit -m "feat(entity-master): add entity type classifier with tests"
```

---

### Task 2: Deterministic UUID Generation

**Files:**
- Modify: `tests/test_entity_master.py`
- Modify: `src/dagster_pipeline/defs/entity_master_asset.py`

- [ ] **Step 1: Write failing tests for UUID generation**

```python
# tests/test_entity_master.py — append to existing file
from dagster_pipeline.defs.entity_master_asset import generate_entity_id


def test_entity_id_is_deterministic():
    """Same inputs in any order produce the same UUID."""
    members_a = ["unique_id_1", "unique_id_2", "unique_id_3"]
    members_b = ["unique_id_3", "unique_id_1", "unique_id_2"]  # different order
    assert generate_entity_id(members_a) == generate_entity_id(members_b)


def test_entity_id_differs_for_different_members():
    """Different cluster members produce different UUIDs."""
    members_a = ["unique_id_1", "unique_id_2"]
    members_b = ["unique_id_1", "unique_id_3"]
    assert generate_entity_id(members_a) != generate_entity_id(members_b)


def test_entity_id_single_member():
    """Single-member cluster produces a valid UUID."""
    result = generate_entity_id(["unique_id_42"])
    assert len(str(result)) == 36  # UUID format: 8-4-4-4-12
    assert "-" in str(result)


def test_entity_id_is_uuid_format():
    """Output is a valid UUID string."""
    import uuid
    result = generate_entity_id(["a", "b", "c"])
    parsed = uuid.UUID(str(result))
    assert str(parsed) == str(result)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py::test_entity_id_is_deterministic -v`
Expected: FAIL with `ImportError: cannot import name 'generate_entity_id'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/dagster_pipeline/defs/entity_master_asset.py — add to existing file
import hashlib
import uuid


def generate_entity_id(member_unique_ids: list[str]) -> uuid.UUID:
    """Generate a deterministic UUID from sorted cluster member IDs.

    Same members in any order always produce the same UUID.
    Different members always produce a different UUID.
    """
    sorted_hashes = sorted(hashlib.md5(m.encode()).hexdigest() for m in member_unique_ids)
    combined = "|".join(sorted_hashes)
    return uuid.UUID(hashlib.md5(combined.encode()).hexdigest())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py -v`
Expected: All tests PASS (both entity_type and entity_id tests)

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add tests/test_entity_master.py src/dagster_pipeline/defs/entity_master_asset.py
git commit -m "feat(entity-master): add deterministic UUID generation from cluster members"
```

---

### Task 3: Canonical Name and Confidence Aggregation

**Files:**
- Modify: `tests/test_entity_master.py`
- Modify: `src/dagster_pipeline/defs/entity_master_asset.py`

- [ ] **Step 1: Write failing tests for canonical name selection and confidence**

```python
# tests/test_entity_master.py — append to existing file
from dagster_pipeline.defs.entity_master_asset import (
    select_canonical_name,
    aggregate_confidence,
)


def test_canonical_name_most_frequent():
    """Most frequent (last_name, first_name) pair wins."""
    records = [
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMITH", "first_name": "J"},
        {"last_name": "SMYTH", "first_name": "JOHN"},
    ]
    last, first = select_canonical_name(records)
    assert last == "SMITH"
    assert first == "JOHN"


def test_canonical_name_tiebreak_alphabetical():
    """Ties broken alphabetically for determinism."""
    records = [
        {"last_name": "SMITH", "first_name": "JOHN"},
        {"last_name": "SMYTH", "first_name": "JOHN"},
    ]
    last, first = select_canonical_name(records)
    assert last == "SMITH"  # S < S, M < M, I < Y — SMITH wins alphabetically


def test_canonical_name_single_record():
    """Single record cluster returns that name."""
    records = [{"last_name": "JONES", "first_name": "ALICE"}]
    last, first = select_canonical_name(records)
    assert last == "JONES"
    assert first == "ALICE"


def test_aggregate_confidence_returns_mean():
    """Confidence is mean of pairwise match probabilities."""
    probabilities = [0.95, 0.92, 0.98]
    result = aggregate_confidence(probabilities)
    assert abs(result - 0.95) < 0.001


def test_aggregate_confidence_empty():
    """No probabilities (singleton cluster) returns 1.0."""
    assert aggregate_confidence([]) == 1.0


def test_aggregate_confidence_single():
    """Single probability returned as-is."""
    assert aggregate_confidence([0.93]) == 0.93
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py::test_canonical_name_most_frequent -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/dagster_pipeline/defs/entity_master_asset.py — add to existing file
from collections import Counter


def select_canonical_name(records: list[dict]) -> tuple[str, str]:
    """Select the most frequent (last_name, first_name) pair in a cluster.

    Ties broken alphabetically for determinism.
    """
    counts = Counter((r["last_name"], r["first_name"]) for r in records)
    max_count = max(counts.values())
    candidates = sorted(
        (name for name, count in counts.items() if count == max_count)
    )
    return candidates[0]


def aggregate_confidence(probabilities: list[float]) -> float:
    """Compute cluster confidence from pairwise match probabilities.

    Returns mean probability. Singleton clusters (no pairs) return 1.0.
    """
    if not probabilities:
        return 1.0
    return sum(probabilities) / len(probabilities)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add tests/test_entity_master.py src/dagster_pipeline/defs/entity_master_asset.py
git commit -m "feat(entity-master): add canonical name selection and confidence aggregation"
```

---

### Task 4: Persist Match Probabilities in Resolved Entities Pipeline

**Files:**
- Modify: `src/dagster_pipeline/defs/resolved_entities_asset.py`
- Create: `tests/test_resolved_entities_probabilities.py`

This is the critical change: currently Splink's predict output (which contains `match_probability` per pair) is discarded during clustering. We need to persist it so entity_master can compute per-cluster confidence.

- [ ] **Step 1: Read the current resolved_entities_asset.py**

Run: `cd ~/Desktop/dagster-pipeline && cat src/dagster_pipeline/defs/resolved_entities_asset.py`

Identify the exact lines where:
- `linker.inference.predict()` is called (produces pairwise predictions with match_probability)
- `linker.clustering.cluster_pairwise_predictions_at_threshold()` is called (produces cluster assignments)
- The predict DataFrame is discarded

- [ ] **Step 2: Write a test for probability persistence**

```python
# tests/test_resolved_entities_probabilities.py
"""Test that match probabilities structure is correct for entity_master consumption."""
import pytest


def test_probability_table_schema():
    """The pairwise_probabilities table must have cluster_id and match_probability."""
    # This tests the SQL that will aggregate probabilities per cluster
    expected_columns = {"cluster_id", "avg_probability", "min_probability", "pair_count"}
    # Actual test will run against DuckDB in-memory
    # For now, validate the SQL string is well-formed
    from dagster_pipeline.defs.entity_master_asset import CLUSTER_CONFIDENCE_SQL
    assert "cluster_id" in CLUSTER_CONFIDENCE_SQL
    assert "match_probability" in CLUSTER_CONFIDENCE_SQL
    assert "AVG" in CLUSTER_CONFIDENCE_SQL
```

- [ ] **Step 3: Add the SQL constant to entity_master_asset.py**

```python
# src/dagster_pipeline/defs/entity_master_asset.py — add constant
CLUSTER_CONFIDENCE_SQL = """
    SELECT
        re.cluster_id,
        AVG(pp.match_probability) AS avg_probability,
        MIN(pp.match_probability) AS min_probability,
        COUNT(*) AS pair_count
    FROM lake.federal.pairwise_probabilities pp
    JOIN lake.federal.resolved_entities re
        ON pp.unique_id_l = re.unique_id OR pp.unique_id_r = re.unique_id
    GROUP BY re.cluster_id
"""
```

- [ ] **Step 4: Modify resolved_entities_asset.py to persist predictions**

In the batch processing loop, after `linker.inference.predict()` and before clustering, save the pairwise predictions:

```python
# In resolved_entities_asset.py — inside the batch loop, after predict():
# EXISTING LINE (approximately):
#   df_predict = linker.inference.predict(threshold_match_probability=PREDICT_THRESHOLD)

# ADD after predict, before cluster:
# Persist pairwise match probabilities for entity_master confidence scores
predict_df = df_predict.as_duckdbpyrelation()
conn.execute("""
    INSERT INTO pairwise_probabilities_staging
    SELECT unique_id_l, unique_id_r, match_probability
    FROM predict_df
""")
```

Add staging table creation before the batch loop:
```python
# Before the batch loop:
conn.execute("""
    CREATE OR REPLACE TABLE pairwise_probabilities_staging (
        unique_id_l BIGINT,
        unique_id_r BIGINT,
        match_probability DOUBLE
    )
""")
```

After the batch loop, persist to DuckLake:
```python
# After all batches complete:
conn.execute("""
    CREATE OR REPLACE TABLE lake.federal.pairwise_probabilities AS
    SELECT * FROM pairwise_probabilities_staging
""")
conn.execute("DROP TABLE IF EXISTS pairwise_probabilities_staging")
```

- [ ] **Step 5: Run the probability test**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_resolved_entities_probabilities.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/defs/resolved_entities_asset.py tests/test_resolved_entities_probabilities.py src/dagster_pipeline/defs/entity_master_asset.py
git commit -m "feat(entity-master): persist pairwise match probabilities from Splink predict step"
```

---

### Task 5: Entity Master Dagster Asset

**Files:**
- Modify: `src/dagster_pipeline/defs/entity_master_asset.py`
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Write the entity_master Dagster asset**

```python
# src/dagster_pipeline/defs/entity_master_asset.py — add the asset function
import dagster
from dagster_pipeline.resources.ducklake import DuckLakeResource

ENTITY_MASTER_SQL = """
    WITH cluster_stats AS (
        SELECT
            cluster_id,
            -- Canonical name: most frequent (last_name, first_name) pair
            MODE(last_name) AS canonical_last,
            MODE(first_name) AS canonical_first,
            -- Name variants: all distinct spellings
            LIST(DISTINCT (last_name, first_name)) AS name_variants,
            -- Sources
            COUNT(DISTINCT source_table) AS source_count,
            COUNT(*) AS record_count,
            LIST(DISTINCT source_table) AS source_tables,
            -- Member IDs for deterministic UUID
            LIST(unique_id::VARCHAR ORDER BY unique_id) AS member_ids,
            -- Address aggregation
            LIST(DISTINCT {
                address: address,
                city: city,
                zip: zip
            }) FILTER (WHERE address IS NOT NULL) AS addresses,
        FROM lake.federal.resolved_entities
        GROUP BY cluster_id
    ),
    cluster_confidence AS (
        SELECT
            re.cluster_id,
            AVG(pp.match_probability) AS avg_probability,
            MIN(pp.match_probability) AS min_probability,
            COUNT(*) AS pair_count
        FROM lake.federal.pairwise_probabilities pp
        JOIN lake.federal.resolved_entities re
            ON pp.unique_id_l = re.unique_id
        GROUP BY re.cluster_id
    )
    SELECT
        -- Deterministic UUID from sorted member hashes
        md5(string_agg(md5(m.id), '|' ORDER BY md5(m.id)))::UUID AS entity_id,
        cs.cluster_id,
        cs.canonical_last,
        cs.canonical_first,
        cs.name_variants,
        CASE
            WHEN cs.canonical_last LIKE '%LLC%'
              OR cs.canonical_last LIKE '%CORP%'
              OR cs.canonical_last LIKE '%INC%'
              OR cs.canonical_last LIKE '%REALTY%'
              OR cs.canonical_last LIKE '%HOLDINGS%'
              OR cs.canonical_last LIKE '%MANAGEMENT%'
              OR cs.canonical_last LIKE '%TRUST%'
              OR cs.canonical_last LIKE '%BANK%'
              OR cs.canonical_last LIKE '%PROPERTIES%'
              OR cs.canonical_last LIKE '%CITY OF%'
              OR cs.canonical_last LIKE '%DEPARTMENT%'
              OR cs.canonical_last LIKE '%AUTHORITY%'
            THEN 'ORGANIZATION'
            ELSE 'PERSON'
        END AS entity_type,
        COALESCE(cc.avg_probability, 1.0) AS confidence,
        COALESCE(cc.min_probability, 1.0) AS min_confidence,
        COALESCE(cc.pair_count, 0) AS match_pair_count,
        cs.source_count,
        cs.record_count,
        cs.source_tables,
        cs.addresses,
        cs.member_ids,
    FROM cluster_stats cs
    LEFT JOIN cluster_confidence cc ON cs.cluster_id = cc.cluster_id
    CROSS JOIN UNNEST(cs.member_ids) AS m(id)
    GROUP BY ALL
"""


@dagster.asset(
    key=["foundation", "entity_master"],
    deps=[
        dagster.AssetKey(["federal", "resolved_entities"]),
        dagster.AssetKey(["federal", "pairwise_probabilities"]),
    ],
    kinds={"duckdb"},
    description="Canonical entity table with stable UUIDs, entity types, and confidence scores",
)
def entity_master(context: dagster.AssetExecutionContext, ducklake: DuckLakeResource):
    conn = ducklake.connect()
    try:
        context.log.info("Building entity_master from resolved_entities + pairwise_probabilities...")

        conn.execute(f"""
            CREATE OR REPLACE TABLE lake.foundation.entity_master AS
            {ENTITY_MASTER_SQL}
        """)

        row_count = conn.execute("SELECT COUNT(*) FROM lake.foundation.entity_master").fetchone()[0]
        person_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.entity_master WHERE entity_type = 'PERSON'"
        ).fetchone()[0]
        org_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.entity_master WHERE entity_type = 'ORGANIZATION'"
        ).fetchone()[0]
        avg_confidence = conn.execute(
            "SELECT AVG(confidence) FROM lake.foundation.entity_master"
        ).fetchone()[0]

        context.log.info(
            f"entity_master complete: {row_count:,} entities "
            f"({person_count:,} persons, {org_count:,} orgs), "
            f"avg confidence: {avg_confidence:.3f}"
        )

        return dagster.MaterializeResult(
            metadata={
                "row_count": row_count,
                "person_count": person_count,
                "org_count": org_count,
                "avg_confidence": round(avg_confidence, 3),
            }
        )
    finally:
        conn.close()
```

**Note:** The ENTITY_MASTER_SQL above is the target query. The exact syntax will need validation against DuckDB — particularly the `MODE()`, `LIST(DISTINCT struct)`, and the `CROSS JOIN UNNEST` for UUID generation. If `MODE()` is not available, replace with a subquery using `ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC)`. Test against real data and adjust.

- [ ] **Step 2: Register the asset in definitions.py**

```python
# src/dagster_pipeline/definitions.py — add import
from dagster_pipeline.defs.entity_master_asset import entity_master
```

Add `entity_master` to the `all_assets` list where the other foundation assets are listed.

- [ ] **Step 3: Verify the asset is discoverable**

Run: `cd ~/Desktop/dagster-pipeline && uv run dagster asset list -m dagster_pipeline.definitions | grep entity_master`
Expected: `foundation/entity_master` appears in the output

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add src/dagster_pipeline/defs/entity_master_asset.py src/dagster_pipeline/definitions.py
git commit -m "feat(entity-master): add Dagster asset producing lake.foundation.entity_master"
```

---

### Task 6: Wire Entity Master into MCP Entity Tools

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`

The 3 direct consumers of `cluster_id` in `tools/entity.py` need updating:

1. `_resolve_name_variants()` (lines ~260-310) — lookup by name → get entity_id, then get all variant names
2. `_entity_xray()` Splink section (lines ~1051-1090) — replace cluster_id with entity_id in output
3. `_top_crossrefs()` (lines ~1688-1771) — use entity_master directly instead of GROUP BY on resolved_entities

- [ ] **Step 1: Read the current entity.py to get exact line numbers**

Run: `cd ~/Desktop/dagster-pipeline && head -n 20 infra/duckdb-server/tools/entity.py`

Then read the three specific functions to understand their exact current implementation.

- [ ] **Step 2: Update `_resolve_name_variants()` to use entity_master**

Replace the `resolved_entities` query with an `entity_master` query. The function currently:
1. Looks up `cluster_id` WHERE `last_name = ? AND first_name = ?`
2. Fetches all DISTINCT `(last_name, first_name)` pairs in that cluster

Replace with:
```python
async def _resolve_name_variants(conn, last_name: str, first_name: str) -> list[tuple[str, str]]:
    """Find all name variants for a person via entity_master."""
    search_last = last_name.strip().upper()
    search_first = first_name.strip().upper()

    # Look up entity_id from entity_master
    result = conn.execute("""
        SELECT entity_id, name_variants
        FROM lake.foundation.entity_master
        WHERE canonical_last = ? AND canonical_first = ?
        LIMIT 1
    """, [search_last, search_first]).fetchone()

    if not result:
        # Fallback: check if name appears as a variant
        result = conn.execute("""
            SELECT entity_id, name_variants
            FROM lake.foundation.entity_master
            WHERE list_contains(
                list_transform(name_variants, x -> x.last_name || '|' || x.first_name),
                ? || '|' || ?
            )
            LIMIT 1
        """, [search_last, search_first]).fetchone()

    if not result:
        return [(search_last, search_first)]

    # Extract all variant names from the struct array
    variants = result[1]  # name_variants is LIST of STRUCT(last_name, first_name)
    return [(v["last_name"], v["first_name"]) for v in variants]
```

**Important:** The exact column access syntax for DuckDB struct arrays may need adjustment. Test against real data. If `name_variants` returns as a Python list of dicts, the code above works. If it returns as a DuckDB nested type, use `conn.execute("SELECT UNNEST(name_variants) FROM ...")` instead.

- [ ] **Step 3: Update `_entity_xray()` Splink section to use entity_master**

Replace the `resolved_entities` query in the Splink cluster section with:
```python
# Replace the existing cluster_id lookup with entity_master lookup
entity_result = conn.execute("""
    SELECT entity_id, entity_type, confidence, source_count, record_count, source_tables
    FROM lake.foundation.entity_master
    WHERE canonical_last = ? AND canonical_first = ?
    LIMIT 1
""", [search_last, search_first]).fetchone()

if entity_result:
    entity_id, entity_type, confidence, source_count, record_count, source_tables = entity_result
    xray_sections.append({
        "section": "ENTITY IDENTITY",
        "entity_id": str(entity_id),
        "entity_type": entity_type,
        "confidence": round(confidence, 3),
        "source_count": source_count,
        "record_count": record_count,
        "source_tables": source_tables,
    })
```

- [ ] **Step 4: Update `_top_crossrefs()` to use entity_master**

Replace the expensive `GROUP BY cluster_id` on `resolved_entities` with a direct query on `entity_master`:
```python
# Replace the existing resolved_entities GROUP BY with:
result = conn.execute("""
    SELECT
        entity_id,
        canonical_last AS last_name,
        canonical_first AS first_name,
        entity_type,
        confidence,
        source_count AS table_count,
        record_count AS total_records,
        source_tables AS tables
    FROM lake.foundation.entity_master
    WHERE source_count >= 5
    ORDER BY source_count DESC, record_count DESC
    LIMIT ?
""", [limit]).fetchall()
```

- [ ] **Step 5: Test the changes manually against the MCP server**

Run: `cd ~/Desktop/dagster-pipeline && ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 "docker exec common-ground-duckdb-server-1 python -c \"import duckdb; print(duckdb.connect('/data/common-ground/ducklake').execute('SELECT COUNT(*) FROM lake.foundation.entity_master').fetchone())\"`"

Expected: Row count returned (entity_master table exists and is queryable)

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/entity.py
git commit -m "feat(entity-master): wire entity_master into MCP entity tools, replace cluster_id lookups"
```

---

### Task 7: Add entity_id to Embeddings Pipeline

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (entity_names table schema + build function)

The `entity_names` in-memory table (used for vector search) currently has schema `(name, sources, embedding)`. Add `entity_id` so vector search results can return stable UUIDs.

- [ ] **Step 1: Read the current entity_names table creation**

Run: `cd ~/Desktop/dagster-pipeline && grep -n "entity_names" infra/duckdb-server/mcp_server.py | head -20`

Find the CREATE TABLE and the `_build_entity_name_embeddings()` function.

- [ ] **Step 2: Update entity_names schema to include entity_id**

In `mcp_server.py`, find the entity_names table creation (around line 522-527) and add the `entity_id` column:

```sql
CREATE TABLE IF NOT EXISTS entity_names (
    name VARCHAR,
    sources VARCHAR,
    entity_id VARCHAR,  -- stable UUID from entity_master
    embedding FLOAT[{dims}]
)
```

- [ ] **Step 3: Update `_build_entity_name_embeddings()` to join entity_master**

In the function that populates entity_names (around line 1565-1653), join against entity_master to get the entity_id:

```sql
-- In the SELECT that reads from name_index, add a LEFT JOIN to entity_master:
SELECT
    ni.last_name || ', ' || ni.first_name AS name,
    STRING_AGG(DISTINCT ni.source_table, ', ') AS sources,
    em.entity_id::VARCHAR AS entity_id
FROM lake.federal.name_index ni
LEFT JOIN lake.foundation.entity_master em
    ON em.canonical_last = UPPER(TRIM(ni.last_name))
    AND em.canonical_first = UPPER(TRIM(ni.first_name))
GROUP BY ni.last_name, ni.first_name, em.entity_id
HAVING COUNT(DISTINCT ni.source_table) >= 2
```

- [ ] **Step 4: Update lance.py vector search results to return entity_id**

In `shared/lance.py`, update `vector_expand_names()` and `lance_route_entity()` to include `entity_id` in their result sets:

```python
# In vector_expand_names() — add entity_id to the SELECT:
SELECT name, entity_id, array_cosine_distance(embedding, ?::FLOAT[{dims}]) AS distance
FROM entity_names
WHERE distance < {VS_NAME_DISTANCE}
ORDER BY distance
LIMIT ?
```

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/shared/lance.py
git commit -m "feat(entity-master): add entity_id to vector search pipeline"
```

---

### Task 8: Verification and Stability Test

**Files:**
- Modify: `tests/test_entity_master.py`

- [ ] **Step 1: Write a UUID stability integration test**

```python
# tests/test_entity_master.py — append
def test_entity_id_stability_across_rebuilds():
    """Simulates two runs of entity_master and verifies UUIDs match.

    This tests the core invariant: same cluster members → same entity_id.
    """
    # Simulate first run
    cluster_1_members = ["100", "200", "300"]
    cluster_2_members = ["400", "500"]

    run_1_ids = {
        "cluster_a": generate_entity_id(cluster_1_members),
        "cluster_b": generate_entity_id(cluster_2_members),
    }

    # Simulate second run (same data, different cluster labels from Splink)
    # Splink assigns NEW cluster_ids each run, but members are the same
    run_2_ids = {
        "cluster_x": generate_entity_id(cluster_1_members),  # same members
        "cluster_y": generate_entity_id(cluster_2_members),  # same members
    }

    # entity_ids should be identical regardless of Splink's cluster labels
    assert run_1_ids["cluster_a"] == run_2_ids["cluster_x"]
    assert run_1_ids["cluster_b"] == run_2_ids["cluster_y"]


def test_entity_id_changes_when_cluster_membership_changes():
    """If a new record joins a cluster, the entity_id should change.

    This is correct behavior — a different cluster IS a different entity version.
    """
    original_members = ["100", "200", "300"]
    expanded_members = ["100", "200", "300", "400"]  # new member joined

    original_id = generate_entity_id(original_members)
    expanded_id = generate_entity_id(expanded_members)

    assert original_id != expanded_id
```

- [ ] **Step 2: Run the full test suite**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_entity_master.py -v`
Expected: All tests PASS

- [ ] **Step 3: Run the existing test suite to check for regressions**

Run: `cd ~/Desktop/dagster-pipeline && uv run pytest tests/ -v --tb=short`
Expected: No regressions in existing tests

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add tests/test_entity_master.py
git commit -m "test(entity-master): add UUID stability verification tests"
```

---

## Implementation Notes

### What entity_master does NOT do (out of scope for Phase 11):

- **Address standardization** — Phase 12 adds libpostal
- **Organization entities** — name_index currently filters orgs out. entity_master v1 is mostly persons. Phase 12+ will expand.
- **Graph node replacement** — Phase 18 (Graph RAG) will replace raw name strings in graph tables with entity_ids
- **Temporal tracking** — first_seen/last_seen require date extraction from source tables (future phase)

### DuckDB SQL caveats to watch for:

- `MODE()` may not exist in DuckDB 1.5.1 — use `ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC)` subquery instead
- `LIST(DISTINCT struct)` syntax — test on real data, may need `LIST(DISTINCT ROW(last_name, first_name))`
- `list_contains` on struct arrays — may need `list_has_any` or `UNNEST` + `WHERE` instead
- The `CROSS JOIN UNNEST` for UUID generation must be tested — if performance is poor on 2.96M entities, pre-compute member hashes in a CTE

### Rollback strategy:

All existing code continues to work without entity_master. The MCP tools should have fallback: if `lake.foundation.entity_master` doesn't exist, fall back to the existing `resolved_entities` + `cluster_id` queries. Add a simple `try/except` or table-existence check at the top of each modified function.

### Deployment order:

1. Deploy modified `resolved_entities_asset.py` (persists match probabilities)
2. Materialize `resolved_entities` (now creates `pairwise_probabilities` table too)
3. Deploy `entity_master_asset.py`
4. Materialize `entity_master`
5. Deploy modified `tools/entity.py` and `mcp_server.py` to Hetzner
6. Restart duckdb-server container
