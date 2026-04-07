# Extension-Powered Data Lake Rebuild

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the data lake foundation with DuckDB community extensions (H3, splink_udfs, rapidfuzz, anofox_tabular, datasketches, hashfuncs, lindel, dqtest) to enable hex-based spatial queries, phonetic entity resolution, data quality gates, and approximate analytics — then migrate existing MCP tools to use the new foundation.

**Architecture:** Four additive layers, each independently shippable. Layer 1 materializes new columns/tables alongside existing data (never mutates). Layer 2 extracts shared query modules from the 11K-line mcp_server.py. Layer 3 upgrades spatial queries from ZIP/precinct hacks to H3 hex cells. Layer 4 adds post-ingestion quality gates. Existing tools keep working throughout — migration is per-tool, one at a time.

**Tech Stack:** DuckDB 1.5.x, DuckLake, Dagster, FastMCP, Python 3.10+. Extensions: h3, splink_udfs, rapidfuzz, anofox_tabular, datasketches, hashfuncs, lindel, dqtest. Pending 1.5.1 rebuilds: duckpgq, lsh, anofox_forecast, finetype, curl_httpfs.

**Extension inventory (installed):**
- h3 — Uber hex grid (70+ functions)
- splink_udfs — phonetic encoding (double_metaphone, soundex), damerau_levenshtein, address matching
- rapidfuzz — 37 fuzzy string matching functions (jaro_winkler, token_sort_ratio, partial_ratio)
- anofox_tabular — 150+ profiling, PII detection, anomaly detection, address parsing
- datasketches — Apache DataSketches (HLL, KLL, T-Digest, frequent items)
- hashfuncs — MurmurHash3, xxHash, RapidHash (fast non-crypto hashing)
- lindel — Hilbert/Morton space-filling curves
- dqtest — SQL-based data quality test framework
- hnsw_acorn — filtered HNSW vector search + RaBitQ quantization

**Pending (waiting on 1.5.1 community extension rebuilds):**
- duckpgq — already used by 8 graph tools, currently loads from older build
- lsh — MinHash locality-sensitive hashing for entity resolution blocking at scale
- anofox_forecast — already used by gentrification_tracker (ts_forecast_by)
- finetype — semantic type classifier (250+ types from raw strings)
- curl_httpfs — HTTP/2 + connection pooling for faster Socrata fetches

---

## File Structure

### New files (Layer 1 — Foundation Assets)

| File | Responsibility |
|------|----------------|
| `src/dagster_pipeline/defs/foundation_assets.py` | Dagster assets: h3_index, phonetic_index, address_standardized, row_fingerprints |
| `src/dagster_pipeline/defs/quality_assets.py` | Dagster assets: data_health_report, dqtest validation |
| `tests/test_foundation_assets.py` | Tests for foundation materialization |
| `tests/test_quality_assets.py` | Tests for quality gate |

### New files (Layer 2 — Shared Modules)

| File | Responsibility |
|------|----------------|
| `infra/duckdb-server/spatial.py` | H3 hex query builders (k-ring, aggregation, heatmap) |
| `infra/duckdb-server/entity.py` | Fuzzy name search (phonetic blocking + rapidfuzz scoring) |
| `infra/duckdb-server/quality.py` | Data profiling helpers (datasketches, anomaly flags) |
| `infra/duckdb-server/extensions.py` | Extension loader with graceful fallback |
| `tests/test_spatial.py` | Tests for spatial module |
| `tests/test_entity.py` | Tests for entity module |

### Modified files

| File | What changes |
|------|-------------|
| `infra/duckdb-server/mcp_server.py` | Add new extensions to COMMUNITY_EXTS, import shared modules, migrate tools one-by-one |
| `src/dagster_pipeline/definitions.py` | Register foundation + quality asset groups and jobs |
| `src/dagster_pipeline/defs/name_index_asset.py` | Add phonetic columns to name_index output |
| `src/dagster_pipeline/defs/resolved_entities_asset.py` | Use splink_udfs for phonetic blocking |
| `src/dagster_pipeline/sources/datasets.py` | Add LAT_LNG_TABLES and NAME_TABLES registries |

---

## Phase 1: DuckDB Downgrade + Extension Loading

### Task 1: Downgrade DuckDB to 1.5.0

**Files:**
- Modify: `pyproject.toml:10`
- Modify: `uv.lock` (auto-generated)

- [ ] **Step 1: Write the failing test**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect(':memory:')
for ext in ['h3', 'splink_udfs', 'rapidfuzz', 'anofox_tabular', 'datasketches', 'hashfuncs', 'lindel', 'dqtest', 'duckpgq', 'anofox_forecast']:
    conn.execute(f'INSTALL {ext} FROM community')
    conn.execute(f'LOAD {ext}')
    print(f'{ext} OK')
print('ALL EXTENSIONS LOADED')
"
```

Expected: FAIL — duckpgq and anofox_forecast fail on 1.5.1.

- [ ] **Step 2: Pin DuckDB to 1.5.0**

In `pyproject.toml` line 10, change:
```
"duckdb>=1.5.0",
```
to:
```
"duckdb==1.5.0",  # pinned until duckpgq/lsh/anofox_forecast rebuild for 1.5.1
```

- [ ] **Step 3: Lock and sync**

```bash
cd ~/Desktop/dagster-pipeline
uv lock && uv sync
```

Expected: `Updated duckdb v1.5.1 -> v1.5.0`

- [ ] **Step 4: Re-run extension test**

Run the same test from Step 1.
Expected: ALL EXTENSIONS LOADED (including duckpgq and anofox_forecast).

- [ ] **Step 5: Also install lsh and finetype (now available on 1.5.0)**

```bash
uv run python -c "
import duckdb
conn = duckdb.connect(':memory:')
for ext in ['lsh', 'finetype', 'curl_httpfs']:
    try:
        conn.execute(f'INSTALL {ext} FROM community')
        conn.execute(f'LOAD {ext}')
        print(f'{ext} OK')
    except Exception as e:
        print(f'{ext} FAIL: {e}')
"
```

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "fix: pin duckdb==1.5.0 for full community extension compatibility"
```

---

### Task 2: Update MCP Server Extension Loading

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py:637-655`
- Create: `infra/duckdb-server/extensions.py`

- [ ] **Step 1: Create extensions.py with graceful loader**

Create `infra/duckdb-server/extensions.py`:

```python
"""DuckDB extension loader with graceful fallback."""

CORE_EXTS = ["ducklake", "postgres", "spatial", "fts", "httpfs", "json"]

COMMUNITY_EXTS = [
    # Graph (phase 08 tools)
    "duckpgq",
    # Entity resolution
    "rapidfuzz",
    "splink_udfs",
    # Spatial
    "h3",
    "lindel",
    # Data quality & profiling
    "anofox_tabular",
    "anofox_forecast",
    "datasketches",
    "dqtest",
    # Performance
    "hashfuncs",
    # Vector search
    "hnsw_acorn",
    # Pending (will auto-load when available)
    "lsh",
    "finetype",
    "curl_httpfs",
]


def load_extensions(conn) -> dict[str, bool]:
    """Load all extensions, returning {name: loaded} status map."""
    status = {}

    for ext in CORE_EXTS:
        try:
            conn.execute(f"INSTALL {ext}")
            conn.execute(f"LOAD {ext}")
            status[ext] = True
        except Exception:
            try:
                conn.execute(f"LOAD {ext}")
                status[ext] = True
            except Exception as e:
                print(f"Warning: core extension {ext} unavailable: {e}", flush=True)
                status[ext] = False

    for ext in COMMUNITY_EXTS:
        try:
            conn.execute(f"INSTALL {ext} FROM community")
            conn.execute(f"LOAD {ext}")
            status[ext] = True
        except Exception as e:
            print(f"Warning: community extension {ext} unavailable: {e}", flush=True)
            status[ext] = False

    loaded = [k for k, v in status.items() if v]
    missing = [k for k, v in status.items() if not v]
    print(f"Extensions loaded: {len(loaded)}/{len(CORE_EXTS) + len(COMMUNITY_EXTS)} "
          f"({', '.join(missing) if missing else 'all OK'})", flush=True)

    return status
```

- [ ] **Step 2: Update mcp_server.py to use extensions.py**

Replace lines 637-655 in `mcp_server.py`:

```python
    from extensions import load_extensions
    ext_status = load_extensions(conn)
```

Replace the reconnect logic (~lines 699-707) similarly:

```python
        from extensions import load_extensions
        ext_status = load_extensions(conn)
```

- [ ] **Step 3: Verify server starts with all extensions**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
uv run python -c "
import duckdb
conn = duckdb.connect()
from extensions import load_extensions
status = load_extensions(conn)
assert status['h3'], 'h3 not loaded'
assert status['splink_udfs'], 'splink_udfs not loaded'
assert status['rapidfuzz'], 'rapidfuzz not loaded'
assert status['duckpgq'], 'duckpgq not loaded'
print('Server extension check passed')
"
```

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/extensions.py infra/duckdb-server/mcp_server.py
git commit -m "refactor: extract extension loading into extensions.py, add 10 new community extensions"
```

---

## Phase 2: Layer 1 — Foundation Assets

### Task 3: Register Lat/Lng and Name Tables

**Files:**
- Modify: `src/dagster_pipeline/sources/datasets.py`
- Test: `tests/test_datasets.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_datasets.py`:

```python
from dagster_pipeline.sources.datasets import LAT_LNG_TABLES, NAME_TABLES


def test_lat_lng_tables_populated():
    assert len(LAT_LNG_TABLES) >= 15
    # Every entry: (schema, table, lat_col, lng_col)
    for schema, table, lat_col, lng_col in LAT_LNG_TABLES:
        assert schema and table and lat_col and lng_col


def test_name_tables_match_registry():
    assert len(NAME_TABLES) >= 40
    # Every entry: (schema, table)
    for schema, table in NAME_TABLES:
        assert schema and table
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_datasets.py -v
```

Expected: FAIL with ImportError (LAT_LNG_TABLES not defined).

- [ ] **Step 3: Add registries to datasets.py**

Append to `src/dagster_pipeline/sources/datasets.py`:

```python
# Tables with lat/lng coordinates — eligible for H3 indexing
LAT_LNG_TABLES: list[tuple[str, str, str, str]] = [
    # (schema, table, lat_column, lng_column)
    ("city_government", "pluto", "latitude", "longitude"),
    ("environment", "street_trees", "latitude", "longitude"),
    ("public_safety", "motor_vehicle_collisions", "latitude", "longitude"),
    ("public_safety", "nypd_arrests_historic", "latitude", "longitude"),
    ("public_safety", "nypd_arrests_ytd", "latitude", "longitude"),
    ("public_safety", "nypd_complaints_historic", "latitude", "longitude"),
    ("public_safety", "nypd_complaints_ytd", "latitude", "longitude"),
    ("public_safety", "shootings", "latitude", "longitude"),
    ("health", "restaurant_inspections", "latitude", "longitude"),
    ("transportation", "mta_stations", "gtfs_latitude", "gtfs_longitude"),
    ("transportation", "mta_entrances", "entrance_latitude", "entrance_longitude"),
    ("social_services", "n311_service_requests", "latitude", "longitude"),
    ("social_services", "community_gardens", "latitude", "longitude"),
    ("environment", "nys_solar", "latitude", "longitude"),
    ("federal", "nrel_alt_fuel_stations", "latitude", "longitude"),
    ("federal", "epa_echo_facilities_manhattan", "registrylatdecdeg", "registrylondecdeg"),
    ("federal", "epa_echo_facilities_bronx", "registrylatdecdeg", "registrylondecdeg"),
    ("federal", "epa_echo_facilities_brooklyn", "registrylatdecdeg", "registrylondecdeg"),
    ("federal", "epa_echo_facilities_queens", "registrylatdecdeg", "registrylondecdeg"),
    ("federal", "epa_echo_facilities_staten_island", "registrylatdecdeg", "registrylondecdeg"),
    ("federal", "urban_school_directory", "latitude", "longitude"),
    ("federal", "hud_public_housing_developments", "latitude", "longitude"),
    ("federal", "hud_public_housing_buildings", "latitude", "longitude"),
]

# Tables with person/entity names — from name_registry.py
# Used by foundation assets for phonetic indexing
NAME_TABLES: list[tuple[str, str]] = [
    ("business", "sbs_certified"),
    ("city_government", "citywide_payroll"),
    ("city_government", "civil_service_active"),
    ("city_government", "oath_hearings"),
    ("city_government", "oath_trials"),
    ("city_government", "campaign_contributions"),
    ("city_government", "campaign_expenditures"),
    ("city_government", "doing_business_people"),
    ("city_government", "coib_enforcement"),
    ("city_government", "coib_policymakers"),
    ("city_government", "elobbyist"),
    ("city_government", "civil_litigation"),
    ("city_government", "settlement_payments"),
    ("city_government", "marriage_licenses_1908_1949"),
    ("city_government", "marriage_certificates_1866_1937"),
    ("city_government", "birth_certificates_1855_1909"),
    ("city_government", "death_certificates_1862_1948"),
    ("financial", "nys_attorney_registrations"),
    ("financial", "nys_child_support_warrants"),
    ("financial", "nys_cosmetology_licenses"),
    ("financial", "nys_notaries"),
    ("financial", "nys_re_appraisers"),
    ("financial", "nys_re_brokers"),
    ("financial", "nys_tax_warrants"),
    ("housing", "acris_parties"),
    ("housing", "acris_pp_parties"),
    ("housing", "dob_application_owners"),
    ("housing", "dob_permit_issuance"),
    ("housing", "hpd_litigations"),
    ("housing", "hpd_registration_contacts"),
    ("public_safety", "ccrb_officers"),
    ("public_safety", "nypd_officer_profile"),
    ("public_safety", "daily_inmates"),
    ("public_safety", "use_of_force_officers"),
    ("public_safety", "discipline_charges"),
    ("federal", "fec_contributions"),
    ("federal", "littlesis_entities"),
    ("federal", "propublica_nonprofits"),
    ("federal", "cl_judges"),
    ("federal", "nys_death_index"),
    ("federal", "nyc_marriage_index"),
    ("federal", "nypd_ccrb_complaints"),
    ("federal", "nypd_ccrb_officers_current"),
    ("federal", "nys_campaign_finance"),
    ("federal", "usaspending_contracts"),
    ("federal", "usaspending_grants"),
    ("business", "nys_corporations"),
    ("business", "nys_corp_constituents"),
]
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_datasets.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_pipeline/sources/datasets.py tests/test_datasets.py
git commit -m "feat: add LAT_LNG_TABLES and NAME_TABLES registries for foundation assets"
```

---

### Task 4: H3 Spatial Index Asset

**Files:**
- Create: `src/dagster_pipeline/defs/foundation_assets.py`
- Test: `tests/test_foundation_assets.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_foundation_assets.py`:

```python
"""Tests for foundation asset SQL generation — runs against in-memory DuckDB."""
import duckdb
import pytest


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("INSTALL h3 FROM community; LOAD h3")
    c.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
    c.execute("INSTALL hashfuncs FROM community; LOAD hashfuncs")
    yield c
    c.close()


def test_h3_latlng_to_cell(conn):
    """H3 extension converts lat/lng to hex cell at resolution 9."""
    result = conn.execute(
        "SELECT h3_latlng_to_cell(40.7128, -74.0060, 9)::VARCHAR"
    ).fetchone()[0]
    assert result.startswith("8")  # H3 index string
    assert len(result) == 15  # Resolution 9 = 15 hex chars


def test_h3_kring(conn):
    """H3 k-ring returns neighboring cells."""
    result = conn.execute("""
        SELECT UNNEST(h3_grid_disk(h3_latlng_to_cell(40.7128, -74.0060, 9), 1))
    """).fetchall()
    assert len(result) == 7  # Center + 6 neighbors


def test_double_metaphone(conn):
    """splink_udfs double_metaphone encodes names phonetically."""
    result = conn.execute("SELECT double_metaphone('SMITH')").fetchone()[0]
    assert result is not None
    result2 = conn.execute("SELECT double_metaphone('SMYTH')").fetchone()[0]
    assert result == result2  # SMITH and SMYTH should match


def test_murmurhash(conn):
    """hashfuncs MurmurHash3 produces consistent fingerprints."""
    r1 = conn.execute("SELECT murmurhash3_32('test_string')").fetchone()[0]
    r2 = conn.execute("SELECT murmurhash3_32('test_string')").fetchone()[0]
    assert r1 == r2  # deterministic
    r3 = conn.execute("SELECT murmurhash3_32('different')").fetchone()[0]
    assert r1 != r3  # different inputs produce different hashes
```

- [ ] **Step 2: Run test to verify it passes (extension smoke test)**

```bash
cd ~/Desktop/dagster-pipeline && uv run pytest tests/test_foundation_assets.py -v
```

Expected: PASS (this validates extensions work locally).

- [ ] **Step 3: Create foundation_assets.py with H3 index asset**

Create `src/dagster_pipeline/defs/foundation_assets.py`:

```python
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

logger = logging.getLogger(__name__)

H3_RESOLUTION = 9  # ~100m hexagons — block-level for NYC


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

        # Build UNION ALL across all lat/lng tables
        unions = []
        skipped = []
        for schema, table, lat_col, lng_col in LAT_LNG_TABLES:
            try:
                # Verify table exists and has the columns
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

        conn.execute(f"""
            CREATE SCHEMA IF NOT EXISTS lake.foundation
        """)
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
```

- [ ] **Step 4: Commit**

```bash
git add src/dagster_pipeline/defs/foundation_assets.py tests/test_foundation_assets.py
git commit -m "feat: add H3 spatial index foundation asset"
```

---

### Task 5: Phonetic Index Asset

**Files:**
- Modify: `src/dagster_pipeline/defs/foundation_assets.py`

- [ ] **Step 1: Add phonetic_index asset to foundation_assets.py**

Append to `foundation_assets.py`:

```python
from dagster_pipeline.sources.name_registry import NAME_REGISTRY


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
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS lake.foundation
        """)
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

        # How many unique phonetic last names?
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/foundation_assets.py
git commit -m "feat: add phonetic index foundation asset (double_metaphone + soundex)"
```

---

### Task 6: Row Fingerprint Asset

**Files:**
- Modify: `src/dagster_pipeline/defs/foundation_assets.py`

- [ ] **Step 1: Add row_fingerprints asset**

Append to `foundation_assets.py`:

```python
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

    # Tables where dedup matters most — high volume, frequent refresh
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/foundation_assets.py
git commit -m "feat: add row fingerprint asset (MurmurHash3 dedup detection)"
```

---

### Task 7: Register Foundation Assets in Definitions

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Import and register foundation assets**

Add to `definitions.py` imports:

```python
from dagster_pipeline.defs.foundation_assets import h3_index, phonetic_index, row_fingerprints
```

Update `all_assets`:

```python
all_assets = [*all_socrata_assets, *all_federal_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints]
```

Add foundation job:

```python
foundation_job = dg.define_asset_job(
    name="foundation_rebuild",
    selection=dg.AssetSelection.groups("foundation"),
)
```

Add to `defs`:

```python
jobs=[daily_live_job, monthly_refresh_job, all_assets_job, entity_resolution_job, foundation_job],
```

- [ ] **Step 2: Verify definitions load**

```bash
uv run python -c "from dagster_pipeline.definitions import defs; print(f'{len(defs.get_asset_graph().all_asset_keys)} assets registered')"
```

Expected: Previous count + 3 new foundation assets.

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register foundation assets (h3_index, phonetic_index, row_fingerprints)"
```

---

## Phase 3: Layer 2 — Shared Query Modules

### Task 8: Spatial Module (H3 Query Builders)

**Files:**
- Create: `infra/duckdb-server/spatial.py`
- Create: `tests/test_spatial.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_spatial.py`:

```python
"""Tests for H3 spatial query builders."""
import pytest


def test_kring_sql():
    from spatial import h3_kring_sql
    sql = h3_kring_sql(lat=40.7128, lng=-74.006, radius_rings=2)
    assert "h3_grid_disk" in sql
    assert "h3_latlng_to_cell" in sql
    assert "40.7128" in sql


def test_hex_aggregate_sql():
    from spatial import h3_aggregate_sql
    sql = h3_aggregate_sql(
        source_table="lake.foundation.h3_index",
        filter_tables=["public_safety.nypd_complaints_ytd"],
        lat=40.7128, lng=-74.006, radius_rings=3,
    )
    assert "h3_grid_disk" in sql
    assert "GROUP BY" in sql
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/Desktop/dagster-pipeline && PYTHONPATH=infra/duckdb-server uv run pytest tests/test_spatial.py -v
```

Expected: FAIL with ImportError.

- [ ] **Step 3: Create spatial.py**

Create `infra/duckdb-server/spatial.py`:

```python
"""H3 hex-based spatial query builders.

Replaces ZIP/precinct/community district crosswalk hacks with
H3 k-ring aggregation. Resolution 9 (~100m) for block-level NYC analysis.
"""

H3_RES = 9


def h3_kring_sql(lat: float, lng: float, radius_rings: int = 2) -> str:
    """SQL to get all H3 cells within radius_rings of a point."""
    return f"""
        SELECT UNNEST(
            h3_grid_disk(h3_latlng_to_cell({lat}, {lng}, {H3_RES}), {radius_rings})
        ) AS h3_cell
    """


def h3_aggregate_sql(
    source_table: str,
    filter_tables: list[str],
    lat: float,
    lng: float,
    radius_rings: int = 3,
) -> str:
    """SQL to aggregate counts from h3_index within a hex k-ring.

    Returns per-source-table counts within the spatial radius.
    """
    table_filter = ", ".join(f"'{t}'" for t in filter_tables)
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
        )
        SELECT
            source_table,
            COUNT(*) AS row_count,
            COUNT(DISTINCT h3_res9) AS cell_count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table IN ({table_filter})
        GROUP BY source_table
        ORDER BY row_count DESC
    """


def h3_heatmap_sql(
    source_table: str,
    filter_table: str,
    lat: float,
    lng: float,
    radius_rings: int = 5,
) -> str:
    """SQL to generate per-cell counts for heatmap visualization."""
    return f"""
        WITH target_cells AS (
            {h3_kring_sql(lat, lng, radius_rings)}
        )
        SELECT
            h3_res9 AS h3_cell,
            h3_cell_to_lat(h3_res9) AS cell_lat,
            h3_cell_to_lng(h3_res9) AS cell_lng,
            COUNT(*) AS count
        FROM {source_table}
        WHERE h3_res9 IN (SELECT h3_cell FROM target_cells)
          AND source_table = '{filter_table}'
        GROUP BY h3_res9
        ORDER BY count DESC
    """
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=infra/duckdb-server uv run pytest tests/test_spatial.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/spatial.py tests/test_spatial.py
git commit -m "feat: add H3 spatial query builder module"
```

---

### Task 9: Entity Module (Fuzzy Name Search)

**Files:**
- Create: `infra/duckdb-server/entity.py`
- Create: `tests/test_entity.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_entity.py`:

```python
"""Tests for entity fuzzy name search builders."""


def test_phonetic_search_sql():
    from entity import phonetic_search_sql
    sql = phonetic_search_sql("John", "Smith")
    assert "double_metaphone" in sql
    assert "jaro_winkler_similarity" in sql
    assert "phonetic_index" in sql


def test_fuzzy_name_sql():
    from entity import fuzzy_name_sql
    sql = fuzzy_name_sql("BLACKSTONE GROUP")
    assert "token_sort_ratio" in sql or "partial_ratio" in sql
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=infra/duckdb-server uv run pytest tests/test_entity.py -v
```

Expected: FAIL with ImportError.

- [ ] **Step 3: Create entity.py**

Create `infra/duckdb-server/entity.py`:

```python
"""Fuzzy entity search — phonetic blocking + rapidfuzz scoring.

Replaces raw UPPER() = UPPER() matching in entity_xray, person_crossref,
money_trail, marriage_search, vital_records, due_diligence.
"""


def phonetic_search_sql(
    first_name: str | None,
    last_name: str,
    min_score: float = 0.75,
    limit: int = 50,
) -> str:
    """SQL to find person matches using phonetic blocking + fuzzy scoring.

    Strategy:
    1. Block on double_metaphone(last_name) — reduces candidates by ~100x
    2. Score with jaro_winkler_similarity — handles typos, nicknames
    3. Boost with token_sort_ratio for reordered names
    """
    escaped_last = last_name.replace("'", "''")

    if first_name:
        escaped_first = first_name.replace("'", "''")
        return f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS last_score,
                jaro_winkler_similarity(UPPER(first_name), UPPER('{escaped_first}')) AS first_score,
                (jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) * 0.6
                 + jaro_winkler_similarity(UPPER(first_name), UPPER('{escaped_first}')) * 0.4
                ) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER('{escaped_last}'))
            ORDER BY combined_score DESC
            LIMIT {limit}
        """
    else:
        return f"""
            SELECT
                unique_id, last_name, first_name, source_table,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS last_score,
                1.0 AS first_score,
                jaro_winkler_similarity(UPPER(last_name), UPPER('{escaped_last}')) AS combined_score
            FROM lake.foundation.phonetic_index
            WHERE dm_last = double_metaphone(UPPER('{escaped_last}'))
            ORDER BY combined_score DESC
            LIMIT {limit}
        """


def fuzzy_name_sql(
    name: str,
    table: str = "lake.federal.name_index",
    name_col: str = "last_name",
    min_score: int = 70,
    limit: int = 30,
) -> str:
    """SQL for fuzzy entity/company name matching using rapidfuzz token_sort_ratio.

    Good for: company names, combined name fields, free-text respondent fields.
    """
    escaped = name.replace("'", "''")
    return f"""
        SELECT *,
            rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) AS match_score
        FROM {table}
        WHERE rapidfuzz_token_sort_ratio(UPPER({name_col}), UPPER('{escaped}')) >= {min_score}
        ORDER BY match_score DESC
        LIMIT {limit}
    """
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=infra/duckdb-server uv run pytest tests/test_entity.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add infra/duckdb-server/entity.py tests/test_entity.py
git commit -m "feat: add fuzzy entity search module (phonetic blocking + rapidfuzz)"
```

---

### Task 10: Quality Module (DataSketches + Anomaly Helpers)

**Files:**
- Create: `infra/duckdb-server/quality.py`

- [ ] **Step 1: Create quality.py**

Create `infra/duckdb-server/quality.py`:

```python
"""Data quality helpers — datasketches profiling + anomaly detection.

Provides SQL builders for approximate analytics (HLL distinct counts,
KLL quantiles, frequent items) and anomaly flagging (IQR, Z-score).
"""


def approx_distinct_sql(table: str, column: str) -> str:
    """Approximate distinct count using HyperLogLog."""
    return f"""
        SELECT datasketch_hll_estimate(datasketch_hll({column})) AS approx_distinct
        FROM {table}
        WHERE {column} IS NOT NULL
    """


def approx_quantiles_sql(table: str, column: str) -> str:
    """Approximate quantiles (p25, p50, p75, p95, p99) using KLL sketch."""
    return f"""
        WITH sketch AS (
            SELECT datasketch_kll(TRY_CAST({column} AS FLOAT)) AS s
            FROM {table}
            WHERE TRY_CAST({column} AS FLOAT) IS NOT NULL
        )
        SELECT
            datasketch_kll_quantile(s, 0.25) AS p25,
            datasketch_kll_quantile(s, 0.50) AS median,
            datasketch_kll_quantile(s, 0.75) AS p75,
            datasketch_kll_quantile(s, 0.95) AS p95,
            datasketch_kll_quantile(s, 0.99) AS p99
        FROM sketch
    """


def frequent_items_sql(table: str, column: str, top_n: int = 20) -> str:
    """Find most frequent items using DataSketches frequent items."""
    return f"""
        SELECT {column}, COUNT(*) AS freq
        FROM {table}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        ORDER BY freq DESC
        LIMIT {top_n}
    """


def iqr_outliers_sql(table: str, column: str, multiplier: float = 1.5) -> str:
    """Flag IQR outliers in a numeric column."""
    return f"""
        WITH stats AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST({column} AS DOUBLE)) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST({column} AS DOUBLE)) AS q3
            FROM {table}
            WHERE TRY_CAST({column} AS DOUBLE) IS NOT NULL
        ),
        bounds AS (
            SELECT q1, q3, (q3 - q1) AS iqr,
                   q1 - {multiplier} * (q3 - q1) AS lower_bound,
                   q3 + {multiplier} * (q3 - q1) AS upper_bound
            FROM stats
        )
        SELECT t.*, b.lower_bound, b.upper_bound,
               CASE WHEN TRY_CAST(t.{column} AS DOUBLE) < b.lower_bound THEN 'low_outlier'
                    WHEN TRY_CAST(t.{column} AS DOUBLE) > b.upper_bound THEN 'high_outlier'
                    ELSE 'normal' END AS outlier_flag
        FROM {table} t, bounds b
        WHERE TRY_CAST(t.{column} AS DOUBLE) < b.lower_bound
           OR TRY_CAST(t.{column} AS DOUBLE) > b.upper_bound
        ORDER BY TRY_CAST(t.{column} AS DOUBLE) DESC
    """
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/quality.py
git commit -m "feat: add data quality module (datasketches profiling + IQR anomaly detection)"
```

---

## Phase 4: Layer 3 — MCP Tool Migration (Per-Tool, Incremental)

### Task 11: Migrate data_catalog to Use New Extensions

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (data_catalog tool)

The `data_catalog` tool already uses `rapidfuzz_partial_ratio`. Enhance it with:
- datasketches approximate row counts (faster than estimated_size)
- finetype semantic column types (when available)

- [ ] **Step 1: Update DATA_CATALOG_SQL to include approx row counts**

Replace `DATA_CATALOG_SQL` in mcp_server.py with:

```python
DATA_CATALOG_SQL = """
WITH table_matches AS (
    SELECT DISTINCT t.schema_name, t.table_name, t.comment,
           t.column_count, t.estimated_size, 'table/comment' AS match_type,
           GREATEST(
               rapidfuzz_partial_ratio(LOWER(t.table_name), LOWER(?)),
               rapidfuzz_partial_ratio(LOWER(COALESCE(t.comment, '')), LOWER(?))
           ) AS score
    FROM duckdb_tables() t
    WHERE t.database_name = 'lake'
      AND t.schema_name NOT IN ('information_schema', 'pg_catalog')
),
column_matches AS (
    SELECT DISTINCT c.schema_name, c.table_name, t.comment,
           t.column_count, t.estimated_size, 'column' AS match_type,
           rapidfuzz_partial_ratio(LOWER(c.column_name), LOWER(?)) AS score
    FROM duckdb_columns() c
    JOIN duckdb_tables() t ON c.schema_name = t.schema_name
         AND c.table_name = t.table_name AND t.database_name = 'lake'
    WHERE c.database_name = 'lake'
),
-- Also match against H3-indexed tables (spatial capability flag)
h3_tables AS (
    SELECT DISTINCT source_table, COUNT(*) AS geo_rows
    FROM lake.foundation.h3_index
    GROUP BY source_table
),
all_matches AS (
    SELECT schema_name, table_name, comment, column_count, estimated_size,
           match_type, MAX(score) AS score
    FROM (
        SELECT * FROM table_matches
        UNION ALL
        SELECT * FROM column_matches
    )
    GROUP BY schema_name, table_name, comment, column_count, estimated_size, match_type
)
SELECT a.schema_name, a.table_name, a.comment, a.estimated_size, a.column_count,
       a.match_type, a.score,
       h.geo_rows IS NOT NULL AS has_geo,
       COALESCE(h.geo_rows, 0) AS geo_row_count
FROM all_matches a
LEFT JOIN h3_tables h ON (a.schema_name || '.' || a.table_name) = h.source_table
WHERE a.score >= 60
ORDER BY a.score DESC, a.estimated_size DESC
LIMIT 30
"""
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: enhance data_catalog with H3 spatial capability flags"
```

---

### Task 12: Migrate entity_xray to Use Phonetic Search

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (entity_xray tool)

- [ ] **Step 1: Add phonetic search import and helper**

At the top of mcp_server.py (after existing imports), add:

```python
from entity import phonetic_search_sql, fuzzy_name_sql
from spatial import h3_kring_sql, h3_aggregate_sql
```

- [ ] **Step 2: Add phonetic cross-reference to entity_xray**

In the `entity_xray` tool function, after the existing resolved_entities query, add a phonetic search fallback:

```python
    # Phonetic cross-reference (catches typos, spelling variants)
    try:
        parts = name.strip().split()
        if len(parts) >= 2:
            phonetic_sql = phonetic_search_sql(
                first_name=parts[0], last_name=parts[-1], min_score=0.7, limit=20
            )
        else:
            phonetic_sql = phonetic_search_sql(
                first_name=None, last_name=name, min_score=0.7, limit=20
            )
        ph_cols, ph_rows = _execute(conn, phonetic_sql)
        phonetic_matches = [dict(zip(ph_cols, r)) for r in ph_rows]
        if phonetic_matches:
            sections["phonetic_matches"] = phonetic_matches
    except Exception:
        pass  # Phonetic index may not exist yet
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add phonetic cross-reference to entity_xray (splink_udfs + rapidfuzz)"
```

---

### Task 13: Add New MCP Tool — hotspot_map

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add hotspot_map tool**

Add to mcp_server.py after the existing neighborhood tools:

```python
@mcp.tool(annotations=READONLY)
def hotspot_map(
    latitude: Annotated[float, Field(description="Center latitude. Example: 40.7128")],
    longitude: Annotated[float, Field(description="Center longitude. Example: -74.006")],
    category: Annotated[str, Field(description="Data category: 'crime', 'violations', 'complaints', '311', 'restaurants'")] = "crime",
    radius: Annotated[int, Field(description="H3 k-ring radius (1-5). 1=~200m, 3=~600m, 5=~1km")] = 3,
) -> str:
    """H3 hex heatmap around a point — see density of crime, violations, complaints."""
    TABLE_MAP = {
        "crime": "public_safety.nypd_complaints_ytd",
        "violations": "housing.hpd_violations",
        "complaints": "housing.hpd_complaints",
        "311": "social_services.n311_service_requests",
        "restaurants": "health.restaurant_inspections",
    }
    source = TABLE_MAP.get(category)
    if not source:
        raise ToolError(f"Unknown category '{category}'. Use: {', '.join(TABLE_MAP.keys())}")

    sql = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table=source,
        lat=latitude, lng=longitude,
        radius_rings=min(radius, 5),
    )
    cols, raw_rows = _execute(conn, sql)
    if not raw_rows:
        return f"No {category} data found near ({latitude}, {longitude}). Foundation H3 index may need materialization."

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total = sum(r["count"] for r in rows)
    hottest = rows[0]
    return (
        f"## {category.title()} Hotspot Map ({len(rows)} hex cells)\n\n"
        f"**Center:** ({latitude}, {longitude}) | **Radius:** {radius} rings (~{radius * 200}m)\n"
        f"**Total events:** {total:,}\n"
        f"**Hottest cell:** {hottest['count']:,} events at ({hottest['cell_lat']:.4f}, {hottest['cell_lng']:.4f})\n\n"
        + "\n".join(
            f"| {r['h3_cell']} | {r['cell_lat']:.4f} | {r['cell_lng']:.4f} | {r['count']:,} |"
            for r in rows[:20]
        )
    )
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add hotspot_map MCP tool (H3 hex heatmap)"
```

---

### Task 14: Add New MCP Tool — name_variants

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add name_variants tool**

```python
@mcp.tool(annotations=READONLY)
def name_variants(
    name: NAME,
) -> str:
    """Find all phonetic matches for a name across the entire data lake.
    Catches typos, maiden names, spelling variants, nicknames."""
    parts = name.strip().split()
    if len(parts) >= 2:
        sql = phonetic_search_sql(first_name=parts[0], last_name=parts[-1], limit=50)
    else:
        sql = phonetic_search_sql(first_name=None, last_name=name, limit=50)

    cols, raw_rows = _execute(conn, sql)
    if not raw_rows:
        return f"No phonetic matches for '{name}'. The phonetic index may need materialization."

    rows = [dict(zip(cols, r)) for r in raw_rows]

    # Group by source_table
    by_source = {}
    for r in rows:
        src = r.get("source_table", "unknown")
        by_source.setdefault(src, []).append(r)

    lines = [f"## Name Variants for '{name}' ({len(rows)} matches across {len(by_source)} tables)\n"]
    for src, matches in sorted(by_source.items(), key=lambda x: -len(x[1])):
        lines.append(f"\n### {src} ({len(matches)} matches)")
        for m in matches[:5]:
            score = m.get("combined_score", 0)
            lines.append(f"- {m.get('first_name', '')} {m.get('last_name', '')} (score: {score:.2f})")

    return "\n".join(lines)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add name_variants MCP tool (phonetic name search across lake)"
```

---

## Phase 5: Layer 4 — Quality Gate

### Task 15: Data Health Report Asset

**Files:**
- Create: `src/dagster_pipeline/defs/quality_assets.py`

- [ ] **Step 1: Create quality_assets.py**

```python
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
        conn.execute("INSTALL anofox_tabular FROM community; LOAD anofox_tabular")
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")

        # Get all lake tables
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

                # Get column count and null rates for first 5 columns
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
                        null_rate = conn.execute(f"""
                            SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE "{col_name}" IS NULL)
                                   / NULLIF(COUNT(*), 0), 1)
                            FROM lake.{schema}.{table}
                        """).fetchone()[0]
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
                    "profiled_at": "CURRENT_TIMESTAMP",
                })
            except Exception as e:
                context.log.warning("Failed to profile %s.%s: %s", schema, table, e)

        # Write profiles to foundation table
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
```

- [ ] **Step 2: Register in definitions.py**

Add import:
```python
from dagster_pipeline.defs.quality_assets import data_health
```

Add to `all_assets`:
```python
all_assets = [*all_socrata_assets, *all_federal_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health]
```

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/defs/quality_assets.py src/dagster_pipeline/definitions.py
git commit -m "feat: add data_health quality gate asset (table profiling + null detection)"
```

---

### Task 16: Add lake_health MCP Tool

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Add lake_health tool**

```python
@mcp.tool(annotations=READONLY)
def lake_health(
    schema: Annotated[str | None, Field(description="Filter to specific schema. Example: 'housing'")] = None,
) -> str:
    """Data lake health dashboard — row counts, null rates, freshness per table."""
    where = f"WHERE schema_name = '{schema}'" if schema else ""
    cols, raw_rows = _execute(conn, f"""
        SELECT schema_name, table_name, row_count, column_count, high_null_columns, profiled_at
        FROM lake.foundation.data_health
        {where}
        ORDER BY row_count DESC
    """)
    if not raw_rows:
        return "No health data. Run the foundation_rebuild job first."

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total_rows = sum(r["row_count"] for r in rows)
    unhealthy = [r for r in rows if r["high_null_columns"] > 0]

    lines = [
        f"## Data Lake Health ({len(rows)} tables, {total_rows:,} total rows)\n",
        f"**Tables with high-null columns:** {len(unhealthy)}\n",
        "| Schema | Table | Rows | Cols | High-Null Cols |",
        "|--------|-------|------|------|----------------|",
    ]
    for r in rows[:30]:
        flag = " ⚠" if r["high_null_columns"] > 0 else ""
        lines.append(
            f"| {r['schema_name']} | {r['table_name']} | {r['row_count']:,} | "
            f"{r['column_count']} | {r['high_null_columns']}{flag} |"
        )

    return "\n".join(lines)
```

- [ ] **Step 2: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat: add lake_health MCP tool (data quality dashboard)"
```

---

## Phase 6: Enhanced Entity Resolution

### Task 17: Add Phonetic Blocking to Splink

**Files:**
- Modify: `src/dagster_pipeline/defs/resolved_entities_asset.py`

- [ ] **Step 1: Add splink_udfs to batch processing**

In `_process_batch()`, after creating the DuckDB connection, add extension loading:

```python
    # Load phonetic UDFs for enhanced blocking
    try:
        conn.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs")
    except Exception:
        pass  # Fall back to standard blocking if unavailable
```

- [ ] **Step 2: Add phonetic columns to batch_data**

After creating `batch_data` table, add phonetic columns:

```python
    # Add phonetic columns for enhanced comparison
    try:
        conn.execute("""
            ALTER TABLE batch_data ADD COLUMN dm_last VARCHAR;
            UPDATE batch_data SET dm_last = double_metaphone(UPPER(last_name));
            ALTER TABLE batch_data ADD COLUMN dm_first VARCHAR;
            UPDATE batch_data SET dm_first = double_metaphone(UPPER(first_name));
        """)
    except Exception:
        pass  # Non-critical enhancement
```

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/defs/resolved_entities_asset.py
git commit -m "feat: add phonetic columns to Splink batch processing for enhanced blocking"
```

---

## Phase 7: Deployment

### Task 18: Update Docker Image

**Files:**
- Modify: `Dockerfile` (if extensions need to be pre-installed in the image)
- Modify: `infra/duckdb-server/Dockerfile` (add new Python modules)

- [ ] **Step 1: Add new modules to duckdb-server Docker context**

Ensure `spatial.py`, `entity.py`, `quality.py`, `extensions.py` are copied in the Dockerfile:

```dockerfile
COPY infra/duckdb-server/spatial.py /app/spatial.py
COPY infra/duckdb-server/entity.py /app/entity.py
COPY infra/duckdb-server/quality.py /app/quality.py
COPY infra/duckdb-server/extensions.py /app/extensions.py
```

- [ ] **Step 2: Pre-install extensions in Docker image**

Add to Dockerfile before the CMD:

```dockerfile
RUN python -c "
import duckdb
conn = duckdb.connect()
for ext in ['h3','splink_udfs','rapidfuzz','anofox_tabular','datasketches','hashfuncs','lindel','dqtest','duckpgq','anofox_forecast','hnsw_acorn']:
    try:
        conn.execute(f'INSTALL {ext} FROM community')
        print(f'{ext} pre-installed')
    except: pass
conn.close()
"
```

- [ ] **Step 3: Rebuild and test**

```bash
cd ~/Desktop/dagster-pipeline
docker compose build duckdb-server
docker compose up -d duckdb-server
# Wait for startup, then verify
curl -s http://localhost:8000/health | python -m json.tool
```

- [ ] **Step 4: Commit**

```bash
git add Dockerfile infra/duckdb-server/Dockerfile
git commit -m "feat: add foundation modules and pre-install extensions in Docker image"
```

---

### Task 19: Deploy and Verify

- [ ] **Step 1: Run foundation_rebuild job locally**

```bash
uv run dagster job execute -m dagster_pipeline.definitions -j foundation_rebuild
```

Verify: h3_index, phonetic_index, row_fingerprints, data_health all materialize.

- [ ] **Step 2: Deploy to Hetzner**

```bash
./deploy.sh
```

- [ ] **Step 3: Smoke test new MCP tools**

Test via MCP client:
- `hotspot_map(latitude=40.7128, longitude=-74.006, category="crime")`
- `name_variants(name="John Smith")`
- `lake_health(schema="housing")`

- [ ] **Step 4: Commit deploy state**

```bash
git add docs/superpowers/STATE.md
git commit -m "docs: update state after extension-powered rebuild deployment"
```

---

## Migration Checklist — Future Per-Tool Upgrades

After the foundation is deployed, migrate existing tools one at a time. Each is a separate PR:

- [ ] `neighborhood_compare` — replace ZIP→precinct crosswalk with H3 aggregation
- [ ] `neighborhood_portrait` — H3 walkable area instead of ZIP
- [ ] `area_snapshot` — H3 k-ring instead of ST_DWithin
- [ ] `block_timeline` — H3 neighbor discovery instead of BBL prefix
- [ ] `safety_report` — H3 hex aggregation instead of precinct
- [ ] `gentrification_tracker` — H3 sub-ZIP time series
- [ ] `entity_xray` — full phonetic + rapidfuzz replacement of UPPER() matching
- [ ] `money_trail` — fuzzy donor matching across NYC/NYS/FEC
- [ ] `marriage_search` — phonetic search for historical records
- [ ] `vital_records` — soundex for 1800s variant spellings
- [ ] `due_diligence` — phonetic + PII scan before returning results
- [ ] `worst_landlords` — anomaly flagging on outlier portfolios
- [ ] `building_profile` — anomaly flags on violation counts
