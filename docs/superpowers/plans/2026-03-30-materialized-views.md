# Materialized Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build 5 Dagster-managed materialized views in `lake.foundation.*` that pre-join and pre-aggregate the hottest query paths, dropping P99 across all MCP tools from 22.6s to <2s.

**Architecture:** Each materialized view is a Dagster `@asset` with `AutomationCondition.eager()` so it auto-refreshes when upstream ingestion assets complete. Views are written to `lake.foundation.*` as DuckLake tables (Parquet on MinIO). The MCP server reads them via normal `lake.foundation.*` SQL — no startup graph cache needed for these, they're always fresh in the lake. The MCP server's tool SQL is updated to query the materialized views instead of scanning raw tables.

**Tech Stack:** Dagster 1.12, DuckDB 1.5, DuckLake, `AutomationCondition.eager()`

**Working directory:** `/Users/fattie2020/Desktop/dagster-pipeline`

---

### Task 1: Create `materialized_view_assets.py` with building hub

**Files:**
- Create: `src/dagster_pipeline/defs/materialized_view_assets.py`

- [ ] **Step 1: Create the asset file with `mv_building_hub`**

```python
"""Materialized view assets — pre-joined/pre-aggregated tables for MCP tool performance.

These assets create foundation.mv_* tables in DuckLake that replace expensive
S3 full-table scans with pre-computed results. Each auto-refreshes when its
upstream ingestion assets complete via AutomationCondition.eager().

Consumed by: MCP server tools (building_profile, landlord_watchdog, etc.)
Stored in: lake.foundation.mv_* (DuckLake Parquet on MinIO)
"""
import logging
import time

import dagster as dg
import duckdb

from dagster_pipeline.defs.name_index_asset import _connect_ducklake

logger = logging.getLogger(__name__)


@dg.asset(
    key=dg.AssetKey(["foundation", "mv_building_hub"]),
    group_name="foundation",
    description=(
        "Pre-joined building stats by BBL: address, owner, violations, complaints, "
        "DOB violations, latest sale. Replaces 5 S3 table scans in building_profile."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "hpd_violations"]),
        dg.AssetKey(["housing", "hpd_complaints"]),
        dg.AssetKey(["housing", "dob_ecb_violations"]),
        dg.AssetKey(["housing", "hpd_jurisdiction"]),
        dg.AssetKey(["city_government", "pluto"]),
    ],
)
def mv_building_hub(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_building_hub")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_building_hub AS
            WITH buildings AS (
                SELECT
                    bbl,
                    FIRST(address) AS address,
                    FIRST(ownername) AS ownername,
                    FIRST(zipcode) AS zipcode,
                    FIRST(yearbuilt) AS yearbuilt,
                    FIRST(unitsres) AS unitsres,
                    FIRST(unitstotal) AS unitstotal,
                    FIRST(bldgclass) AS bldgclass,
                    FIRST(zonedist1) AS zoning,
                    FIRST(TRY_CAST(assesstot AS DOUBLE)) AS assessed_total
                FROM lake.city_government.pluto
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            viol AS (
                SELECT bbl,
                    COUNT(*) AS total_violations,
                    COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_violations,
                    COUNT(*) FILTER (WHERE UPPER(class) = 'C') AS class_c_violations,
                    MAX(TRY_CAST(novissueddate AS DATE)) AS latest_violation
                FROM lake.housing.hpd_violations
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            comp AS (
                SELECT bbl,
                    COUNT(DISTINCT complaint_id) AS total_complaints,
                    COUNT(DISTINCT complaint_id) FILTER (WHERE complaint_status = 'OPEN') AS open_complaints,
                    MAX(TRY_CAST(received_date AS DATE)) AS latest_complaint
                FROM lake.housing.hpd_complaints
                WHERE bbl IS NOT NULL
                GROUP BY bbl
            ),
            dob AS (
                SELECT
                    (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                    COUNT(*) AS dob_violations,
                    MAX(TRY_CAST(issue_date AS DATE)) AS latest_dob
                FROM lake.housing.dob_ecb_violations
                WHERE boro IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
                GROUP BY 1
            ),
            jurisdiction AS (
                SELECT
                    (boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) AS bbl,
                    FIRST(legalstories) AS stories,
                    MAX(COALESCE(TRY_CAST(legalclassa AS INTEGER), 0)
                      + COALESCE(TRY_CAST(legalclassb AS INTEGER), 0)) AS total_units,
                    FIRST(managementprogram) AS mgmt_program,
                    FIRST(registrationid) AS registration_id
                FROM lake.housing.hpd_jurisdiction
                WHERE boroid IS NOT NULL
                GROUP BY 1
            )
            SELECT
                COALESCE(b.bbl, j.bbl) AS bbl,
                b.address, b.ownername, b.zipcode, b.yearbuilt, b.unitsres,
                b.unitstotal, b.bldgclass, b.zoning, b.assessed_total,
                j.stories, j.total_units, j.mgmt_program, j.registration_id,
                COALESCE(v.total_violations, 0) AS total_violations,
                COALESCE(v.open_violations, 0) AS open_violations,
                COALESCE(v.class_c_violations, 0) AS class_c_violations,
                v.latest_violation,
                COALESCE(c.total_complaints, 0) AS total_complaints,
                COALESCE(c.open_complaints, 0) AS open_complaints,
                c.latest_complaint,
                COALESCE(d.dob_violations, 0) AS dob_violations,
                d.latest_dob
            FROM buildings b
            FULL OUTER JOIN jurisdiction j ON b.bbl = j.bbl
            LEFT JOIN viol v ON COALESCE(b.bbl, j.bbl) = v.bbl
            LEFT JOIN comp c ON COALESCE(b.bbl, j.bbl) = c.bbl
            LEFT JOIN dob d ON COALESCE(b.bbl, j.bbl) = d.bbl
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_building_hub"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_building_hub: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "feat: add mv_building_hub materialized view asset"
```

---

### Task 2: Add ACRIS deed index and ZIP stats views

**Files:**
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py`

- [ ] **Step 1: Add `mv_acris_deeds` asset**

Append to the file:

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_acris_deeds"]),
    group_name="foundation",
    description=(
        "Pre-joined ACRIS deed transactions: master+legals+parties with BBL. "
        "Replaces 85M row 3-table S3 join in property_history, flipper_detector."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "acris_master"]),
        dg.AssetKey(["housing", "acris_legals"]),
        dg.AssetKey(["housing", "acris_parties"]),
    ],
)
def mv_acris_deeds(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_acris_deeds")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_acris_deeds AS
            SELECT
                (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                m.document_id,
                m.doc_type,
                TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                TRY_CAST(m.document_date AS DATE) AS doc_date,
                p.name AS party_name,
                CASE WHEN p.party_type = '1' THEN 'SELLER'
                     WHEN p.party_type = '2' THEN 'BUYER'
                     ELSE 'OTHER' END AS role,
                p.address_1,
                p.city,
                p.state
            FROM lake.housing.acris_master m
            JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
            JOIN lake.housing.acris_parties p ON m.document_id = p.document_id
            WHERE m.doc_type IN ('DEED', 'DEEDO', 'DEED, RP')
              AND l.borough IS NOT NULL
              AND p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 1
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_acris_deeds"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_acris_deeds: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 2: Add `mv_zip_stats` asset**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_zip_stats"]),
    group_name="foundation",
    description=(
        "Pre-aggregated neighborhood stats by ZIP: 311 complaints, HPD complaints, "
        "crime counts, restaurant grades, income. Replaces 37M+ row scans."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["social_services", "n311_service_requests"]),
        dg.AssetKey(["housing", "hpd_complaints"]),
        dg.AssetKey(["health", "restaurant_inspections"]),
        dg.AssetKey(["economics", "irs_soi_zip_income"]),
    ],
)
def mv_zip_stats(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_zip_stats")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_zip_stats AS
            WITH n311 AS (
                SELECT incident_zip AS zip,
                    COUNT(*) AS n311_total,
                    COUNT(*) FILTER (WHERE agency = 'NYPD') AS nypd_311_calls,
                    COUNT(*) FILTER (WHERE TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY) AS n311_1yr
                FROM lake.social_services.n311_service_requests
                WHERE incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5 AND incident_zip LIKE '1%'
                GROUP BY incident_zip
            ),
            hpd AS (
                SELECT post_code AS zip,
                    COUNT(DISTINCT complaint_id) AS hpd_complaints_total,
                    COUNT(DISTINCT complaint_id) FILTER (
                        WHERE TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
                    ) AS hpd_complaints_1yr
                FROM lake.housing.hpd_complaints
                WHERE post_code IS NOT NULL AND LENGTH(post_code) = 5
                GROUP BY post_code
            ),
            rest AS (
                SELECT zipcode AS zip,
                    COUNT(DISTINCT camis) AS restaurant_count,
                    COUNT(DISTINCT camis) FILTER (WHERE grade = 'A') AS grade_a_count,
                    COUNT(DISTINCT camis) FILTER (WHERE grade IN ('A','B','C')) AS graded_count
                FROM lake.health.restaurant_inspections
                WHERE zipcode IS NOT NULL AND LENGTH(zipcode) = 5
                GROUP BY zipcode
            ),
            income AS (
                SELECT zipcode AS zip,
                    ROUND(1000.0 * SUM(TRY_CAST(agi_amount AS DOUBLE))
                        / NULLIF(SUM(TRY_CAST(num_returns AS DOUBLE)), 0), 0) AS avg_agi
                FROM lake.economics.irs_soi_zip_income
                WHERE TRY_CAST(tax_year AS INTEGER) = (
                    SELECT MAX(TRY_CAST(tax_year AS INTEGER)) FROM lake.economics.irs_soi_zip_income
                )
                GROUP BY zipcode
            )
            SELECT
                COALESCE(n.zip, h.zip, r.zip) AS zip,
                COALESCE(n.n311_total, 0) AS n311_total,
                COALESCE(n.nypd_311_calls, 0) AS nypd_311_calls,
                COALESCE(n.n311_1yr, 0) AS n311_1yr,
                COALESCE(h.hpd_complaints_total, 0) AS hpd_complaints_total,
                COALESCE(h.hpd_complaints_1yr, 0) AS hpd_complaints_1yr,
                COALESCE(r.restaurant_count, 0) AS restaurant_count,
                COALESCE(r.grade_a_count, 0) AS grade_a_count,
                COALESCE(r.graded_count, 0) AS graded_count,
                ROUND(100.0 * r.grade_a_count / NULLIF(r.graded_count, 0), 1) AS pct_grade_a,
                i.avg_agi
            FROM n311 n
            FULL OUTER JOIN hpd h ON n.zip = h.zip
            FULL OUTER JOIN rest r ON COALESCE(n.zip, h.zip) = r.zip
            LEFT JOIN income i ON COALESCE(n.zip, h.zip, r.zip) = i.zip
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_zip_stats"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_zip_stats: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "feat: add mv_acris_deeds and mv_zip_stats materialized view assets"
```

---

### Task 3: Add crime precinct and corp network views

**Files:**
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py`

- [ ] **Step 1: Add `mv_crime_precinct` asset**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_crime_precinct"]),
    group_name="foundation",
    description=(
        "Pre-aggregated crime stats by precinct: felonies, misdemeanors, arrests, "
        "top offense types. Replaces 9.5M+ row scan in safety_report."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["public_safety", "nypd_complaints_historic"]),
        dg.AssetKey(["public_safety", "nypd_complaints_ytd"]),
        dg.AssetKey(["public_safety", "nypd_arrests_historic"]),
    ],
)
def mv_crime_precinct(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_crime_precinct")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_crime_precinct AS
            WITH all_complaints AS (
                SELECT addr_pct_cd AS precinct, law_cat_cd, ofns_desc,
                       TRY_CAST(rpt_dt AS DATE) AS rpt_date
                FROM lake.public_safety.nypd_complaints_historic
                UNION ALL
                SELECT addr_pct_cd, law_cat_cd, ofns_desc,
                       TRY_CAST(rpt_dt AS DATE)
                FROM lake.public_safety.nypd_complaints_ytd
            ),
            stats AS (
                SELECT precinct,
                    COUNT(*) AS total_complaints,
                    COUNT(*) FILTER (WHERE rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS complaints_1yr,
                    COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS felonies_1yr,
                    COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS misdemeanors_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS assaults_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%robbery%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS robberies_1yr,
                    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%burglary%' AND rpt_date >= CURRENT_DATE - INTERVAL 365 DAY) AS burglaries_1yr
                FROM all_complaints
                WHERE precinct IS NOT NULL
                GROUP BY precinct
            ),
            arrests AS (
                SELECT
                    REGEXP_EXTRACT(arrest_precinct, '\\d+') AS precinct,
                    COUNT(*) AS arrests_total,
                    COUNT(*) FILTER (WHERE TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY) AS arrests_1yr
                FROM lake.public_safety.nypd_arrests_historic
                WHERE arrest_precinct IS NOT NULL
                GROUP BY 1
            )
            SELECT s.precinct,
                s.total_complaints, s.complaints_1yr,
                s.felonies_1yr, s.misdemeanors_1yr,
                s.assaults_1yr, s.robberies_1yr, s.burglaries_1yr,
                COALESCE(a.arrests_total, 0) AS arrests_total,
                COALESCE(a.arrests_1yr, 0) AS arrests_1yr
            FROM stats s
            LEFT JOIN arrests a ON s.precinct = a.precinct
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_crime_precinct"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_crime_precinct: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 2: Add `mv_corp_network` asset**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_corp_network"]),
    group_name="foundation",
    description=(
        "Pre-joined NYS corps + entity addresses for NYC-relevant corps. "
        "Replaces 50M row join in llc_piercer and corporate_web."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["business", "nys_corporations"]),
        dg.AssetKey(["business", "nys_entity_addresses"]),
    ],
)
def mv_corp_network(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_corp_network")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_corp_network AS
            WITH nyc_corps AS (
                SELECT dos_id, current_entity_name, entity_type,
                       initial_dos_filing_date, county,
                       dos_process_name, dos_process_address_1, dos_process_city,
                       registered_agent_name, chairman_name
                FROM lake.business.nys_corporations
                WHERE current_entity_name IS NOT NULL
                  AND (county IN ('NEW YORK', 'KINGS', 'QUEENS', 'BRONX', 'RICHMOND')
                       OR dos_process_city IN ('NEW YORK', 'BROOKLYN', 'BRONX', 'QUEENS',
                                               'STATEN ISLAND', 'MANHATTAN', 'FLUSHING',
                                               'ASTORIA', 'JAMAICA', 'LONG ISLAND CITY'))
            ),
            people AS (
                SELECT
                    a.corpid_num AS dos_id,
                    UPPER(TRIM(a.name)) AS person_name,
                    a.addr_type AS role,
                    UPPER(TRIM(a.addr1)) AS address
                FROM lake.business.nys_entity_addresses a
                WHERE a.corpid_num IN (SELECT dos_id FROM nyc_corps)
                  AND a.name IS NOT NULL AND LENGTH(TRIM(a.name)) > 3
                  AND UPPER(a.name) NOT IN ('NONE', 'NA', 'N/A', 'SAME', 'THE LLC')
            )
            SELECT
                c.dos_id, c.current_entity_name, c.entity_type,
                c.initial_dos_filing_date, c.county,
                c.dos_process_name, c.dos_process_address_1, c.dos_process_city,
                c.registered_agent_name, c.chairman_name,
                p.person_name, p.role, p.address AS person_address
            FROM nyc_corps c
            LEFT JOIN people p ON c.dos_id = p.dos_id
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_corp_network"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_corp_network: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 3: Commit**

```bash
git add src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "feat: add mv_crime_precinct and mv_corp_network materialized view assets"
```

---

### Task 4: Register assets in definitions.py

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Import the new assets**

Add after the existing foundation imports (line ~14):
```python
from dagster_pipeline.defs.materialized_view_assets import (
    mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
)
```

- [ ] **Step 2: Add to `all_assets` list**

Change line ~19 from:
```python
all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings]
```
to:
```python
all_assets = [*all_socrata_direct_assets, *all_federal_direct_assets, name_index, resolved_entities,
              h3_index, phonetic_index, row_fingerprints, data_health, entity_name_embeddings,
              mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network]
```

- [ ] **Step 3: Add a materialized views job**

After the `foundation_job` (line ~79):
```python
materialized_views_job = dg.define_asset_job(
    name="materialized_views_rebuild",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "mv_building_hub"]),
        dg.AssetKey(["foundation", "mv_acris_deeds"]),
        dg.AssetKey(["foundation", "mv_zip_stats"]),
        dg.AssetKey(["foundation", "mv_crime_precinct"]),
        dg.AssetKey(["foundation", "mv_corp_network"]),
    ),
)
```

Add to the jobs list in `dg.Definitions` (line ~110):
```python
jobs=[..., materialized_views_job],
```

- [ ] **Step 4: Commit**

```bash
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register materialized view assets in Dagster definitions"
```

---

### Task 5: Run initial materialization

- [ ] **Step 1: Materialize all 5 views via Dagster**

```bash
cd ~/Desktop/dagster-pipeline
DAGSTER_HOME=~/.dagster-home uv run dagster asset materialize \
  --select 'foundation/mv_building_hub foundation/mv_acris_deeds foundation/mv_zip_stats foundation/mv_crime_precinct foundation/mv_corp_network' \
  -m dagster_pipeline.definitions
```

Or via Docker if running in Docker:
```bash
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose exec dagster-code uv run dagster asset materialize \
   --select 'foundation/mv_building_hub foundation/mv_acris_deeds foundation/mv_zip_stats foundation/mv_crime_precinct foundation/mv_corp_network' \
   -m dagster_pipeline.definitions"
```

- [ ] **Step 2: Verify views exist**

```bash
# Via MCP tool
sql_query("SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'foundation' AND table_name LIKE 'mv_%' ORDER BY table_name")
```

Expected: 5 rows with row counts matching expectations:
- mv_building_hub: ~858K
- mv_acris_deeds: ~8-10M
- mv_zip_stats: ~200
- mv_crime_precinct: ~78
- mv_corp_network: ~500K-1M

- [ ] **Step 3: Commit any fixes**

```bash
git add src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "fix: materialized view adjustments from initial run"
```

---

### Task 6: Update MCP server tools to use materialized views

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

This task rewires the slowest tools to query `lake.foundation.mv_*` instead of raw lake tables. The tools fall back to live queries if the materialized view doesn't exist (graceful degradation).

- [ ] **Step 1: Update `building_profile` (the biggest win)**

The current `BUILDING_PROFILE_SQL` constant (line ~66) does 4 separate CTEs joining hpd_jurisdiction + hpd_violations + hpd_complaints + dob_ecb_violations. Replace with a single lookup from `mv_building_hub`:

```python
BUILDING_PROFILE_MV_SQL = """
SELECT bbl, address, zipcode AS zip, stories, total_units, mgmt_program AS managementprogram,
       ownername, yearbuilt, bldgclass, zoning, assessed_total,
       total_violations, open_violations, class_c_violations, latest_violation AS latest_violation_date,
       total_complaints, open_complaints, latest_complaint AS latest_complaint_date,
       dob_violations AS total_dob_violations, latest_dob AS latest_dob_date
FROM lake.foundation.mv_building_hub
WHERE bbl = ?
"""
```

In the `building_profile` tool function, try the MV first, fall back to the original SQL. **CRITICAL: both paths must return the same column set.** The MV has extra columns (ownername, yearbuilt, bldgclass, etc.) that the original SQL doesn't — this is fine as an enhancement, but the fallback path must match. Use explicit column aliasing:

```python
# Check if MV exists (once at startup, cached)
_mv_building_hub_exists = False  # set True during lifespan if table found

# In the tool:
if _mv_building_hub_exists:
    try:
        cols, rows = _execute(pool, BUILDING_PROFILE_MV_SQL, [bbl])
    except Exception:
        cols, rows = _execute(pool, BUILDING_PROFILE_SQL, [bbl, bbl, bbl, bbl])
else:
    cols, rows = _execute(pool, BUILDING_PROFILE_SQL, [bbl, bbl, bbl, bbl])
```

The `BUILDING_PROFILE_MV_SQL` returns a superset of columns. The tool function's downstream logic (which builds the summary text) should use `dict.get()` with defaults for any columns that may not exist in the fallback path.

- [ ] **Step 2: Update `property_history` to use `mv_acris_deeds`**

Replace the ACRIS 3-table join with:
```sql
SELECT * FROM lake.foundation.mv_acris_deeds WHERE bbl = ? ORDER BY doc_date DESC
```

- [ ] **Step 3: Update `flipper_detector` to use `mv_acris_deeds`**

Replace the inline ACRIS deed scan with a query against `mv_acris_deeds`.

- [ ] **Step 4: Update `neighborhood_portrait` and `complaints_by_zip` to use `mv_zip_stats`**

For ZIP-based stats, query `mv_zip_stats WHERE zip = ?` instead of scanning 311 + HPD + restaurants.

- [ ] **Step 5: Update `safety_report` to use `mv_crime_precinct`**

Replace the NYPD complaints scan with:
```sql
SELECT * FROM lake.foundation.mv_crime_precinct WHERE precinct = ?
```

- [ ] **Step 6: Update `llc_piercer` to use `mv_corp_network`**

Replace the nys_corporations + nys_entity_addresses join with a query against `mv_corp_network`.

- [ ] **Step 7: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "perf: rewire 6 tools to use materialized views instead of raw S3 scans"
```

---

### Task 7: Deploy and benchmark

- [ ] **Step 0: Verify asset keys match upstream**

Before deploying, confirm the dep asset keys are correct:
```bash
cd ~/Desktop/dagster-pipeline
uv run dagster asset list -m dagster_pipeline.definitions 2>&1 | grep -E "hpd_violations|acris_master|n311_service"
```
Expected: keys like `housing/hpd_violations`, `housing/acris_master`, `social_services/n311_service_requests`. If different, update the `deps` lists in `materialized_view_assets.py` to match.

- [ ] **Step 1: Deploy Dagster pipeline (new assets + definitions)**

The Dagster code server needs the new `materialized_view_assets.py` and updated `definitions.py` for `AutomationCondition.eager()` to trigger on the server:

```bash
cd ~/Desktop/dagster-pipeline
# Full deploy — rsyncs pipeline code + rebuilds Dagster containers
bash infra/deploy.sh
```

Or if only updating the Dagster code:
```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  src/dagster_pipeline/ \
  fattie@178.156.228.119:/opt/common-ground/dagster-pipeline/src/dagster_pipeline/

ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose restart dagster-code dagster-daemon"
```

- [ ] **Step 2: Deploy MCP server changes**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  infra/duckdb-server/mcp_server.py \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d --build --force-recreate duckdb-server 2>&1 | tail -5"
```

- [ ] **Step 2: Wait for startup, then run stress test audit**

```bash
sleep 45
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python3 stress_test.py --phase audit 2>&1
```

Compare per-tool latency to the baseline:

| Tool | Before (S3) | Target (MV) |
|------|-------------|-------------|
| building_profile | 6.4s | <500ms |
| property_history | 7.2s | <500ms |
| flipper_detector | 6.7s | <1s |
| llc_piercer | 6.7s | <500ms |
| safety_report | 2.5s | <200ms |
| neighborhood_portrait | 1.3s | <200ms |
| entity_xray | 22.6s | <5s (partial — still hits some live tables) |

- [ ] **Step 3: Run ramp test**

```bash
python3 stress_test.py --phase ramp --max-concurrency 50 2>&1
```

Target: P50 at 50 concurrent should drop significantly since each query is now a single-table local scan instead of multi-table S3 joins.

- [ ] **Step 4: Commit results**

```bash
python3 stress_test.py --phase audit 2>&1 | tee BENCHMARK-MV.md
git add BENCHMARK-MV.md
git commit -m "docs: benchmark results after materialized views"
```
