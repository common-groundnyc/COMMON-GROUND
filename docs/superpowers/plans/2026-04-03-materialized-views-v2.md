# Materialized Views v2 — Eliminate Full Table Scans

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create 5 new Dagster materialized view assets that pre-compute expensive aggregations, eliminating full table scans that take 3-33 seconds per MCP tool call. Expected: entity_xray ACRIS 33s→<1s, address_report percentiles 15s→<1s, network/safety city averages 3-10s→<1s.

**Architecture:** Same pattern as existing `mv_building_hub` in `materialized_view_assets.py`. Each MV is a Dagster `@asset` with `AutomationCondition.eager()` that creates a `lake.foundation.mv_*` table via `CREATE TABLE AS SELECT`. MCP tools then query the small MV instead of scanning raw tables. MVs auto-refresh when upstream data changes.

**Tech Stack:** Dagster, DuckDB/DuckLake, Python

---

## File Structure

| Action | File | What Changes |
|--------|------|-------------|
| Modify | `src/dagster_pipeline/defs/materialized_view_assets.py` | Add 5 new MV assets |
| Modify | `src/dagster_pipeline/definitions.py` | Register new MVs in all_assets + automation sensor |
| Modify | `infra/duckdb-server/tools/entity.py` | Use mv_entity_acris instead of triple-join |
| Modify | `infra/duckdb-server/tools/network.py` | Use mv_city_averages instead of full GROUP BY |
| Modify | `infra/duckdb-server/tools/safety.py` | Use mv_city_averages for precinct averages |
| Modify | `infra/duckdb-server/tools/_address_queries.py` | Use mv_pctile_* instead of window scans |

---

### Task 1: Create mv_entity_acris — pre-joined ACRIS party+transaction index

**Files:**
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py`

The ACRIS triple-join (parties + master + legals) takes 33s because it scans 3 large tables with a `LIKE '%NAME%'` filter. Pre-join them into a name→transaction lookup table.

- [ ] **Step 1: Add the asset**

Append to `materialized_view_assets.py`:

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_entity_acris"]),
    group_name="foundation",
    description=(
        "Pre-joined ACRIS party→transaction index. Maps party names to document details "
        "and BBLs. Replaces expensive 3-table JOIN in entity_xray."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "acris_master"]),
        dg.AssetKey(["housing", "acris_parties"]),
        dg.AssetKey(["housing", "acris_legals"]),
    ],
)
def mv_entity_acris(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_entity_acris")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_entity_acris AS
            SELECT
                UPPER(p.name) AS party_name,
                p.party_type,
                m.doc_type,
                TRY_CAST(m.document_amt AS DOUBLE) AS amount,
                TRY_CAST(m.document_date AS DATE) AS document_date,
                (l.borough || LPAD(l.block::VARCHAR, 5, '0')
                           || LPAD(l.lot::VARCHAR, 4, '0')) AS bbl,
                l.street_name,
                l.unit
            FROM lake.housing.acris_parties p
            JOIN lake.housing.acris_master m ON p.document_id = m.document_id
            JOIN lake.housing.acris_legals l ON p.document_id = l.document_id
            WHERE p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 1
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_entity_acris"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_entity_acris: %s rows in %ss", f"{row_count:,}", elapsed)
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
git commit -m "feat: add mv_entity_acris — pre-joined ACRIS party+transaction index"
```

---

### Task 2: Create mv_city_averages — pre-computed citywide stats

**Files:**
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py`

Both `network.py` (WATCHDOG_CITY_AVERAGES_SQL) and `safety.py` (CITY_AVERAGES_SQL) do full GROUP BY scans on hpd_violations (10.9M) and nypd_complaints/arrests. Pre-compute these.

- [ ] **Step 1: Add the asset**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_city_averages"]),
    group_name="foundation",
    description=(
        "Pre-computed citywide averages: violations per building, crimes/arrests per precinct. "
        "Replaces full GROUP BY scans on hpd_violations (10.9M) and nypd tables."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "hpd_violations"]),
        dg.AssetKey(["public_safety", "nypd_arrests_historic"]),
        dg.AssetKey(["public_safety", "nypd_complaints_historic"]),
    ],
)
def mv_city_averages(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_city_averages")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_city_averages AS
            WITH building_violations AS (
                SELECT bbl,
                       COUNT(*) AS v_cnt,
                       COUNT(*) FILTER (WHERE violationstatus = 'Open') AS open_cnt
                FROM lake.housing.hpd_violations
                GROUP BY bbl
                HAVING COUNT(*) > 0
            ),
            arrest_totals AS (
                SELECT arrest_precinct AS pct, COUNT(*) AS arrests
                FROM lake.public_safety.nypd_arrests_historic
                WHERE TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
                GROUP BY arrest_precinct
            ),
            crime_totals AS (
                SELECT addr_pct_cd AS pct, COUNT(*) AS crimes
                FROM lake.public_safety.nypd_complaints_historic
                WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
                GROUP BY addr_pct_cd
            )
            SELECT
                'building' AS scope,
                AVG(v_cnt) AS avg_violations_per_bldg,
                AVG(open_cnt) AS avg_open_violations_per_bldg,
                NULL::DOUBLE AS avg_arrests,
                NULL::DOUBLE AS avg_crimes,
                NULL::DOUBLE AS avg_arrest_rate_per_1k
            FROM building_violations
            UNION ALL
            SELECT
                'precinct' AS scope,
                NULL, NULL,
                AVG(a.arrests),
                AVG(c.crimes),
                AVG(CASE WHEN c.crimes > 0 THEN a.arrests * 1000.0 / c.crimes END)
            FROM arrest_totals a
            JOIN crime_totals c ON a.pct = c.pct
        """)
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_city_averages in %ss", elapsed)
        return dg.MaterializeResult(
            metadata={"elapsed_s": dg.MetadataValue.float(elapsed)}
        )
    finally:
        conn.close()
```

- [ ] **Step 2: Commit**

```bash
git add src/dagster_pipeline/defs/materialized_view_assets.py
git commit -m "feat: add mv_city_averages — pre-computed violation + crime averages"
```

---

### Task 3: Create mv_pctile_violations and mv_pctile_311

**Files:**
- Modify: `src/dagster_pipeline/defs/materialized_view_assets.py`

The address_report runs `PERCENT_RANK() OVER (ORDER BY cnt)` on the ENTIRE hpd_violations table (10.9M rows) and n311_service_requests (30M+) on every call — just to get one percentile value for one BBL/ZIP.

- [ ] **Step 1: Add mv_pctile_violations**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_pctile_violations"]),
    group_name="foundation",
    description=(
        "Pre-computed violation percentiles by BBL. Replaces PERCENT_RANK() window scan "
        "over 10.9M hpd_violations rows on every address_report call."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[
        dg.AssetKey(["housing", "hpd_violations"]),
        dg.AssetKey(["housing", "hpd_complaints"]),
    ],
)
def mv_pctile_violations(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_pctile_violations")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_pctile_violations AS
            WITH v_counts AS (
                SELECT LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS bbl,
                       COUNT(*) AS violation_cnt
                FROM lake.housing.hpd_violations
                WHERE bbl IS NOT NULL
                GROUP BY 1
            ),
            c_counts AS (
                SELECT LPAD(TRY_CAST(TRY_CAST(bbl AS DOUBLE) AS BIGINT)::VARCHAR, 10, '0') AS bbl,
                       COUNT(DISTINCT complaint_id) AS complaint_cnt
                FROM lake.housing.hpd_complaints
                WHERE bbl IS NOT NULL
                GROUP BY 1
            )
            SELECT
                COALESCE(v.bbl, c.bbl) AS bbl,
                COALESCE(v.violation_cnt, 0) AS violation_cnt,
                PERCENT_RANK() OVER (ORDER BY COALESCE(v.violation_cnt, 0)) AS violation_pctile,
                COALESCE(c.complaint_cnt, 0) AS complaint_cnt,
                PERCENT_RANK() OVER (ORDER BY COALESCE(c.complaint_cnt, 0)) AS complaint_pctile
            FROM v_counts v
            FULL OUTER JOIN c_counts c ON v.bbl = c.bbl
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_pctile_violations"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_pctile_violations: %s rows in %ss", f"{row_count:,}", elapsed)
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "elapsed_s": dg.MetadataValue.float(elapsed),
            }
        )
    finally:
        conn.close()
```

- [ ] **Step 2: Add mv_pctile_311**

```python
@dg.asset(
    key=dg.AssetKey(["foundation", "mv_pctile_311"]),
    group_name="foundation",
    description=(
        "Pre-computed 311 complaint percentiles by ZIP. Replaces PERCENT_RANK() window "
        "scan over 30M+ n311_service_requests on every address_report call."
    ),
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager(),
    deps=[dg.AssetKey(["social_services", "n311_service_requests"])],
)
def mv_pctile_311(context) -> dg.MaterializeResult:
    conn = _connect_ducklake()
    t0 = time.time()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS lake.foundation")
        conn.execute("DROP TABLE IF EXISTS lake.foundation.mv_pctile_311")
        conn.execute("""
            CREATE TABLE lake.foundation.mv_pctile_311 AS
            WITH zip_counts AS (
                SELECT incident_zip AS zipcode,
                       COUNT(*) AS complaint_cnt
                FROM lake.social_services.n311_service_requests
                WHERE incident_zip IS NOT NULL
                  AND LENGTH(TRIM(incident_zip)) = 5
                GROUP BY incident_zip
            )
            SELECT
                zipcode,
                complaint_cnt,
                PERCENT_RANK() OVER (ORDER BY complaint_cnt) AS complaint_pctile
            FROM zip_counts
        """)
        row_count = conn.execute(
            "SELECT COUNT(*) FROM lake.foundation.mv_pctile_311"
        ).fetchone()[0]
        elapsed = round(time.time() - t0, 1)
        context.log.info("mv_pctile_311: %s rows in %ss", f"{row_count:,}", elapsed)
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
git commit -m "feat: add mv_pctile_violations + mv_pctile_311 — pre-computed percentile rankings"
```

---

### Task 4: Register new MVs in Dagster definitions

**Files:**
- Modify: `src/dagster_pipeline/definitions.py`

- [ ] **Step 1: Add imports**

After the existing `from dagster_pipeline.defs.materialized_view_assets import` line, add the new MVs:

```python
from dagster_pipeline.defs.materialized_view_assets import (
    mv_building_hub, mv_acris_deeds, mv_zip_stats, mv_crime_precinct, mv_corp_network,
    mv_entity_acris, mv_city_averages, mv_pctile_violations, mv_pctile_311,
)
```

- [ ] **Step 2: Add to all_assets**

Add `mv_entity_acris, mv_city_averages, mv_pctile_violations, mv_pctile_311` to the `all_assets` list.

- [ ] **Step 3: Add to automation sensor target**

Update `mv_automation_sensor` to include the new asset keys:

```python
mv_automation_sensor = dg.AutomationConditionSensorDefinition(
    name="mv_auto_materialize",
    target=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "mv_building_hub"]),
        dg.AssetKey(["foundation", "mv_acris_deeds"]),
        dg.AssetKey(["foundation", "mv_zip_stats"]),
        dg.AssetKey(["foundation", "mv_crime_precinct"]),
        dg.AssetKey(["foundation", "mv_corp_network"]),
        dg.AssetKey(["foundation", "mv_entity_acris"]),
        dg.AssetKey(["foundation", "mv_city_averages"]),
        dg.AssetKey(["foundation", "mv_pctile_violations"]),
        dg.AssetKey(["foundation", "mv_pctile_311"]),
    ),
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
```

- [ ] **Step 4: Add to materialized_views_job selection**

Update the job to include new MVs:

```python
materialized_views_job = dg.define_asset_job(
    name="materialized_views_rebuild",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["foundation", "mv_building_hub"]),
        dg.AssetKey(["foundation", "mv_acris_deeds"]),
        dg.AssetKey(["foundation", "mv_zip_stats"]),
        dg.AssetKey(["foundation", "mv_crime_precinct"]),
        dg.AssetKey(["foundation", "mv_corp_network"]),
        dg.AssetKey(["foundation", "mv_entity_acris"]),
        dg.AssetKey(["foundation", "mv_city_averages"]),
        dg.AssetKey(["foundation", "mv_pctile_violations"]),
        dg.AssetKey(["foundation", "mv_pctile_311"]),
    ),
)
```

- [ ] **Step 5: Verify Dagster loads**

```bash
cd ~/Desktop/dagster-pipeline
uv run python -c "from dagster_pipeline.definitions import defs; print(f'Loaded {len(defs.get_all_asset_specs())} assets')"
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/definitions.py
git commit -m "feat: register 4 new MVs in Dagster definitions + automation sensor"
```

---

### Task 5: Materialize all new MVs

**Files:** None (operational)

- [ ] **Step 1: Materialize all 4 new MVs**

```bash
cd ~/Desktop/dagster-pipeline
# Set env vars (same pattern as address_lookup materialization)
export CATALOG_URL="$(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc 2>/dev/null | grep 'CATALOG=' | sed 's/^[^=]*=//' | sed 's/common-ground-postgres-1/178.156.228.119/')"
export MINIO_SECRET="$(sops --decrypt --input-type dotenv --output-type dotenv .env.secrets.enc 2>/dev/null | grep 'SECRET_ACCESS_KEY=' | head -1 | sed 's/^[^=]*=//')"

DAGSTER_HOME=/tmp/dagster-home \
  DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG="$CATALOG_URL" \
  S3_ENDPOINT="178.156.228.119:9000" \
  DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID="minioadmin" \
  DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY="$MINIO_SECRET" \
  uv run dagster asset materialize \
    --select 'foundation/mv_entity_acris foundation/mv_city_averages foundation/mv_pctile_violations foundation/mv_pctile_311' \
    -m dagster_pipeline.definitions
```

Expected: each MV materializes in 30-120 seconds. The ACRIS join will be the slowest.

- [ ] **Step 2: Verify table contents**

```bash
# Via MCP query tool after server restart
curl ... query "SELECT COUNT(*) FROM lake.foundation.mv_entity_acris"
curl ... query "SELECT COUNT(*) FROM lake.foundation.mv_city_averages"
curl ... query "SELECT COUNT(*) FROM lake.foundation.mv_pctile_violations"
curl ... query "SELECT COUNT(*) FROM lake.foundation.mv_pctile_311"
```

---

### Task 6: Wire MCP tools to use new MVs

**Files:**
- Modify: `infra/duckdb-server/tools/entity.py`
- Modify: `infra/duckdb-server/tools/network.py`
- Modify: `infra/duckdb-server/tools/safety.py`
- Modify: `infra/duckdb-server/tools/_address_queries.py`

- [ ] **Step 1: entity.py — use mv_entity_acris**

Replace the ACRIS query in `_entity_xray` from:
```sql
SELECT p.name AS party_name, p.party_type, m.doc_type, ...
FROM lake.housing.acris_parties p
JOIN lake.housing.acris_master m ON p.document_id = m.document_id
JOIN lake.housing.acris_legals l ON p.document_id = l.document_id
WHERE UPPER(p.name) LIKE ?
```
to:
```sql
SELECT party_name, party_type, doc_type, amount, document_date, bbl, street_name, unit
FROM lake.foundation.mv_entity_acris
WHERE party_name LIKE ?
ORDER BY document_date DESC
LIMIT 20
```

- [ ] **Step 2: network.py — use mv_city_averages**

Replace `WATCHDOG_CITY_AVERAGES_SQL` from the full GROUP BY to:
```sql
SELECT avg_violations_per_bldg, avg_open_violations_per_bldg
FROM lake.foundation.mv_city_averages
WHERE scope = 'building'
```

- [ ] **Step 3: safety.py — use mv_city_averages**

Replace `CITY_AVERAGES_SQL` from the full GROUP BY to:
```sql
SELECT avg_arrests, avg_crimes, avg_arrest_rate_per_1k
FROM lake.foundation.mv_city_averages
WHERE scope = 'precinct'
```

- [ ] **Step 4: _address_queries.py — use mv_pctile_violations and mv_pctile_311**

Replace `pctile_violations` query from:
```sql
WITH bbl_counts AS (SELECT bbl, COUNT(*) ... FROM lake.housing.hpd_violations GROUP BY bbl),
     ranked AS (SELECT bbl, cnt, PERCENT_RANK() OVER (...) FROM bbl_counts)
SELECT pctile FROM ranked WHERE ... = ?
```
to:
```sql
SELECT violation_pctile AS pctile FROM lake.foundation.mv_pctile_violations WHERE bbl = ?
```

Replace `pctile_complaints` similarly:
```sql
SELECT complaint_pctile AS pctile FROM lake.foundation.mv_pctile_violations WHERE bbl = ?
```

Replace `pctile_311` similarly:
```sql
SELECT complaint_pctile AS pctile FROM lake.foundation.mv_pctile_311 WHERE zipcode = ?
```

- [ ] **Step 5: Commit all**

```bash
git add infra/duckdb-server/tools/entity.py infra/duckdb-server/tools/network.py \
        infra/duckdb-server/tools/safety.py infra/duckdb-server/tools/_address_queries.py
git commit -m "perf: wire MCP tools to use new MVs — eliminates 4 full table scans"
```

---

### Task 7: Deploy and benchmark

**Files:** None (operational)

- [ ] **Step 1: Deploy MCP server changes to Hetzner**

```bash
cd ~/Desktop/dagster-pipeline
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
  --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
  --exclude 'model/' --exclude 'tests/' \
  infra/duckdb-server/ \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
ssh -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose build duckdb-server && docker compose up -d duckdb-server"
```

- [ ] **Step 2: Benchmark the fixed tools**

Test each tool and compare to pre-MV times:
- `entity(name="BLACKSTONE")` — ACRIS was 33s, should be <2s with MV
- `network(name="KUSHNER")` — city_averages was 3-10s, should be <0.1s
- `safety(location="14")` — city_averages was 3s, should be <0.1s
- `building(identifier="200 E 10th St, Manhattan")` — address_report percentiles should drop

Expected results:
| Tool | Before MV | After MV |
|------|-----------|----------|
| entity_xray | 78s (ACRIS=33s) | ~45s (ACRIS<2s, others unchanged) |
| network | 13s | ~5s |
| safety | 4s | ~2s |
| address_report | percentiles 15s+ | percentiles <0.5s |
