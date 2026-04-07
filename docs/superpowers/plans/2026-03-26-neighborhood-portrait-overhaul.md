# Neighborhood Portrait Overhaul — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild `neighborhood_portrait` from a 5-dimension text dump into a graded, LLM-optimized neighborhood data product with 10 dimensions, citywide percentile rankings, and pre-computed narrative hooks.

**Architecture:** The tool lives in `/opt/common-ground/duckdb-server/mcp_server.py` (9,680 lines, single file). All edits happen in this file on the Hetzner server (178.156.228.119, SSH alias `hetzner`). The tool queries the DuckLake lake at `/data/ducklake/lake.db` via DuckDB. The pattern is: SQL constants at the top, tool function using `_execute(db, SQL, params)` returning `ToolResult(content=text, structured_content=dict)`. After editing, restart the container: `docker restart common-ground-duckdb-server-1`.

**Tech Stack:** Python 3.12, FastMCP, DuckDB, DuckLake (Postgres metadata + MinIO S3 data)

**Key constraint:** All NYPD crime data uses precinct (not ZIP). A zip-to-precinct crosswalk via 311 data already exists in `neighborhood_compare` and must be reused.

**Data gap:** No ACS demographics or IRS income by ZIP exists in the lake yet. Task 1 ingests ACS ZCTA data. Until then, demographics/income sections use what's available (PLUTO owner data, community district profiles derived from 311).

---

## File Structure

All changes are in one file on the server:

- **Modify:** `/opt/common-ground/duckdb-server/mcp_server.py`
  - Lines ~5648-5880: Replace existing `PORTRAIT_*_SQL` constants and `neighborhood_portrait` function
  - Add new SQL constants before the function
  - Add `_percentile_grade()` and `_compute_headline()` helper functions near the existing helpers (~line 420)

No new files. Follows existing single-file pattern.

---

## Task 1: Ingest ACS 5-Year ZCTA Demographics

**Why:** The portrait needs population, median income, race/ethnicity, poverty rate, median rent, and homeownership rate by ZIP. The Census ACS 5-Year ZCTA data is the canonical source. Without it, the portrait is missing its most important dimension.

**Files:**
- Create: `src/dagster_pipeline/sources/census_zcta.py` (dlt source)
- Modify: `src/dagster_pipeline/defs/federal_assets.py` (add asset)
- Modify: `src/dagster_pipeline/definitions.py` (register)

- [ ] **Step 1: Research the ACS 5-Year API endpoint**

The Census Bureau API serves ACS 5-Year data at ZCTA level. The URL is built dynamically from the `VARIABLES` dict in the source code — do NOT hardcode it.

Key variables:
| Variable | Description |
|----------|------------|
| B01003_001E | Total population |
| B01002_001E | Median age |
| B19013_001E | Median household income |
| B25064_001E | Median gross rent |
| B25003_001E | Total occupied housing units |
| B25003_002E | Owner-occupied units |
| B17001_001E | Population for poverty status |
| B17001_002E | Below poverty level |
| B02001_001E | Race universe |
| B02001_002E | White alone |
| B02001_003E | Black alone |
| B02001_005E | Asian alone |
| B03003_001E | Ethnicity universe |
| B03003_003E | Hispanic/Latino |
| B25001_001E | Total housing units |
| B25002_003E | Vacant units |
| B08303_001E | Commute universe |
| B08303_013E | Commute 60+ min |
| B25077_001E | Median home value |

Verify the API works (no API key required for small queries):
```bash
curl -s "https://api.census.gov/data/2023/acs/acs5?get=NAME,B01003_001E,B19013_001E&for=zip%20code%20tabulation%20area:10003&in=state:36" | head -5
```

- [ ] **Step 2: Write the dlt source**

```python
# src/dagster_pipeline/sources/census_zcta.py
"""ACS 5-Year ZCTA demographics for NYC ZIP codes."""

import dlt
import requests

ACS_YEAR = 2023
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"

# All NYC ZCTAs (10001-10499, 10800-10899, 11001-11499, 11690-11697)
NYC_ZIP_PREFIXES = {"100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116"}

VARIABLES = {
    "B01003_001E": "total_population",
    "B01002_001E": "median_age",
    "B19013_001E": "median_household_income",
    "B25064_001E": "median_gross_rent",
    "B25003_001E": "total_occupied_units",
    "B25003_002E": "owner_occupied_units",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "below_poverty",
    "B02001_001E": "race_universe",
    "B02001_002E": "white_alone",
    "B02001_003E": "black_alone",
    "B02001_005E": "asian_alone",
    "B03003_001E": "ethnicity_universe",
    "B03003_003E": "hispanic_latino",
    "B25001_001E": "total_housing_units",
    "B25002_003E": "vacant_units",
    "B08303_001E": "commute_universe",
    "B08303_013E": "commute_60_plus_min",
    "B25077_001E": "median_home_value",
}


@dlt.source(name="acs_zcta_demographics")
def acs_zcta_source():
    @dlt.resource(
        name="acs_zcta_demographics",
        write_disposition="replace",
        columns={"zcta": {"data_type": "text", "nullable": False}},
    )
    def acs_zcta_demographics():
        var_codes = ",".join(VARIABLES.keys())
        url = f"{ACS_BASE}?get=NAME,{var_codes}&for=zip%20code%20tabulation%20area:*&in=state:36"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        headers = data[0]

        for row in data[1:]:
            record = dict(zip(headers, row))
            zcta = record.get("zip code tabulation area", "")
            if zcta[:3] not in NYC_ZIP_PREFIXES:
                continue

            out = {"zcta": zcta, "acs_year": ACS_YEAR, "name": record.get("NAME", "")}
            for code, col_name in VARIABLES.items():
                val = record.get(code)
                if val and val not in ("-666666666", "-999999999", "null", "None"):
                    try:
                        out[col_name] = float(val)
                    except (ValueError, TypeError):
                        out[col_name] = None
                else:
                    out[col_name] = None
            yield out

    return acs_zcta_demographics


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="acs_zcta",
        destination="duckdb",
        dataset_name="federal",
    )
    load_info = pipeline.run(acs_zcta_source())
    print(load_info)
```

- [ ] **Step 3: Add Dagster asset**

Add to `src/dagster_pipeline/defs/federal_assets.py`:
```python
@asset(
    key_prefix=["federal"],
    group_name="federal",
    kinds={"dlt", "duckdb"},
    description="ACS 5-Year ZCTA demographics for NYC ZIP codes",
)
def acs_zcta_demographics(context):
    from dagster_pipeline.sources.census_zcta import acs_zcta_source
    # ... standard dlt asset pattern matching existing assets in this file
```

- [ ] **Step 4: Test locally via dlt CLI**

```bash
cd ~/Desktop/dagster-pipeline
uv run python src/dagster_pipeline/sources/census_zcta.py
```
Expected: ~200 NYC ZCTAs loaded into DuckDB.

- [ ] **Step 5: Deploy and materialize**

```bash
./deploy.sh
# Then materialize via Dagster UI or CLI
ssh hetzner "cd /opt/common-ground && docker compose exec dagster-code dagster asset materialize --select federal/acs_zcta_demographics"
```

- [ ] **Step 6: Verify data on server**

```bash
ssh hetzner "docker exec common-ground-duckdb-server-1 python3 -c \"
import duckdb
db = duckdb.connect('/data/ducklake/lake.db', read_only=True)
print(db.sql('SELECT zcta, total_population, median_household_income, median_gross_rent FROM lake.federal.acs_zcta_demographics WHERE zcta = \\\"10003\\\" LIMIT 1').fetchall())
\""
```
Expected: One row with population ~50K, income ~$90K, rent ~$1800 for East Village.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_pipeline/sources/census_zcta.py src/dagster_pipeline/defs/federal_assets.py src/dagster_pipeline/definitions.py
git commit -m "feat: add ACS 5-Year ZCTA demographics ingest for neighborhood portrait"
```

---

## Task 2: Add Percentile Grading Engine

**Why:** Every competitor (Niche, AreaVibes, NeighborhoodScout) presents letter grades. An LLM narrates "B for safety, 68th percentile" far better than raw crime counts. This engine computes citywide percentiles for any metric across all NYC ZIPs and maps to letter grades.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (~line 420, near `_execute`)

- [ ] **Step 1: Write the percentile and grading helpers**

Add these after the `_execute` function (~line 430):

```python
# ── Percentile grading engine ──────────────────────────────────────

_GRADE_THRESHOLDS = [
    (90, "A+"), (80, "A"), (70, "A-"),
    (65, "B+"), (55, "B"), (45, "B-"),
    (40, "C+"), (30, "C"), (20, "C-"),
    (15, "D+"), (10, "D"), (5, "D-"),
    (0, "F"),
]


def _percentile_grade(value: float, all_values: list[float], higher_is_better: bool = True) -> dict:
    """Compute percentile rank and letter grade for a value against all NYC ZIPs.

    Args:
        value: The metric value for this ZIP
        all_values: The same metric across all NYC ZIPs
        higher_is_better: True for income/parks, False for crime/noise
    Returns:
        {"value": float, "percentile": int, "grade": str}
    """
    if not all_values or value is None:
        return {"value": value, "percentile": None, "grade": None}

    sorted_vals = sorted(v for v in all_values if v is not None)
    if not sorted_vals:
        return {"value": value, "percentile": None, "grade": None}

    count_below = sum(1 for v in sorted_vals if v < value)
    percentile = round(count_below / len(sorted_vals) * 100)

    if not higher_is_better:
        percentile = 100 - percentile

    grade = "F"
    for threshold, letter in _GRADE_THRESHOLDS:
        if percentile >= threshold:
            grade = letter
            break

    return {"value": round(value, 2), "percentile": percentile, "grade": grade}


def _trend_direction(current: float, previous: float) -> str:
    """Return 'improving', 'stable', or 'declining' based on % change.
    NOTE: Not used in initial portrait release — reserved for future trend dimensions.
    """
    if current is None or previous is None or previous == 0:
        return "unknown"
    pct_change = (current - previous) / abs(previous)
    if pct_change > 0.05:
        return "improving"
    elif pct_change < -0.05:
        return "declining"
    return "stable"
```

- [ ] **Step 2: Test the helper manually**

```bash
ssh hetzner "docker exec common-ground-duckdb-server-1 python3 -c \"
# Quick test of grading logic
values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
value = 75
count_below = sum(1 for v in values if v < value)
percentile = round(count_below / len(values) * 100)
print(f'Value: {value}, Percentile: {percentile}')
# Expected: 70th percentile -> A-
\""
```

- [ ] **Step 3: Commit**

```bash
# Edit is on the server via SSH, commit there
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'feat: add percentile grading engine for neighborhood portrait'"
```

---

## Task 3: Build ZIP Crosswalk Materialized View

**Why:** Crime data (NYPD) uses precinct, not ZIP. Education uses DBN (school code), not ZIP. Air quality uses UHF42 geo areas. The portrait needs a reusable zip-to-precinct crosswalk. The `neighborhood_compare` CTE does this at query time via 311 data — but it's expensive to recompute per call. We'll compute it once and store it.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (add to lifespan/startup)

- [ ] **Step 1: Add crosswalk SQL as a startup materialization**

Add a startup function that creates temporary crosswalk tables when the server boots (in the lifespan context, after the DB is connected):

```python
CROSSWALK_ZIP_PRECINCT_SQL = """
CREATE OR REPLACE TEMP TABLE zip_precinct_crosswalk AS
WITH ranked AS (
    SELECT
        incident_zip AS zipcode,
        REGEXP_EXTRACT(police_precinct, '\\d+') AS precinct,
        COUNT(*) AS weight,
        ROW_NUMBER() OVER (PARTITION BY incident_zip ORDER BY COUNT(*) DESC) AS rn
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IS NOT NULL
      AND LENGTH(incident_zip) = 5
      AND police_precinct NOT IN ('Unspecified', '')
      AND police_precinct IS NOT NULL
    GROUP BY 1, 2
)
SELECT zipcode, precinct
FROM ranked
WHERE rn = 1
"""

CROSSWALK_ZIP_POPULATION_SQL = """
CREATE OR REPLACE TEMP TABLE zip_population AS
SELECT
    zcta AS zipcode,
    total_population,
    median_household_income,
    median_gross_rent,
    median_age,
    CASE WHEN total_occupied_units > 0
         THEN ROUND(100.0 * owner_occupied_units / total_occupied_units, 1)
         ELSE NULL END AS homeownership_pct,
    CASE WHEN poverty_universe > 0
         THEN ROUND(100.0 * below_poverty / poverty_universe, 1)
         ELSE NULL END AS poverty_rate,
    CASE WHEN race_universe > 0 THEN ROUND(100.0 * white_alone / race_universe, 1) ELSE NULL END AS pct_white,
    CASE WHEN race_universe > 0 THEN ROUND(100.0 * black_alone / race_universe, 1) ELSE NULL END AS pct_black,
    CASE WHEN race_universe > 0 THEN ROUND(100.0 * asian_alone / race_universe, 1) ELSE NULL END AS pct_asian,
    CASE WHEN ethnicity_universe > 0 THEN ROUND(100.0 * hispanic_latino / ethnicity_universe, 1) ELSE NULL END AS pct_hispanic,
    total_housing_units,
    CASE WHEN total_housing_units > 0
         THEN ROUND(100.0 * vacant_units / total_housing_units, 1)
         ELSE NULL END AS vacancy_rate,
    median_home_value
FROM lake.federal.acs_zcta_demographics
"""
```

- [ ] **Step 2: Execute crosswalks at server startup**

In the lifespan function (find the existing one), add after the DB connection:

```python
# Materialize crosswalks for neighborhood tools
try:
    db.execute(CROSSWALK_ZIP_PRECINCT_SQL)
    db.execute(CROSSWALK_ZIP_POPULATION_SQL)
    log.info("Crosswalk tables materialized")
except Exception as e:
    log.warning(f"Crosswalk materialization failed (ACS may not be ingested yet): {e}")
```

- [ ] **Step 3: Verify crosswalks work**

NOTE: Temp tables are session-scoped in DuckDB — they won't be visible from a separate `duckdb.connect()` call. Verify via the MCP tool itself or by testing the startup logs.

```bash
# Restart and check server logs for "Crosswalk tables materialized"
ssh hetzner "docker restart common-ground-duckdb-server-1 && sleep 60 && docker logs common-ground-duckdb-server-1 --tail 5 2>&1 | grep -i crosswalk"

# Alternatively, verify via MCP tool call (the portrait function uses the crosswalk):
curl -s -X POST https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":"sql_query","arguments":{"sql":"SELECT * FROM zip_precinct_crosswalk WHERE zipcode = '\''10003'\'' LIMIT 1"}}}'
```
Expected: Precinct 9 for ZIP 10003 (9th Precinct covers East Village)

- [ ] **Step 4: Commit**

```bash
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'feat: add zip-precinct and zip-population crosswalk tables'"
```

---

## Task 4: Write the New Portrait SQL Constants

**Why:** Replace the existing 8 SQL constants with 15 that cover all dimensions. Each query is designed to return one section of the portrait, joinable by ZIP.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (replace lines ~5648-5731)

- [ ] **Step 1: Replace existing PORTRAIT SQL constants**

Delete the existing `PORTRAIT_CUISINE_SQL` through `PORTRAIT_BIZ_SQL` constants and replace with:

```python
# ── Neighborhood Portrait SQL ──────────────────────────────────────

# Section 1: Demographics (from ACS ZCTA materialized view)
PORTRAIT_DEMOGRAPHICS_SQL = """
SELECT * FROM zip_population WHERE zipcode = ?
"""

# Section 2: Demographics — all zips for percentile computation
PORTRAIT_DEMOGRAPHICS_ALL_SQL = """
SELECT zipcode, total_population, median_household_income, median_gross_rent,
       poverty_rate, median_home_value
FROM zip_population
WHERE total_population IS NOT NULL AND total_population > 0
"""

# Section 3: Housing quality — HPD violations per unit
PORTRAIT_HOUSING_QUALITY_SQL = """
WITH units AS (
    SELECT postcode AS zipcode, SUM(TRY_CAST(unitsres AS INT)) AS total_units
    FROM lake.city_government.pluto
    WHERE postcode = ?
    GROUP BY postcode
),
violations AS (
    SELECT zip AS zipcode,
           COUNT(*) AS total_violations,
           COUNT(*) FILTER (WHERE currentstatus != 'CLOSE') AS open_violations,
           COUNT(*) FILTER (WHERE class = 'C') AS class_c_violations
    FROM lake.housing.hpd_violations
    WHERE zip = ?
    GROUP BY zip
),
complaints AS (
    SELECT zip AS zipcode,
           COUNT(*) FILTER (
               WHERE TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
           ) AS complaints_1yr
    FROM lake.housing.hpd_complaints
    WHERE zip = ?
    GROUP BY zip
)
SELECT u.zipcode, u.total_units,
       v.total_violations, v.open_violations, v.class_c_violations,
       c.complaints_1yr,
       CASE WHEN u.total_units > 0
            THEN ROUND(1000.0 * v.open_violations / u.total_units, 1)
            ELSE NULL END AS open_violations_per_1k_units,
       CASE WHEN u.total_units > 0
            THEN ROUND(1000.0 * c.complaints_1yr / u.total_units, 1)
            ELSE NULL END AS complaints_per_1k_units
FROM units u
LEFT JOIN violations v ON u.zipcode = v.zipcode
LEFT JOIN complaints c ON u.zipcode = c.zipcode
"""

# Section 3b: Housing quality — all zips for percentile
PORTRAIT_HOUSING_ALL_SQL = """
WITH units AS (
    SELECT postcode AS zipcode, SUM(TRY_CAST(unitsres AS INT)) AS total_units
    FROM lake.city_government.pluto
    GROUP BY postcode
    HAVING SUM(TRY_CAST(unitsres AS INT)) > 100
),
violations AS (
    SELECT zip AS zipcode, COUNT(*) FILTER (WHERE currentstatus != 'CLOSE') AS open_violations
    FROM lake.housing.hpd_violations
    GROUP BY zip
)
SELECT u.zipcode,
       CASE WHEN u.total_units > 0
            THEN ROUND(1000.0 * COALESCE(v.open_violations, 0) / u.total_units, 1)
            ELSE NULL END AS open_violations_per_1k_units
FROM units u LEFT JOIN violations v ON u.zipcode = v.zipcode
"""

# Section 4: Safety — crime via precinct crosswalk
# NOTE: Uses UNION (not UNION ALL) to deduplicate across YTD and historic tables,
# since YTD data overlaps with recent historic data. Both tables are date-filtered
# to the same 365-day window to prevent double-counting.
PORTRAIT_SAFETY_SQL = """
WITH crimes AS (
    SELECT cmplnt_num, law_cat_cd, ofns_desc
    FROM lake.public_safety.nypd_complaints_ytd
    WHERE addr_pct_cd = (SELECT precinct FROM zip_precinct_crosswalk WHERE zipcode = ?)
      AND TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    UNION
    SELECT cmplnt_num, law_cat_cd, ofns_desc
    FROM lake.public_safety.nypd_complaints_historic
    WHERE addr_pct_cd = (SELECT precinct FROM zip_precinct_crosswalk WHERE zipcode = ?)
      AND TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
)
SELECT
    COUNT(*) AS total_crimes,
    COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
    COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors,
    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%') AS assaults,
    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%robbery%') AS robberies,
    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%burglary%') AS burglaries,
    COUNT(*) FILTER (WHERE ofns_desc ILIKE '%larceny%') AS larcenies
FROM crimes
"""

# Section 4b: Safety — all precincts for percentile
PORTRAIT_SAFETY_ALL_SQL = """
WITH crimes AS (
    SELECT addr_pct_cd,
           COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies
    FROM lake.public_safety.nypd_complaints_ytd
    GROUP BY addr_pct_cd
)
SELECT zp.zipcode, c.felonies
FROM zip_precinct_crosswalk zp
JOIN crimes c ON zp.precinct = c.addr_pct_cd
"""

# Section 5: Collisions (has native ZIP)
PORTRAIT_COLLISIONS_SQL = """
SELECT
    COUNT(*) AS total_collisions,
    SUM(TRY_CAST(number_of_persons_injured AS INT)) AS persons_injured,
    SUM(TRY_CAST(number_of_persons_killed AS INT)) AS persons_killed,
    SUM(TRY_CAST(number_of_pedestrians_injured AS INT)) AS pedestrians_injured
FROM lake.public_safety.motor_vehicle_collisions
WHERE zip_code = ?
  AND TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
"""

# Section 6: Building stock (keep existing, enhanced)
PORTRAIT_BUILDINGS_SQL = """
SELECT
    COUNT(*) AS total_buildings,
    AVG(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS avg_year_built,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    MEDIAN(TRY_CAST(assesstot AS DOUBLE)) AS median_assessed,
    COUNT(*) FILTER (WHERE landmark IS NOT NULL AND landmark != '') AS landmarks,
    COUNT(*) FILTER (WHERE histdist IS NOT NULL AND histdist != '') AS in_hist_district,
    SUM(TRY_CAST(unitsres AS INT)) AS total_res_units,
    COUNT(*) FILTER (WHERE TRY_CAST(yearbuilt AS INT) < 1946) AS pre_war,
    COUNT(*) FILTER (WHERE TRY_CAST(yearbuilt AS INT) BETWEEN 1946 AND 1999) AS post_war,
    COUNT(*) FILTER (WHERE TRY_CAST(yearbuilt AS INT) >= 2000) AS new_construction
FROM lake.city_government.pluto
WHERE postcode = ?
"""

# Section 7: Cuisine fingerprint (keep existing)
PORTRAIT_CUISINE_SQL = """
SELECT cuisine_description, COUNT(DISTINCT camis) AS restaurants
FROM lake.health.restaurant_inspections
WHERE zipcode = ? AND grade IS NOT NULL
GROUP BY cuisine_description
ORDER BY restaurants DESC LIMIT 10
"""

PORTRAIT_CUISINE_CITY_SQL = """
SELECT cuisine_description,
       COUNT(DISTINCT camis) AS restaurants,
       COUNT(DISTINCT camis) * 1.0 / NULLIF(COUNT(DISTINCT zipcode), 0) AS avg_per_zip
FROM lake.health.restaurant_inspections
WHERE grade IS NOT NULL
GROUP BY cuisine_description
HAVING COUNT(DISTINCT camis) > 50
ORDER BY restaurants DESC LIMIT 30
"""

PORTRAIT_RESTAURANT_GRADES_SQL = """
WITH latest AS (
    SELECT camis, grade,
           ROW_NUMBER() OVER (PARTITION BY camis ORDER BY grade_date DESC) AS rn
    FROM lake.health.restaurant_inspections
    WHERE zipcode = ? AND grade IN ('A','B','C')
)
SELECT grade, COUNT(*) AS cnt FROM latest WHERE rn = 1 GROUP BY grade ORDER BY grade
"""

# Section 8: 311 quality of life
PORTRAIT_311_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
GROUP BY complaint_type ORDER BY cnt DESC LIMIT 10
"""

PORTRAIT_311_NOISE_SQL = """
SELECT
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%noise%') AS noise_complaints,
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%heat%' OR complaint_type ILIKE '%hot water%') AS heat_complaints,
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%dirty%' OR complaint_type ILIKE '%sanitation%') AS sanitation_complaints,
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%rodent%' OR complaint_type ILIKE '%rat%') AS rodent_complaints,
    COUNT(*) AS total_311
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
"""

PORTRAIT_311_ALL_NOISE_SQL = """
SELECT incident_zip AS zipcode,
       COUNT(*) FILTER (WHERE complaint_type ILIKE '%noise%') AS noise_complaints
FROM lake.social_services.n311_service_requests
WHERE incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
  AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
GROUP BY incident_zip
"""

# Section 9: Business mix (keep existing)
PORTRAIT_BIZ_SQL = """
SELECT industry, COUNT(*) AS cnt
FROM lake.business.issued_licenses
WHERE zip = ? AND license_status = 'Active'
GROUP BY industry ORDER BY cnt DESC LIMIT 8
"""

# Section 10: Environment — trees, lead
PORTRAIT_TREES_SQL = """
SELECT
    COUNT(*) AS total_trees,
    COUNT(*) FILTER (WHERE health = 'Good') AS healthy_trees,
    COUNT(*) FILTER (WHERE health = 'Fair') AS fair_trees,
    COUNT(*) FILTER (WHERE health = 'Poor') AS poor_trees,
    MODE(spc_common) AS most_common_species
FROM lake.environment.street_trees
WHERE zipcode = ?
  AND status = 'Alive'
"""

PORTRAIT_TREES_ALL_SQL = """
SELECT zipcode, COUNT(*) AS total_trees
FROM lake.environment.street_trees
WHERE status = 'Alive' AND zipcode IS NOT NULL AND LENGTH(zipcode) = 5
GROUP BY zipcode
"""

PORTRAIT_LEAD_SQL = """
SELECT
    COUNT(*) AS total_service_lines,
    COUNT(*) FILTER (WHERE LOWER(sl_category) LIKE '%lead%') AS lead_lines,
    COUNT(*) FILTER (WHERE LOWER(sl_category) LIKE '%unknown%') AS unknown_lines
FROM lake.environment.lead_service_lines
WHERE zip_code = ?
"""

# Section 11: Transit access
PORTRAIT_TRANSIT_SQL = """
SELECT COUNT(DISTINCT complex_name) AS subway_stations
FROM lake.transportation.mta_stations
WHERE zipcode = ? OR zip_code = ? OR zip = ?
"""

# Section 12: Parks & recreation
PORTRAIT_PARKS_SQL = """
SELECT
    COUNT(*) AS total_parks,
    ROUND(SUM(TRY_CAST(acres AS DOUBLE)), 1) AS total_acres
FROM lake.recreation.properties
WHERE zipcode = ?
"""

# Section 13: Sales trends (last 2 years)
PORTRAIT_SALES_SQL = """
SELECT
    COUNT(*) AS total_sales,
    MEDIAN(TRY_CAST(sale_price AS BIGINT)) FILTER (WHERE TRY_CAST(sale_price AS BIGINT) > 10000) AS median_sale_price,
    AVG(TRY_CAST(sale_price AS DOUBLE)) FILTER (
        WHERE TRY_CAST(sale_price AS BIGINT) > 10000
          AND TRY_CAST(gross_square_feet AS INT) > 0
    ) / NULLIF(AVG(TRY_CAST(gross_square_feet AS DOUBLE)) FILTER (
        WHERE TRY_CAST(sale_price AS BIGINT) > 10000
          AND TRY_CAST(gross_square_feet AS INT) > 0
    ), 0) AS avg_price_per_sqft
FROM lake.housing.rolling_sales
WHERE zip_code = ?
  AND TRY_CAST(sale_date AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
"""
```

- [ ] **Step 2: Verify each SQL works individually**

Test the most complex one (safety with crosswalk):
```bash
ssh hetzner "docker exec common-ground-duckdb-server-1 python3 -c \"
import duckdb
db = duckdb.connect('/data/ducklake/lake.db', read_only=True)
# Create crosswalk first
db.execute('''CREATE OR REPLACE TEMP TABLE zip_precinct_crosswalk AS ...''')
result = db.sql('''SELECT ... FROM PORTRAIT_SAFETY_SQL with param 10003''').fetchall()
print(result)
\""
```

- [ ] **Step 3: Commit**

```bash
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'feat: add 15 SQL constants for neighborhood portrait dimensions'"
```

---

## Task 5: Rewrite the neighborhood_portrait Function

**Why:** The current function returns sparse `structured_content` and builds text line-by-line. The new version returns a rich structured response with grades, distinctive facts, and a headline — optimized for LLM narration.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (replace lines ~5732-5880)

- [ ] **Step 1: Write the new function**

Replace the existing `neighborhood_portrait` function with:

```python
@mcp.tool(annotations=READONLY, tags={"neighborhood"})
def neighborhood_portrait(zipcode: ZIP, ctx: Context) -> ToolResult:
    """Comprehensive neighborhood profile by ZIP — demographics, housing, safety,
    quality of life, amenities, environment, and transit with letter grades and
    citywide percentile rankings. Use this for any neighborhood question by ZIP.
    For comparing multiple ZIPs, use neighborhood_compare([zips]).
    For environmental justice, use environmental_justice(zipcode).
    For gentrification trends, use gentrification_tracker([zips]).
    ZIP: 5 digits. Example: 10003 (East Village), 11201 (Downtown Brooklyn)."""

    t0 = time.time()
    db = ctx.lifespan_context["db"]

    if not zipcode or len(zipcode) != 5 or not zipcode.isdigit():
        raise ToolError("Please provide a valid 5-digit NYC ZIP code.")

    # Look up borough from PLUTO (authoritative) with prefix fallback
    cols_b, rows_b = _execute(db, """
        SELECT CASE borocode
            WHEN '1' THEN 'Manhattan' WHEN '2' THEN 'The Bronx'
            WHEN '3' THEN 'Brooklyn' WHEN '4' THEN 'Queens'
            WHEN '5' THEN 'Staten Island' ELSE 'NYC' END AS borough
        FROM lake.city_government.pluto
        WHERE postcode = ? LIMIT 1
    """, [zipcode])
    if rows_b:
        boro_name = dict(zip(cols_b, rows_b[0]))["borough"]
    else:
        prefix = int(zipcode[:3])
        boro_map = {
            range(100, 103): "Manhattan",
            range(103, 104): "Staten Island",
            range(104, 106): "The Bronx",
            range(112, 113): "Brooklyn",
            range(110, 117): "Queens",
        }
        boro_name = "NYC"
        for rng, name in boro_map.items():
            if prefix in rng:
                boro_name = name
                break

    result = {
        "zipcode": zipcode,
        "borough": boro_name,
        "headline": "",
        "grades": {},
        "distinctive_facts": [],
        "demographics": None,
        "housing": None,
        "safety": None,
        "quality_of_life": None,
        "amenities": None,
        "environment": None,
        "transit": None,
        "building_stock": None,
    }

    # ── Demographics ──
    cols, rows = _execute(db, PORTRAIT_DEMOGRAPHICS_SQL, [zipcode])
    if rows:
        demo = dict(zip(cols, rows[0]))
        # Get all-zip data for percentiles
        cols_all, rows_all = _execute(db, PORTRAIT_DEMOGRAPHICS_ALL_SQL)
        all_incomes = [dict(zip(cols_all, r))["median_household_income"] for r in rows_all
                       if dict(zip(cols_all, r))["median_household_income"]]
        all_rents = [dict(zip(cols_all, r))["median_gross_rent"] for r in rows_all
                     if dict(zip(cols_all, r))["median_gross_rent"]]

        income_grade = _percentile_grade(
            demo.get("median_household_income"), all_incomes, higher_is_better=True
        )
        rent_grade = _percentile_grade(
            demo.get("median_gross_rent"), all_rents, higher_is_better=False
        )

        result["demographics"] = {
            "population": demo.get("total_population"),
            "median_age": demo.get("median_age"),
            "median_household_income": income_grade,
            "median_gross_rent": rent_grade,
            "homeownership_pct": demo.get("homeownership_pct"),
            "poverty_rate": demo.get("poverty_rate"),
            "vacancy_rate": demo.get("vacancy_rate"),
            "median_home_value": demo.get("median_home_value"),
            "race_ethnicity": {
                "pct_white": demo.get("pct_white"),
                "pct_black": demo.get("pct_black"),
                "pct_asian": demo.get("pct_asian"),
                "pct_hispanic": demo.get("pct_hispanic"),
            },
        }

    # ── Housing Quality ──
    cols, rows = _execute(db, PORTRAIT_HOUSING_QUALITY_SQL, [zipcode, zipcode, zipcode])
    if rows:
        hq = dict(zip(cols, rows[0]))
        cols_all, rows_all = _execute(db, PORTRAIT_HOUSING_ALL_SQL)
        all_violation_rates = [dict(zip(cols_all, r))["open_violations_per_1k_units"]
                               for r in rows_all
                               if dict(zip(cols_all, r))["open_violations_per_1k_units"] is not None]

        housing_grade = _percentile_grade(
            hq.get("open_violations_per_1k_units"), all_violation_rates, higher_is_better=False
        )
        result["grades"]["housing_quality"] = housing_grade
        result["housing"] = {
            "total_units": hq.get("total_units"),
            "total_violations": hq.get("total_violations"),
            "open_violations": hq.get("open_violations"),
            "class_c_hazardous": hq.get("class_c_violations"),
            "complaints_1yr": hq.get("complaints_1yr"),
            "open_violations_per_1k_units": housing_grade,
            "complaints_per_1k_units": hq.get("complaints_per_1k_units"),
        }

    # ── Safety ──
    cols, rows = _execute(db, PORTRAIT_SAFETY_SQL, [zipcode, zipcode])
    if rows:
        safety = dict(zip(cols, rows[0]))
        cols_all, rows_all = _execute(db, PORTRAIT_SAFETY_ALL_SQL)
        all_felonies = [dict(zip(cols_all, r))["felonies"] for r in rows_all
                        if dict(zip(cols_all, r))["felonies"] is not None]

        safety_grade = _percentile_grade(
            safety.get("felonies"), all_felonies, higher_is_better=False
        )
        result["grades"]["safety"] = safety_grade
        result["safety"] = {
            "total_crimes_1yr": safety.get("total_crimes"),
            "felonies": safety_grade,
            "misdemeanors": safety.get("misdemeanors"),
            "top_crimes": {
                "assaults": safety.get("assaults"),
                "robberies": safety.get("robberies"),
                "burglaries": safety.get("burglaries"),
                "larcenies": safety.get("larcenies"),
            },
        }

    # ── Collisions ──
    cols, rows = _execute(db, PORTRAIT_COLLISIONS_SQL, [zipcode])
    if rows:
        coll = dict(zip(cols, rows[0]))
        if result.get("safety"):
            result["safety"]["collisions_1yr"] = coll.get("total_collisions")
            result["safety"]["pedestrians_injured_1yr"] = coll.get("pedestrians_injured")

    # ── Building Stock ──
    cols, rows = _execute(db, PORTRAIT_BUILDINGS_SQL, [zipcode])
    if rows:
        b = dict(zip(cols, rows[0]))
        total = int(b.get("total_buildings") or 0)
        pre_war = int(b.get("pre_war") or 0)
        result["building_stock"] = {
            "total_buildings": total,
            "avg_year_built": int(b.get("avg_year_built") or 0) if b.get("avg_year_built") else None,
            "avg_floors": round(float(b.get("avg_floors") or 0), 1),
            "median_assessed_value": b.get("median_assessed"),
            "landmarks": int(b.get("landmarks") or 0),
            "in_historic_district": int(b.get("in_hist_district") or 0),
            "composition": {
                "pre_war_pct": round(100 * pre_war / total) if total else None,
                "post_war_pct": round(100 * int(b.get("post_war") or 0) / total) if total else None,
                "new_construction_pct": round(100 * int(b.get("new_construction") or 0) / total) if total else None,
            },
        }

    # ── Sales ──
    cols, rows = _execute(db, PORTRAIT_SALES_SQL, [zipcode])
    if rows:
        s = dict(zip(cols, rows[0]))
        if result.get("housing") is None:
            result["housing"] = {}
        result["housing"]["median_sale_price_2yr"] = s.get("median_sale_price")
        result["housing"]["avg_price_per_sqft"] = (
            round(s["avg_price_per_sqft"]) if s.get("avg_price_per_sqft") else None
        )

    # ── Cuisine & Restaurants ──
    cols, rows = _execute(db, PORTRAIT_CUISINE_SQL, [zipcode])
    cols_city, rows_city = _execute(db, PORTRAIT_CUISINE_CITY_SQL)
    city_avg = {dict(zip(cols_city, r))["cuisine_description"]: dict(zip(cols_city, r))["avg_per_zip"]
                for r in rows_city} if rows_city else {}

    if rows:
        cuisines = []
        total_local = sum(int(dict(zip(cols, r))["restaurants"]) for r in rows)
        signature = None
        sig_ratio = 0
        for row in rows[:8]:
            r = dict(zip(cols, row))
            name = r["cuisine_description"]
            count = int(r["restaurants"])
            avg = city_avg.get(name, 0)
            ratio = round(count / avg, 1) if avg and avg > 1 else None
            cuisines.append({"cuisine": name, "count": count, "city_avg_ratio": ratio})
            if ratio and ratio > sig_ratio:
                sig_ratio = ratio
                signature = (name, ratio)
        result["amenities"] = {
            "total_restaurants": total_local,
            "cuisine_fingerprint": cuisines[:6],
            "signature_cuisine": {"cuisine": signature[0], "ratio": signature[1]} if signature else None,
        }
        if signature and sig_ratio > 2:
            result["distinctive_facts"].append(
                f"{signature[0]} restaurants at {sig_ratio}x the city average"
            )

    # ── Restaurant Grades ──
    cols, rows = _execute(db, PORTRAIT_RESTAURANT_GRADES_SQL, [zipcode])
    if rows and result.get("amenities"):
        grades = {dict(zip(cols, r))["grade"]: int(dict(zip(cols, r))["cnt"]) for r in rows}
        total_graded = sum(grades.values())
        result["amenities"]["restaurant_grades"] = {
            "total_graded": total_graded,
            "pct_grade_a": round(100 * grades.get("A", 0) / total_graded) if total_graded else None,
        }

    # ── 311 Quality of Life ──
    cols, rows = _execute(db, PORTRAIT_311_SQL, [zipcode])
    cols_n, rows_n = _execute(db, PORTRAIT_311_NOISE_SQL, [zipcode])
    cols_all_n, rows_all_n = _execute(db, PORTRAIT_311_ALL_NOISE_SQL)

    if rows_n:
        n = dict(zip(cols_n, rows_n[0]))
        all_noise = [dict(zip(cols_all_n, r))["noise_complaints"] for r in rows_all_n
                     if dict(zip(cols_all_n, r))["noise_complaints"] is not None] if rows_all_n else []

        noise_grade = _percentile_grade(
            n.get("noise_complaints"), all_noise, higher_is_better=False
        )
        result["grades"]["quality_of_life"] = noise_grade
        result["quality_of_life"] = {
            "noise_complaints_1yr": noise_grade,
            "heat_complaints_1yr": n.get("heat_complaints"),
            "sanitation_complaints_1yr": n.get("sanitation_complaints"),
            "rodent_complaints_1yr": n.get("rodent_complaints"),
            "total_311_1yr": n.get("total_311"),
            "top_complaint_types": [
                {"type": dict(zip(cols, r))["complaint_type"],
                 "count": int(dict(zip(cols, r))["cnt"])}
                for r in (rows[:5] if rows else [])
            ],
        }
        if noise_grade.get("percentile") and noise_grade["percentile"] < 25:
            result["distinctive_facts"].append(
                f"Noise at {noise_grade['percentile']}th percentile — one of the loudest ZIPs"
            )

    # ── Business Mix ──
    cols, rows = _execute(db, PORTRAIT_BIZ_SQL, [zipcode])
    if rows:
        if result.get("amenities") is None:
            result["amenities"] = {}
        result["amenities"]["business_mix"] = [
            {"industry": dict(zip(cols, r))["industry"],
             "active_licenses": int(dict(zip(cols, r))["cnt"])}
            for r in rows[:6]
        ]

    # ── Environment ──
    cols_t, rows_t = _execute(db, PORTRAIT_TREES_SQL, [zipcode])
    cols_l, rows_l = _execute(db, PORTRAIT_LEAD_SQL, [zipcode])
    cols_all_t, rows_all_t = _execute(db, PORTRAIT_TREES_ALL_SQL)

    env = {}
    if rows_t:
        t = dict(zip(cols_t, rows_t[0]))
        all_trees = [dict(zip(cols_all_t, r))["total_trees"] for r in rows_all_t
                     if dict(zip(cols_all_t, r))["total_trees"] is not None] if rows_all_t else []
        tree_grade = _percentile_grade(t.get("total_trees"), all_trees, higher_is_better=True)
        env["street_trees"] = {
            "total": tree_grade,
            "pct_healthy": (round(100 * int(t.get("healthy_trees") or 0) / int(t["total_trees"]))
                            if t.get("total_trees") and int(t["total_trees"]) > 0 else None),
            "most_common_species": t.get("most_common_species"),
        }
    if rows_l:
        l = dict(zip(cols_l, rows_l[0]))
        total_sl = int(l.get("total_service_lines") or 0)
        lead = int(l.get("lead_lines") or 0)
        env["lead_service_lines"] = {
            "total": total_sl,
            "lead_confirmed": lead,
            "pct_lead": round(100 * lead / total_sl, 1) if total_sl > 0 else 0,
        }
    if env:
        result["environment"] = env

    # ── Transit ──
    cols, rows = _execute(db, PORTRAIT_TRANSIT_SQL, [zipcode, zipcode, zipcode])
    if rows:
        result["transit"] = {
            "subway_stations": int(dict(zip(cols, rows[0])).get("subway_stations") or 0),
        }

    # ── Parks ──
    cols, rows = _execute(db, PORTRAIT_PARKS_SQL, [zipcode])
    if rows:
        p = dict(zip(cols, rows[0]))
        if result.get("amenities") is None:
            result["amenities"] = {}
        result["amenities"]["parks"] = {
            "count": int(p.get("total_parks") or 0),
            "total_acres": p.get("total_acres"),
        }

    # ── Compute headline ──
    parts = []
    if result.get("demographics"):
        pop = result["demographics"].get("population")
        if pop:
            if pop > 60000:
                parts.append("densely populated")
            elif pop < 20000:
                parts.append("quiet, low-density")
    if result.get("building_stock"):
        comp = result["building_stock"].get("composition", {})
        if comp.get("pre_war_pct") and comp["pre_war_pct"] > 60:
            parts.append("pre-war")
        elif comp.get("new_construction_pct") and comp["new_construction_pct"] > 30:
            parts.append("new-construction-heavy")
    if result.get("quality_of_life"):
        noise = result["quality_of_life"].get("noise_complaints_1yr", {})
        if isinstance(noise, dict) and noise.get("percentile") and noise["percentile"] < 20:
            parts.append("noisy")
        elif isinstance(noise, dict) and noise.get("percentile") and noise["percentile"] > 80:
            parts.append("quiet")
    if result.get("amenities") and result["amenities"].get("signature_cuisine"):
        sig = result["amenities"]["signature_cuisine"]
        parts.append(f"{sig['cuisine'].lower()}-heavy")

    result["headline"] = (
        f"A {', '.join(parts)} {boro_name} neighborhood" if parts
        else f"A {boro_name} neighborhood"
    )

    # ── Distinctive facts from grades ──
    for category, grade_data in result["grades"].items():
        if grade_data.get("grade") in ("A+", "A"):
            result["distinctive_facts"].append(
                f"Top-tier {category.replace('_', ' ')} — {grade_data['grade']} "
                f"({grade_data['percentile']}th percentile citywide)"
            )
        elif grade_data.get("grade") in ("D", "D-", "F"):
            result["distinctive_facts"].append(
                f"Below average {category.replace('_', ' ')} — {grade_data['grade']} "
                f"({grade_data['percentile']}th percentile citywide)"
            )

    # ── Build text summary for content field ──
    elapsed = round((time.time() - t0) * 1000)
    lines = [f"NEIGHBORHOOD PORTRAIT: {zipcode}, {boro_name}"]
    lines.append(result["headline"])
    lines.append("=" * 60)

    if result["grades"]:
        lines.append("\nGRADES")
        for cat, g in result["grades"].items():
            if g.get("grade"):
                lines.append(f"  {cat.replace('_', ' ').title()}: {g['grade']} ({g['percentile']}th percentile)")

    if result["distinctive_facts"]:
        lines.append("\nDISTINCTIVE")
        for fact in result["distinctive_facts"]:
            lines.append(f"  * {fact}")

    if result.get("demographics"):
        d = result["demographics"]
        lines.append("\nDEMOGRAPHICS")
        if d.get("population"):
            lines.append(f"  Population: {int(d['population']):,}")
        if d.get("median_household_income", {}).get("value"):
            lines.append(f"  Median income: ${d['median_household_income']['value']:,.0f} ({d['median_household_income']['grade']})")
        if d.get("median_gross_rent", {}).get("value"):
            lines.append(f"  Median rent: ${d['median_gross_rent']['value']:,.0f} ({d['median_gross_rent']['grade']})")
        if d.get("poverty_rate"):
            lines.append(f"  Poverty rate: {d['poverty_rate']}%")

    if result.get("safety"):
        s = result["safety"]
        lines.append("\nSAFETY")
        if s.get("felonies", {}).get("value"):
            lines.append(f"  Felonies (1yr): {int(s['felonies']['value']):,} ({s['felonies']['grade']})")
        if s.get("total_crimes_1yr"):
            lines.append(f"  Total crimes: {int(s['total_crimes_1yr']):,}")

    if result.get("housing"):
        h = result["housing"]
        lines.append("\nHOUSING")
        if h.get("open_violations_per_1k_units", {}).get("value"):
            lines.append(f"  Open violations/1k units: {h['open_violations_per_1k_units']['value']} ({h['open_violations_per_1k_units']['grade']})")
        if h.get("median_sale_price_2yr"):
            lines.append(f"  Median sale price (2yr): ${int(h['median_sale_price_2yr']):,}")

    if result.get("quality_of_life"):
        q = result["quality_of_life"]
        lines.append("\n311 QUALITY OF LIFE")
        if q.get("noise_complaints_1yr", {}).get("value"):
            lines.append(f"  Noise complaints: {int(q['noise_complaints_1yr']['value']):,} ({q['noise_complaints_1yr']['grade']})")
        if q.get("top_complaint_types"):
            for tc in q["top_complaint_types"][:3]:
                lines.append(f"  {tc['type']}: {tc['count']:,}")

    if result.get("amenities"):
        a = result["amenities"]
        lines.append("\nAMENITIES")
        if a.get("total_restaurants"):
            lines.append(f"  Restaurants: {a['total_restaurants']}")
        if a.get("signature_cuisine"):
            lines.append(f"  Signature: {a['signature_cuisine']['cuisine']} ({a['signature_cuisine']['ratio']}x city avg)")
        if a.get("parks"):
            lines.append(f"  Parks: {a['parks']['count']} ({a['parks']['total_acres']} acres)")

    if result.get("environment"):
        e = result["environment"]
        lines.append("\nENVIRONMENT")
        if e.get("street_trees", {}).get("total", {}).get("value"):
            lines.append(f"  Street trees: {int(e['street_trees']['total']['value']):,} ({e['street_trees']['total']['grade']})")
        if e.get("lead_service_lines", {}).get("pct_lead"):
            lines.append(f"  Lead service lines: {e['lead_service_lines']['pct_lead']}%")

    if result.get("transit"):
        lines.append(f"\nTRANSIT")
        lines.append(f"  Subway stations: {result['transit']['subway_stations']}")

    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content=result,
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )
```

- [ ] **Step 2: Deploy and test**

```bash
ssh hetzner "cd /opt/common-ground && docker restart common-ground-duckdb-server-1"
# Wait for startup (~60s for graph building)
sleep 60
# Test via curl
curl -s -X POST https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":"neighborhood_portrait","arguments":{"zipcode":"10003"}}}'
```

Expected: Full JSON response with grades, demographics, safety, etc.

- [ ] **Step 3: Test edge cases**

Test with different borough ZIPs:
- Manhattan: 10003 (East Village)
- Brooklyn: 11201 (Downtown Brooklyn)
- Bronx: 10451 (Mott Haven)
- Queens: 11101 (Long Island City)
- Staten Island: 10301 (St. George)
- Non-NYC: 99999 (should return portrait with empty sections — valid format but no NYC data)

- [ ] **Step 4: Commit**

```bash
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'feat: rewrite neighborhood_portrait with 10 dimensions, grades, and structured response'"
```

---

## Task 6: Performance Optimization — Cache Citywide Percentiles

**Why:** The portrait currently runs ~15 "all zips" queries for percentile computation. These change infrequently (daily at most). Pre-compute them at startup.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (lifespan + portrait function)

- [ ] **Step 1: Add percentile cache to startup**

In the lifespan function, after crosswalk materialization:

```python
# Pre-compute citywide percentile distributions
# Each key maps to a sorted list of floats — one value per NYC ZIP.
# The portrait function passes these lists to _percentile_grade() instead
# of running expensive *_ALL_SQL queries per call.
citywide_cache = {}
try:
    # Income percentiles (from ACS ZCTA materialized view)
    rows = db.execute(
        "SELECT median_household_income FROM zip_population "
        "WHERE median_household_income IS NOT NULL"
    ).fetchall()
    citywide_cache["income"] = sorted(r[0] for r in rows)

    # Rent percentiles
    rows = db.execute(
        "SELECT median_gross_rent FROM zip_population "
        "WHERE median_gross_rent IS NOT NULL"
    ).fetchall()
    citywide_cache["rent"] = sorted(r[0] for r in rows)

    # Violation rate percentiles (open HPD violations per 1k residential units)
    rows = db.execute(PORTRAIT_HOUSING_ALL_SQL).fetchall()
    citywide_cache["violations"] = sorted(
        r[1] for r in rows if r[1] is not None  # col 1 = open_violations_per_1k_units
    )

    # Safety percentiles (felonies per precinct, mapped to ZIP)
    rows = db.execute(PORTRAIT_SAFETY_ALL_SQL).fetchall()
    citywide_cache["felonies"] = sorted(
        r[1] for r in rows if r[1] is not None  # col 1 = felonies
    )

    # Noise percentiles (311 noise complaints per ZIP, last 365 days)
    rows = db.execute(PORTRAIT_311_ALL_NOISE_SQL).fetchall()
    citywide_cache["noise"] = sorted(
        r[1] for r in rows if r[1] is not None  # col 1 = noise_complaints
    )

    # Tree percentiles (alive street trees per ZIP)
    rows = db.execute(PORTRAIT_TREES_ALL_SQL).fetchall()
    citywide_cache["trees"] = sorted(
        r[1] for r in rows if r[1] is not None  # col 1 = total_trees
    )

    log.info(f"Citywide percentile cache loaded: {len(citywide_cache)} dimensions, "
             f"~{sum(len(v) for v in citywide_cache.values())} total values")
except Exception as e:
    log.warning(f"Percentile cache failed: {e}")

ctx["citywide_cache"] = citywide_cache
```

- [ ] **Step 2: Update portrait function to use cache**

Replace the 6 `_execute(db, *_ALL_SQL)` calls with cache lookups:

```python
# Instead of:
cols_all, rows_all = _execute(db, PORTRAIT_DEMOGRAPHICS_ALL_SQL)
all_incomes = [...]

# Use:
cache = ctx.lifespan_context.get("citywide_cache", {})
all_incomes = cache.get("income", [])
```

- [ ] **Step 3: Benchmark before/after**

```bash
# Time 3 consecutive calls
for i in 1 2 3; do
  time curl -s -X POST https://mcp.common-ground.nyc/mcp \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d '{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":"neighborhood_portrait","arguments":{"zipcode":"10003"}}}' > /dev/null
done
```
Target: < 2 seconds per call (down from ~5-8 without cache).

- [ ] **Step 4: Commit**

```bash
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'perf: cache citywide percentiles at startup for neighborhood portrait'"
```

---

## Task 7: Update Tool Description and System Prompt

**Why:** The MCP system prompt and tool description need to reflect the new capabilities.

**Files:**
- Modify: `/opt/common-ground/duckdb-server/mcp_server.py` (tool docstring + server instructions)

- [ ] **Step 1: Update the tool docstring**

Already done in Task 5 — verify the docstring accurately describes all 10 dimensions.

- [ ] **Step 2: Update the MCP server instructions**

Find the server instructions/description string (usually in the FastMCP constructor or a variable) and update the routing table:

```
NEIGHBORHOOD by ZIP → neighborhood_portrait (10 dimensions: demographics, housing, safety, quality of life, amenities, environment, transit, building stock + letter grades)
```

- [ ] **Step 3: Update the system prompt numbers**

Change "294 tables, 12 schemas, 60M+ rows" to the actual counts.

- [ ] **Step 4: Commit**

```bash
ssh hetzner "cd /opt/common-ground && git add duckdb-server/mcp_server.py && git commit -m 'docs: update neighborhood_portrait description and routing instructions'"
```

---

## Task 8: Integration Test — Full End-to-End

**Why:** Verify the complete portrait works from Claude Code through the MCP tunnel.

- [ ] **Step 1: Restart Claude Code to reconnect MCP**

Exit and restart Claude Code so it picks up the updated tool schema.

- [ ] **Step 2: Test via MCP tool call**

Call `neighborhood_portrait("10003")` and verify:
- [ ] `structured_content` has all 10 sections populated
- [ ] `grades` has letter grades for safety, housing_quality, quality_of_life
- [ ] `distinctive_facts` has 2+ facts
- [ ] `headline` is a natural sentence
- [ ] Response time < 3 seconds
- [ ] `content` (text) is readable and complete

- [ ] **Step 3: Test a Bronx ZIP for contrast**

Call `neighborhood_portrait("10451")` — Mott Haven should show different grades, higher violation rates, different cuisine signature.

- [ ] **Step 4: Test with neighborhood_compare**

Call `neighborhood_compare(["10003", "10451"])` to verify the old tool still works alongside the new portrait.

- [ ] **Step 5: Final commit**

```bash
ssh hetzner "cd /opt/common-ground && git add -A && git commit -m 'test: verify neighborhood_portrait integration'"
```

---

## Execution Order & Dependencies

```
Task 1 (ACS ingest) ──→ Task 3 (crosswalks, needs ACS) ──→ Task 5 (main function)
                                                              ↑
Task 2 (grade engine) ─────────────────────────────────────────┘
Task 4 (SQL constants) ────────────────────────────────────────┘

Task 5 ──→ Task 6 (perf cache) ──→ Task 7 (docs) ──→ Task 8 (integration test)
```

Tasks 1, 2, and 4 can be **written** in parallel, but Task 4's SQL **verification** depends on Task 3 (crosswalk tables must exist). Tasks 5-8 are sequential.

---

## Rollback Strategy

Before editing `mcp_server.py`, create a backup branch on the server:

```bash
ssh hetzner "cd /opt/common-ground && git checkout -b portrait-backup && git checkout main"
```

If the new portrait breaks ALL MCP tools (since everything is in one file):

```bash
# Immediate rollback
ssh hetzner "cd /opt/common-ground && git checkout portrait-backup -- duckdb-server/mcp_server.py && docker restart common-ground-duckdb-server-1"
```

If only the portrait is broken but other tools work, the old portrait can be restored from git:

```bash
ssh hetzner "cd /opt/common-ground && git diff portrait-backup -- duckdb-server/mcp_server.py | head -100"
# Then selectively revert portrait function only
```

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| ACS Census API rate limits | No API key needed for state-level ZCTA queries; only ~200 NYC ZCTAs |
| 9,680-line file is fragile | Backup branch before editing; rollback strategy documented above |
| Percentile cache stale after new data ingest | Cache rebuilds on container restart; Dagster ingests trigger restart |
| Precinct crosswalk inaccurate for some ZIPs | Some ZIPs span multiple precincts; using the dominant one (highest 311 count) is ~90% accurate |
| Portrait too slow (21 queries before cache) | Task 6 reduces to 15 queries via cache; target < 2s |
| DuckDB container crashes on heavy queries | Safety query uses UNION (dedup) with date filter on both tables |
| Bad edit takes down all MCP tools | Rollback via `git checkout portrait-backup -- duckdb-server/mcp_server.py` |
