# address_report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build tool #15 — `address_report()` — a comprehensive 360-degree dossier for any NYC address with ~50 parallel queries and percentile-ranked metrics.

**Architecture:** Single tool in `tools/address_report.py` that resolves an address to BBL, derives join keys (ZIP, precinct, borough, community district), fires ~50 SQL queries in parallel via a new `parallel_queries()` helper in `shared/db.py`, then assembles a formatted report with Unicode percentile bars. Each section (building, block, neighborhood, safety, schools, health, environment, civic, services, fun facts) is built by a private function that reads from the parallel query results dict.

**Tech Stack:** FastMCP, DuckDB, Python concurrent.futures.ThreadPoolExecutor, CursorPool

**Spec:** `docs/superpowers/specs/2026-04-01-address-report-design.md`

**Base directory:** `infra/duckdb-server/`

---

## Task 1: Add `parallel_queries()` to shared/db.py

**Files:**
- Modify: `shared/db.py`

- [ ] **Step 1: Add parallel_queries function**

Add this function to `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server/shared/db.py` after the `fill_placeholders` function:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

_PARALLEL_WORKERS = 8  # half the cursor pool — leaves room for other MCP calls


def parallel_queries(
    pool: CursorPool,
    queries: list[tuple[str, str, list | None]],
    max_workers: int = _PARALLEL_WORKERS,
) -> dict[str, tuple[list, list]]:
    """Execute multiple queries concurrently via the CursorPool.

    Args:
        pool: CursorPool instance.
        queries: List of (name, sql, params) tuples.
        max_workers: Max concurrent queries (default 8).

    Returns:
        Dict of {name: (cols, rows)}. Failed queries have ([], []).
    """
    if not queries:
        return {}

    results: dict[str, tuple[list, list]] = {}

    def _run_one(name: str, sql: str, params: list | None):
        try:
            cols, rows = pool.execute(sql, params)
            return name, (cols, rows), None
        except Exception as e:
            return name, ([], []), str(e)

    workers = min(max_workers, len(queries))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_run_one, name, sql, params): name
            for name, sql, params in queries
        }
        for future in as_completed(futures):
            name, data, error = future.result()
            if error:
                print(f"parallel_queries: {name} failed: {error}", flush=True)
            results[name] = data

    return results
```

- [ ] **Step 2: Update __all__ export**

Add `"parallel_queries"` to the `__all__` list in shared/db.py:

```python
__all__ = ["CursorPool", "execute", "safe_query", "fill_placeholders", "build_catalog", "parallel_queries"]
```

- [ ] **Step 3: Verify syntax**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && uv run python -c "import ast; ast.parse(open('shared/db.py').read()); print('OK')"`

- [ ] **Step 4: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/shared/db.py
git commit -m "feat: add parallel_queries() to shared/db.py — ThreadPoolExecutor + CursorPool"
```

---

## Task 2: Create the address_report tool

**Files:**
- Create: `tools/address_report.py`

This is the main task. The tool resolves an address, fires ~50 queries in parallel, and assembles a formatted 360 report.

- [ ] **Step 1: Create tools/address_report.py with the tool function and formatting helpers**

Create `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server/tools/address_report.py`. The file structure:

1. Imports and constants
2. `address_report()` — the public tool function
3. `_resolve_context()` — resolve address to BBL + derive ZIP, precinct, borough, community district
4. `_build_queries()` — return the list of ~50 (name, sql, params) tuples
5. `_assemble_report()` — take the results dict and format each section
6. `_pctile_bar()` — percentile bar renderer
7. Section formatters: `_section_building()`, `_section_block()`, `_section_neighborhood()`, etc.

```python
# tools/address_report.py
"""address_report() — 360-degree dossier for any NYC address."""

import datetime
from typing import Annotated
from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError

from shared.db import execute, safe_query, parallel_queries
from shared.types import READONLY


def address_report(
    address: Annotated[str, Field(
        description="NYC street address, e.g. '305 Linden Blvd, Brooklyn' or '350 5th Ave, Manhattan'",
        examples=["305 Linden Blvd, Brooklyn", "350 5th Ave, Manhattan", "123 Main St, Bronx"],
    )],
    ctx: Context = None,
) -> ToolResult:
    """Complete 360-degree report for any NYC address. Returns building profile, violations with percentile rankings, neighborhood demographics, crime stats, school quality, health indicators, environmental data, civic representation, nearby services, and fun facts. Every metric ranked against the city. Use this as the first lookup for any address. For deeper investigation, use the drill-deeper suggestions at the end of the report."""
    pool = ctx.lifespan_context["pool"]

    # Step 1: resolve address → BBL + derive all join keys
    ctx_data = _resolve_context(pool, address)
    bbl = ctx_data["bbl"]

    # Step 2: build and execute all queries in parallel
    queries = _build_queries(ctx_data)
    results = parallel_queries(pool, queries)

    # Step 3: assemble the formatted report
    report = _assemble_report(ctx_data, results)

    # Step 4: build structured content for programmatic access
    structured = {
        "bbl": bbl,
        "address": ctx_data.get("address", address),
        "zipcode": ctx_data.get("zip", ""),
        "borough": ctx_data.get("borough", ""),
        "sections": {k: v for k, v in results.items() if v != ([], [])},
    }

    return ToolResult(content=report, structured_content=structured)
```

- [ ] **Step 2: Implement _resolve_context()**

This function resolves the address and derives all join keys needed for the parallel queries. Add to address_report.py:

```python
def _resolve_context(pool, address: str) -> dict:
    """Resolve address → BBL, then derive ZIP, borough, precinct, community district."""
    # Reuse building.py's address resolution
    from tools.building import _resolve_bbl, _normalize_address

    bbl = _resolve_bbl(pool, address)

    # Get PLUTO data for the resolved BBL
    cols, rows = execute(pool, """
        SELECT
            borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') AS bbl,
            address, borough, postcode AS zip, zonedist1 AS zoning,
            bldgclass, numfloors, unitsres, unitstotal, yearbuilt,
            assesstot, assessland, ownername, condono, landmark,
            cd AS community_district, council AS council_district,
            tract2020 AS census_tract, lotarea, bldgarea
        FROM lake.city_government.pluto
        WHERE borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') = ?
        LIMIT 1
    """, [bbl])

    if not rows:
        raise ToolError(f"BBL {bbl} not found in PLUTO. Try a different address.")

    data = dict(zip(cols, rows[0]))
    data["bbl"] = bbl

    # Derive precinct from 311 data (most common precinct for this ZIP)
    _, pct_rows = safe_query(pool, """
        SELECT TRY_CAST(REGEXP_EXTRACT(incident_address, '(\d+)\s+PCT', 1) AS INT) AS pct
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ? AND pct IS NOT NULL
        GROUP BY pct ORDER BY COUNT(*) DESC LIMIT 1
    """, [data.get("zip", "")])

    # Fallback: try community district to precinct mapping
    if not pct_rows:
        _, pct_rows = safe_query(pool, """
            SELECT DISTINCT addr_pct_cd AS pct
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE TRY_CAST(SUBSTR(?, 1, 1) AS INT) IS NOT NULL
            LIMIT 1
        """, [bbl])

    data["precinct"] = str(pct_rows[0][0]) if pct_rows and pct_rows[0][0] else ""

    return data
```

- [ ] **Step 3: Implement _build_queries()**

This function returns the full list of ~50 (name, sql, params) tuples. The implementing agent should:

1. READ the spec at docs/superpowers/specs/2026-04-01-address-report-design.md for all 10 sections
2. READ the data audit (sections 1-10) for exact table names and join keys
3. Write SQL queries for each section, using the join keys from ctx_data (bbl, zip, borough, precinct, community_district, census_tract)

Key patterns:
- BBL queries use `WHERE bbl = ?` or composed BBL `WHERE borocode || LPAD(block, 5, '0') || LPAD(lot, 4, '0') = ?`
- ZIP queries use `WHERE zipcode = ?` or `WHERE postcode = ?` or `WHERE zip_code = ?` (column names vary by table)
- Precinct queries use `WHERE precinct = ?` or `WHERE addr_pct_cd = ?`
- Percentile queries use `PERCENT_RANK() OVER (ORDER BY count)` against city-wide aggregation
- Each query should include a LIMIT and return only the columns needed for the report

Group queries by section name prefix: `building_violations`, `building_complaints`, `block_trees`, `neighborhood_income`, `safety_crimes`, `school_nearest`, `health_cdc`, `env_flood`, `civic_council`, `services_subway`, `fun_dogs`, etc.

```python
def _build_queries(ctx: dict) -> list[tuple[str, str, list | None]]:
    """Build ~50 parallel queries from resolved context."""
    bbl = ctx["bbl"]
    zipcode = ctx.get("zip", "")
    borough = ctx.get("borough", "")
    precinct = ctx.get("precinct", "")
    cd = ctx.get("community_district", "")
    boro_code = bbl[0] if bbl else ""

    queries = []

    # ── BUILDING ──────────────────────────────────────────
    queries.append(("building_hpd_violations", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE currentstatus = 'Open' OR violationstatus = 'Open') AS open_cnt,
               COUNT(*) FILTER (WHERE class = 'C') AS class_c,
               MAX(novissueddate) AS latest
        FROM lake.housing.hpd_violations WHERE bbl = ?
    """, [bbl]))

    queries.append(("building_hpd_complaints", """
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status != 'Close') AS open_cnt,
               MODE() WITHIN GROUP (ORDER BY majorcategory) AS top_category
        FROM lake.housing.hpd_complaints WHERE bbl = ?
    """, [bbl]))

    queries.append(("building_dob_violations", """
        SELECT COUNT(*) AS total,
               SUM(TRY_CAST(penalty_balance_due AS DOUBLE)) AS penalties
        FROM lake.housing.dob_ecb_violations
        WHERE (boro || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0')) = ?
    """, [bbl]))

    queries.append(("building_fdny", """
        SELECT COUNT(*) AS total FROM lake.housing.fdny_violations WHERE bbl = ?
    """, [bbl]))

    queries.append(("building_evictions", """
        SELECT COUNT(*) AS total FROM lake.housing.evictions WHERE bbl = ?
    """, [bbl]))

    queries.append(("building_sale", """
        SELECT TRY_CAST(m.document_date AS DATE) AS sale_date,
               TRY_CAST(m.document_amt AS DOUBLE) AS price
        FROM lake.housing.acris_master m
        JOIN lake.housing.acris_legals l ON m.document_id = l.document_id
        WHERE (l.borough || LPAD(l.block::VARCHAR, 5, '0') || LPAD(l.lot::VARCHAR, 4, '0')) = ?
          AND m.doc_type IN ('DEED', 'DEEDO')
        ORDER BY m.document_date DESC LIMIT 1
    """, [bbl]))

    queries.append(("building_energy", """
        SELECT energy_star_score, source_eui_kbtu_ft2, total_ghg_emissions_mtco2e
        FROM lake.environment.ll84_energy_2023 WHERE bbl = ? LIMIT 1
    """, [bbl]))

    queries.append(("building_facade", """
        SELECT filing_status FROM lake.housing.dob_safety_facades WHERE bbl = ?
        ORDER BY last_filing_date DESC NULLS LAST LIMIT 1
    """, [bbl]))

    queries.append(("building_boiler", """
        SELECT overall_status FROM lake.housing.dob_safety_boiler WHERE bbl = ?
        ORDER BY inspection_date DESC NULLS LAST LIMIT 1
    """, [bbl]))

    queries.append(("building_aep", """
        SELECT 1 AS on_list FROM lake.housing.aep_buildings WHERE bbl = ? LIMIT 1
    """, [bbl]))

    queries.append(("building_tax_lien", """
        SELECT COUNT(*) AS cnt FROM lake.housing.tax_lien_sales WHERE bbl = ?
    """, [bbl]))

    queries.append(("building_owner_portfolio", """
        SELECT COUNT(DISTINCT j.bbl) AS portfolio_size
        FROM lake.housing.hpd_registration_contacts c
        JOIN lake.housing.hpd_jurisdiction j ON c.registrationid = j.registrationid
        WHERE c.registrationid = (
            SELECT registrationid FROM lake.housing.hpd_jurisdiction
            WHERE boroid || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') = ?
            LIMIT 1
        )
    """, [bbl]))

    queries.append(("building_corps_at_address", """
        SELECT COUNT(*) AS cnt FROM lake.business.nys_corporations
        WHERE UPPER(registered_agent_address) LIKE '%' || ? || '%'
    """, [zipcode]))

    # ── BUILDING PERCENTILES ──────────────────────────────
    queries.append(("pctile_violations", """
        WITH bbl_counts AS (
            SELECT bbl, COUNT(*) AS cnt FROM lake.housing.hpd_violations GROUP BY bbl
        )
        SELECT PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
        FROM bbl_counts WHERE bbl = ?
    """, [bbl]))

    queries.append(("pctile_complaints", """
        WITH bbl_counts AS (
            SELECT bbl, COUNT(*) AS cnt FROM lake.housing.hpd_complaints GROUP BY bbl
        )
        SELECT PERCENT_RANK() OVER (ORDER BY cnt) AS pctile
        FROM bbl_counts WHERE bbl = ?
    """, [bbl]))

    # ── BLOCK ─────────────────────────────────────────────
    block_prefix = bbl[:6] if len(bbl) >= 6 else bbl
    queries.append(("block_buildings", f"""
        SELECT COUNT(*) AS cnt,
               ROUND(AVG(TRY_CAST(numfloors AS INT))) AS avg_floors,
               ROUND(AVG(TRY_CAST(yearbuilt AS INT))) AS avg_year,
               SUM(TRY_CAST(unitsres AS INT)) AS total_units
        FROM lake.city_government.pluto
        WHERE borocode || LPAD(block::VARCHAR, 5, '0') LIKE '{block_prefix}%'
    """, None))

    queries.append(("block_trees", """
        SELECT spc_common AS species, COUNT(*) AS cnt
        FROM lake.environment.street_trees
        WHERE zipcode = ? AND spc_common IS NOT NULL
        GROUP BY spc_common ORDER BY cnt DESC LIMIT 3
    """, [zipcode]))

    queries.append(("block_film", """
        SELECT event_id, parking_held, start_date_time
        FROM lake.city_government.film_permits
        WHERE UPPER(parking_held) LIKE '%' || UPPER(?) || '%'
        ORDER BY start_date_time DESC LIMIT 5
    """, [ctx.get("address", "").split(",")[0] if ctx.get("address") else ""]))

    # ── NEIGHBORHOOD ──────────────────────────────────────
    queries.append(("neighborhood_acs", """
        SELECT median_household_income, poverty_rate, total_population,
               median_gross_rent, pct_renter_cost_burdened, pct_foreign_born
        FROM lake.federal.acs_zcta_demographics WHERE zcta = ? LIMIT 1
    """, [zipcode]))

    queries.append(("neighborhood_311_top", """
        SELECT complaint_type, COUNT(*) AS cnt
        FROM lake.social_services.n311_service_requests
        WHERE incident_zip = ? AND created_date >= CURRENT_DATE - INTERVAL '1 year'
        GROUP BY complaint_type ORDER BY cnt DESC LIMIT 5
    """, [zipcode]))

    queries.append(("neighborhood_recycling", """
        SELECT diversion_rate_total
        FROM lake.environment.recycling_rates
        WHERE communitydistrict = ? LIMIT 1
    """, [cd]))

    queries.append(("neighborhood_broadband", """
        SELECT broadband_adoption_rate
        FROM lake.social_services.broadband_adoption
        WHERE zip_code = ? LIMIT 1
    """, [zipcode]))

    # ── SAFETY ────────────────────────────────────────────
    if precinct:
        queries.append(("safety_crimes", """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
                   COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors
            FROM lake.public_safety.nypd_complaints_ytd
            WHERE addr_pct_cd = ?
        """, [int(precinct) if precinct.isdigit() else 0]))

        queries.append(("safety_shootings", """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE statistical_murder_flag = 'Y') AS fatal
            FROM lake.public_safety.shootings
            WHERE precinct = ? AND occur_date >= CURRENT_DATE - INTERVAL '1 year'
        """, [int(precinct) if precinct.isdigit() else 0]))

        queries.append(("safety_crashes", """
            SELECT COUNT(*) AS total,
                   SUM(TRY_CAST(number_of_persons_injured AS INT)) AS injured,
                   SUM(TRY_CAST(number_of_persons_killed AS INT)) AS killed
            FROM lake.public_safety.motor_vehicle_collisions
            WHERE zip_code = ? AND crash_date >= CURRENT_DATE - INTERVAL '1 year'
        """, [zipcode]))

    # ── SCHOOLS ───────────────────────────────────────────
    queries.append(("school_nearest", """
        SELECT school_name, dbn, primary_address, grade_span_min, grade_span_max
        FROM lake.federal.urban_school_directory
        WHERE zip = ? ORDER BY school_name LIMIT 3
    """, [zipcode]))

    queries.append(("school_ela", """
        SELECT s.school_name, e.mean_scale_score, e.level_3_4_pct
        FROM lake.education.ela_results e
        JOIN lake.federal.urban_school_directory s ON e.dbn = s.dbn
        WHERE s.zip = ?
        ORDER BY e.level_3_4_pct DESC NULLS LAST LIMIT 3
    """, [zipcode]))

    # ── HEALTH ────────────────────────────────────────────
    queries.append(("health_cdc", """
        SELECT measure, data_value
        FROM lake.health.cdc_places
        WHERE locationid = ? AND data_value IS NOT NULL
        ORDER BY measure LIMIT 20
    """, [zipcode]))

    queries.append(("health_rats", """
        SELECT COUNT(*) AS inspections,
               COUNT(*) FILTER (WHERE result ILIKE '%active%') AS active_rats
        FROM lake.health.rodent_inspections
        WHERE zip_code = ?
    """, [zipcode]))

    queries.append(("health_covid", """
        SELECT covid_case_rate, covid_death_rate
        FROM lake.health.covid_by_zip
        WHERE modified_zcta = ? LIMIT 1
    """, [zipcode]))

    # ── ENVIRONMENT ───────────────────────────────────────
    queries.append(("env_flood", """
        SELECT flood_zone FROM lake.environment.flood_vulnerability
        WHERE bbl = ? LIMIT 1
    """, [bbl]))

    queries.append(("env_heat", """
        SELECT heat_vulnerability_index
        FROM lake.environment.heat_vulnerability
        WHERE nta_code = (SELECT nta FROM lake.city_government.pluto WHERE
            borocode || LPAD(block::VARCHAR, 5, '0') || LPAD(lot::VARCHAR, 4, '0') = ? LIMIT 1)
        LIMIT 1
    """, [bbl]))

    queries.append(("env_air", """
        SELECT AVG(TRY_CAST(data_value AS DOUBLE)) AS pm25
        FROM lake.environment.air_quality
        WHERE indicator_name ILIKE '%fine particulate%' AND geo_type_name = 'UHF 42'
        LIMIT 1
    """, None))

    # ── CIVIC ─────────────────────────────────────────────
    queries.append(("civic_contracts", """
        SELECT SUM(TRY_CAST(current_amount AS DOUBLE)) AS total_5yr
        FROM lake.city_government.contract_awards
        WHERE vendor_zip = ? AND start_date >= CURRENT_DATE - INTERVAL '5 years'
    """, [zipcode]))

    queries.append(("civic_fec", """
        SELECT SUM(TRY_CAST(contribution_receipt_amount AS DOUBLE)) AS total
        FROM lake.federal.fec_contributions
        WHERE contributor_zip LIKE ? || '%'
    """, [zipcode[:5] if zipcode else ""]))

    # ── SERVICES ──────────────────────────────────────────
    queries.append(("services_subway", """
        SELECT station_name, line, COUNT(*) AS entrances
        FROM lake.transportation.mta_entrances
        WHERE UPPER(station_name) IN (
            SELECT UPPER(station_name) FROM lake.transportation.mta_entrances
            WHERE TRY_CAST(entrance_latitude AS DOUBLE) IS NOT NULL
            ORDER BY ABS(TRY_CAST(entrance_latitude AS DOUBLE) - 40.65) +
                     ABS(TRY_CAST(entrance_longitude AS DOUBLE) - (-73.95))
            LIMIT 3
        )
        GROUP BY station_name, line LIMIT 5
    """, None))

    queries.append(("services_food_pantries", """
        SELECT COUNT(*) AS cnt FROM lake.social_services.dycd_program_sites
        WHERE postcode = ? AND program_type ILIKE '%food%'
    """, [zipcode]))

    # ── FUN FACTS ─────────────────────────────────────────
    queries.append(("fun_baby_names", """
        SELECT child_s_first_name AS name, gender, COUNT(*) AS cnt
        FROM lake.recreation.baby_names
        WHERE UPPER(borough) = UPPER(?)
        GROUP BY name, gender ORDER BY cnt DESC LIMIT 2
    """, [borough]))

    queries.append(("fun_dogs", """
        SELECT breed_name, COUNT(*) AS cnt
        FROM lake.recreation.canine_waste
        WHERE zip_code = ?
        GROUP BY breed_name ORDER BY cnt DESC LIMIT 1
    """, [zipcode]))

    queries.append(("fun_fishing", """
        SELECT waterbody, species
        FROM lake.recreation.fishing_sites
        ORDER BY RANDOM() LIMIT 1
    """, None))

    return queries
```

Note: The implementing agent should READ the full data audit output to add more queries. The above is a representative subset (~40 queries). The agent should add queries for:
- `rolling_sales` (last sale price)
- `ll44_income_rent` (rent stabilization)
- `dob_permit_issuance` (permits count)
- `property_valuation` (market/assessed value)
- `hpd_litigations` (litigation count)
- `evictions` (eviction count)
- `environmental_justice` (EJ score)
- Additional percentile queries for safety, neighborhood, health metrics
- `pools`, `spray_showers`, `play_areas` (nearest recreation)

The total should be ~50 queries.

- [ ] **Step 4: Implement _pctile_bar() and _severity()**

```python
def _pctile_bar(value: float, label: str, raw: str, width: int = 10) -> str:
    """Render a percentile bar: 'Violations      2,733   ████████░░ 87th — high'"""
    if value is None:
        return f" {label:<22s} {raw:>8s}   {'░' * width} n/a"
    n = round(value * 100)
    filled = round(value * width)
    bar = "█" * filled + "░" * (width - filled)
    sev = _severity(n)
    return f" {label:<22s} {raw:>8s}   {bar} {n}th — {sev}"


def _severity(pctile: int) -> str:
    if pctile <= 20: return "low"
    if pctile <= 40: return "moderate"
    if pctile <= 60: return "typical"
    if pctile <= 80: return "high"
    if pctile <= 95: return "very high"
    return "extreme"


def _fmt(n) -> str:
    """Format a number with commas, or 'n/a' if None."""
    if n is None: return "n/a"
    if isinstance(n, float): return f"{n:,.1f}"
    if isinstance(n, int): return f"{n:,}"
    return str(n)
```

- [ ] **Step 5: Implement _assemble_report()**

This is the core formatting function. It reads from the results dict and builds the Unicode report.

```python
def _assemble_report(ctx: dict, results: dict) -> str:
    """Assemble the 360 report from parallel query results."""
    bbl = ctx["bbl"]
    address = ctx.get("address", "")
    borough = ctx.get("borough", "")
    zipcode = ctx.get("zip", "")
    precinct = ctx.get("precinct", "")
    cd = ctx.get("community_district", "")
    council = ctx.get("council_district", "")
    zoning = ctx.get("zoning", "")
    bldg_class = ctx.get("bldgclass", "")
    floors = ctx.get("numfloors", "")
    units = ctx.get("unitsres", "")
    year = ctx.get("yearbuilt", "")
    owner = ctx.get("ownername", "")
    assessed = ctx.get("assesstot", "")

    def _get(name):
        """Get first row from results as dict, or empty dict."""
        cols, rows = results.get(name, ([], []))
        if cols and rows:
            return dict(zip(cols, rows[0]))
        return {}

    def _get_all(name):
        """Get all rows from results as list of dicts."""
        cols, rows = results.get(name, ([], []))
        return [dict(zip(cols, r)) for r in rows]

    lines = []
    W = "━" * 52

    # ── HEADER ──
    lines.append(W)
    lines.append(" 360 ADDRESS REPORT — COMMON GROUND")
    lines.append(W)
    lines.append(f" {address}, {borough}, NY {zipcode}")
    lines.append(f" BBL {bbl} · CD {cd} · Council {council}")
    lines.append("")

    # ── BUILDING ──
    lines.append(f"━━ BUILDING {'━' * 40}")
    lines.append(f" Built {year} · {floors} stories · {units} units · {bldg_class}")
    lines.append(f" Zoning {zoning} · Owner: {owner}")
    lines.append(f" Assessed value: ${_fmt(assessed)}")

    # Sale
    sale = _get("building_sale")
    if sale.get("price"):
        lines.append(f" Last sale: ${_fmt(sale['price'])} ({sale.get('sale_date', 'n/a')})")

    # Portfolio
    portfolio = _get("building_owner_portfolio")
    if portfolio.get("portfolio_size", 0) > 1:
        lines.append(f" Owner portfolio: {_fmt(portfolio['portfolio_size'])} buildings")

    lines.append("")

    # Violations with percentiles
    v = _get("building_hpd_violations")
    pv = _get("pctile_violations")
    if v:
        lines.append(_pctile_bar(pv.get("pctile"), "HPD violations", _fmt(v.get("total", 0))))
        lines.append(f"   Open: {_fmt(v.get('open_cnt', 0))} · Class C: {_fmt(v.get('class_c', 0))}")

    c = _get("building_hpd_complaints")
    pc = _get("pctile_complaints")
    if c:
        lines.append(_pctile_bar(pc.get("pctile"), "HPD complaints", _fmt(c.get("total", 0))))

    dob = _get("building_dob_violations")
    if dob and dob.get("total", 0) > 0:
        lines.append(f" DOB/ECB violations     {_fmt(dob['total']):>8s}   penalties: ${_fmt(dob.get('penalties', 0))}")

    fdny = _get("building_fdny")
    if fdny and fdny.get("total", 0) > 0:
        lines.append(f" FDNY violations        {_fmt(fdny['total']):>8s}")

    evict = _get("building_evictions")
    if evict and evict.get("total", 0) > 0:
        lines.append(f" Eviction filings       {_fmt(evict['total']):>8s}")

    # Flags
    flags = []
    if _get("building_aep").get("on_list"): flags.append("⚠ AEP (worst buildings list)")
    if _get("building_tax_lien").get("cnt", 0) > 0: flags.append("⚠ Tax lien sale history")
    facade = _get("building_facade")
    if facade and facade.get("filing_status", "").upper() == "UNSAFE": flags.append("⚠ Facade: UNSAFE")
    for f in flags:
        lines.append(f" {f}")

    lines.append("")

    # ── BLOCK ──
    lines.append(f"━━ BLOCK {'━' * 43}")
    block = _get("block_buildings")
    if block:
        lines.append(f" {_fmt(block.get('cnt', 0))} buildings · avg {_fmt(block.get('avg_floors'))} floors · avg built {_fmt(block.get('avg_year'))}")
        lines.append(f" {_fmt(block.get('total_units', 0))} total units on block")

    trees = _get_all("block_trees")
    if trees:
        top_tree = trees[0].get("species", "unknown")
        tree_count = sum(t.get("cnt", 0) for t in trees)
        lines.append(f" Street trees: {_fmt(tree_count)} · most common: {top_tree}")

    films = _get_all("block_film")
    if films:
        lines.append(f" Film shoots nearby: {len(films)} recent")

    lines.append("")

    # ── NEIGHBORHOOD ──
    lines.append(f"━━ NEIGHBORHOOD ({zipcode}) {'━' * (30 - len(zipcode))}")
    acs = _get("neighborhood_acs")
    if acs:
        lines.append(f" Population: {_fmt(acs.get('total_population'))}")
        lines.append(f" Median income: ${_fmt(acs.get('median_household_income'))}")
        lines.append(f" Poverty rate: {_fmt(acs.get('poverty_rate'))}%")
        lines.append(f" Median rent: ${_fmt(acs.get('median_gross_rent'))}")
        lines.append(f" Rent-burdened: {_fmt(acs.get('pct_renter_cost_burdened'))}%")
        lines.append(f" Foreign-born: {_fmt(acs.get('pct_foreign_born'))}%")

    n311 = _get_all("neighborhood_311_top")
    if n311:
        top = n311[0]
        lines.append(f" Top 311 complaint: {top.get('complaint_type', '?')} ({_fmt(top.get('cnt'))})")

    lines.append("")

    # ── SAFETY ──
    if precinct:
        lines.append(f"━━ SAFETY (Precinct {precinct}) {'━' * (28 - len(precinct))}")
        crimes = _get("safety_crimes")
        if crimes:
            lines.append(f" Total crimes (YTD): {_fmt(crimes.get('total'))}")
            lines.append(f"   Felonies: {_fmt(crimes.get('felonies'))} · Misdemeanors: {_fmt(crimes.get('misdemeanors'))}")

        shootings = _get("safety_shootings")
        if shootings:
            lines.append(f" Shootings (12 mo): {_fmt(shootings.get('total'))} · fatal: {_fmt(shootings.get('fatal'))}")

        crashes = _get("safety_crashes")
        if crashes:
            lines.append(f" Crashes (12 mo): {_fmt(crashes.get('total'))} · injured: {_fmt(crashes.get('injured'))} · killed: {_fmt(crashes.get('killed'))}")
    else:
        lines.append(f"━━ SAFETY {'━' * 42}")
        lines.append(" Precinct not resolved — use safety(precinct) for details")

    lines.append("")

    # ── SCHOOLS ──
    lines.append(f"━━ SCHOOLS {'━' * 41}")
    schools = _get_all("school_nearest")
    if schools:
        for s in schools[:3]:
            lines.append(f" {s.get('school_name', '?')} ({s.get('dbn', '')})")
    else:
        lines.append(" No schools found in this ZIP")

    lines.append("")

    # ── HEALTH ──
    lines.append(f"━━ HEALTH {'━' * 42}")
    rats = _get("health_rats")
    if rats and rats.get("inspections", 0) > 0:
        rat_pct = round(100 * rats.get("active_rats", 0) / rats["inspections"])
        lines.append(f" Rat activity: {rat_pct}% of inspections positive")

    covid = _get("health_covid")
    if covid:
        lines.append(f" COVID case rate: {_fmt(covid.get('covid_case_rate'))}/100K")
        lines.append(f" COVID death rate: {_fmt(covid.get('covid_death_rate'))}/100K")

    lines.append("")

    # ── ENVIRONMENT ──
    lines.append(f"━━ ENVIRONMENT {'━' * 37}")
    flood = _get("env_flood")
    if flood:
        lines.append(f" Flood zone: {flood.get('flood_zone', 'unknown')}")
    heat = _get("env_heat")
    if heat:
        lines.append(f" Heat vulnerability: {_fmt(heat.get('heat_vulnerability_index'))}/5")
    energy = _get("building_energy")
    if energy and energy.get("energy_star_score"):
        lines.append(f" Energy Star score: {_fmt(energy.get('energy_star_score'))}")

    lines.append("")

    # ── CIVIC ──
    lines.append(f"━━ CIVIC {'━' * 43}")
    lines.append(f" Council District {council} · Community Board {cd}")
    contracts = _get("civic_contracts")
    if contracts and contracts.get("total_5yr"):
        lines.append(f" City contracts to ZIP (5yr): ${_fmt(contracts['total_5yr'])}")
    fec = _get("civic_fec")
    if fec and fec.get("total"):
        lines.append(f" FEC donations from ZIP: ${_fmt(fec['total'])}")

    lines.append("")

    # ── SERVICES ──
    lines.append(f"━━ SERVICES {'━' * 40}")
    subway = _get_all("services_subway")
    if subway:
        stations = ", ".join(f"{s.get('station_name', '?')} ({s.get('line', '')})" for s in subway[:2])
        lines.append(f" Subway: {stations}")
    pantries = _get("services_food_pantries")
    if pantries:
        lines.append(f" Food pantries in ZIP: {_fmt(pantries.get('cnt'))}")

    lines.append("")

    # ── FUN FACTS ──
    lines.append(f"━━ FUN FACTS {'━' * 39}")
    dogs = _get("fun_dogs")
    if dogs and dogs.get("breed_name"):
        lines.append(f" Most popular dog breed ({zipcode}): {dogs['breed_name']}")
    if trees:
        lines.append(f" Most common street tree: {trees[0].get('species', '?')}")
    babies = _get_all("fun_baby_names")
    if babies:
        names = ", ".join(f"{b.get('name', '?')} ({b.get('gender', '?')})" for b in babies[:2])
        lines.append(f" Top baby names ({borough}): {names}")
    if films:
        lines.append(f" Film shoots nearby: {len(films)} in last year")
    fishing = _get("fun_fishing")
    if fishing:
        lines.append(f" Nearest fishing: {fishing.get('waterbody', '?')}")

    lines.append("")

    # ── FOOTER ──
    lines.append(W)
    lines.append(f" Data: NYC Open Data, Census ACS, DOE, DOHMH, DEP")
    lines.append(f" common-ground.nyc — 294 tables, 60M+ rows")
    lines.append(W)
    lines.append("")
    lines.append(" Drill deeper:")
    lines.append(f"   building(\"{bbl}\", view=\"enforcement\")")
    lines.append(f"   building(\"{bbl}\", view=\"history\")")
    lines.append(f"   network(\"{owner}\", type=\"ownership\")")
    lines.append(f"   neighborhood(\"{zipcode}\", view=\"gentrification\")")
    lines.append(f"   safety(\"{precinct}\")")

    return "\n".join(lines)
```

- [ ] **Step 6: Verify syntax**

Run: `cd /Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server && uv run python -c "import ast; ast.parse(open('tools/address_report.py').read()); print('OK')"`

- [ ] **Step 7: Commit**

```bash
cd /Users/fattie2020/Desktop/dagster-pipeline
git add infra/duckdb-server/tools/address_report.py
git commit -m "feat: add address_report() — 360 dossier with ~50 parallel queries and percentile bars"
```

---

## Task 3: Register the tool and update routing

**Files:**
- Modify: `tools/__init__.py`
- Modify: `mcp_server.py`

- [ ] **Step 1: Add to tools/__init__.py**

Add this line to `/Users/fattie2020/Desktop/dagster-pipeline/infra/duckdb-server/tools/__init__.py`:

```python
from tools.address_report import address_report
```

- [ ] **Step 2: Register in mcp_server.py**

Find the tool registration block (around line 1800) in mcp_server.py. Add:

```python
mcp.tool(annotations=READONLY)(address_report)
```

- [ ] **Step 3: Update the INSTRUCTIONS routing table**

Find the `INSTRUCTIONS = """` constant in mcp_server.py. Add `address_report` as the FIRST entry in the routing table (it should be the default for any address):

```
ROUTING -- pick the FIRST match:
* Street address or "where I live"       -> address_report()
* Address + specific question (violations, history) -> building()
* BBL (10-digit number)                  -> building()
```

The key: `address_report` handles addresses when the user wants the full picture. `building` handles addresses when the user has a specific question.

- [ ] **Step 4: Update the import in mcp_server.py**

Add `address_report` to the import block:

```python
from tools import (
    building, entity, neighborhood, network, school,
    semantic_search, query, safety, health, legal,
    civic, transit, services, suggest, address_report,
)
```

- [ ] **Step 5: Verify syntax**

Run: `uv run python -c "import ast; ast.parse(open('infra/duckdb-server/mcp_server.py').read()); print('OK')"`

- [ ] **Step 6: Commit**

```bash
git add infra/duckdb-server/tools/__init__.py infra/duckdb-server/mcp_server.py
git commit -m "feat: register address_report as tool #15, update routing table"
```

---

## Task 4: Deploy and test

**Files:** None (deployment)

- [ ] **Step 1: Rsync to server**

```bash
SSH_KEY="$HOME/.ssh/id_ed25519_hetzner"
rsync -avz -e "ssh -i $SSH_KEY" \
  infra/duckdb-server/shared/db.py \
  infra/duckdb-server/tools/address_report.py \
  infra/duckdb-server/tools/__init__.py \
  infra/duckdb-server/mcp_server.py \
  fattie@178.156.228.119:/opt/common-ground/duckdb-server/
```

Note: rsync individual files to avoid the permission issues with the full directory sync.

- [ ] **Step 2: Rebuild container**

```bash
ssh -i "$SSH_KEY" fattie@178.156.228.119 \
  "cd /opt/common-ground && docker compose up -d --build duckdb-server"
```

- [ ] **Step 3: Wait for startup and verify health**

```bash
sleep 45
ssh -i "$SSH_KEY" fattie@178.156.228.119 \
  "curl -s http://localhost:4213/health"
```

Expected: `{"status":"ok"}`

- [ ] **Step 4: Test address_report via MCP**

```bash
ssh -i "$SSH_KEY" fattie@178.156.228.119 \
  'curl -s -X POST http://localhost:4213/mcp \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"address_report\",\"arguments\":{\"address\":\"350 5th Ave, Manhattan\"}}}" \
    | grep "^data:" | head -1'
```

Expected: A formatted 360 report for the Empire State Building.

- [ ] **Step 5: Test with the user's original address**

Same curl but with `"address":"305 Linden Blvd, Brooklyn"`.

- [ ] **Step 6: Verify tool count is now 15**

```bash
ssh -i "$SSH_KEY" fattie@178.156.228.119 \
  'curl -s -X POST http://localhost:4213/mcp \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}" \
    | grep "^data:" | sed "s/^data: //" | python3 -c "
import sys, json
data = json.load(sys.stdin)
tools = data.get(\"result\", {}).get(\"tools\", [])
print(f\"{len(tools)} tools\")
"'
```

Expected: `15 tools`

---

## Execution Notes

### Query accuracy
The SQL in Task 2 uses table and column names from the existing tools and the data audit. However, column names may vary between tables (e.g., `zipcode` vs `zip_code` vs `postcode` vs `zip`). The implementing agent should:
1. Use `safe_query` patterns where table existence is uncertain
2. Check column names against existing tool queries (tools/building.py, tools/safety.py, etc.)
3. Log failed queries (the `parallel_queries` helper prints failures) — these can be fixed after deployment

### Iterative refinement
The first deployment will likely have some broken queries (wrong column names, missing tables). This is expected. The architecture (parallel_queries returning empty results for failed queries, sections skipping empty data) means the report still renders — it just has gaps. Fix individual queries iteratively based on the server logs.

### Adding more queries
The spec calls for ~50 queries. Task 2 includes ~40 as a representative set. Additional queries to add in follow-up iterations:
- More percentile queries (safety, neighborhood, health metrics)
- `rolling_sales` for recent sale price
- `ll44_income_rent` for rent stabilization
- `property_valuation` for market/assessed value breakdown
- `hpd_litigations` for litigation count
- `pools`, `spray_showers`, `play_areas` for nearest recreation
- `dob_permit_issuance` for permit count
- Voter turnout data when available
