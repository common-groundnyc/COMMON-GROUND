"""neighborhood() super tool — absorbs 8 area/neighborhood tools into one dispatch.

Views:
  full        → neighborhood_portrait + complaints
  compare     → neighborhood_compare (side-by-side ZIP comparison)
  gentrification → gentrification_tracker (displacement signals)
  environment → environmental_justice (pollution, air quality, EJ score)
  hotspot     → hotspot_map (H3 hex heatmap)
  area        → area_snapshot (spatial radius query)
  restaurants → restaurant_lookup (restaurant search by name)
"""

import re
import time
import uuid
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS, ZIP_PATTERN, COORDS_PATTERN

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_ZIP_PATTERN = ZIP_PATTERN
_COORDS_PATTERN = COORDS_PATTERN


# ---------------------------------------------------------------------------
# SQL constants — complaints_by_zip
# ---------------------------------------------------------------------------

COMPLAINTS_BY_ZIP_SQL = """
WITH hpd AS (
    SELECT major_category AS category, 'HPD' AS source,
           COUNT(DISTINCT complaint_id) AS cnt
    FROM lake.housing.hpd_complaints
    WHERE post_code = ?
      AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    GROUP BY major_category
),
svc311 AS (
    SELECT problem_formerly_complaint_type AS category, '311' AS source,
           COUNT(*) AS cnt
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip = ?
      AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL (?) DAY
    GROUP BY problem_formerly_complaint_type
),
combined AS (
    SELECT * FROM hpd
    UNION ALL
    SELECT * FROM svc311
)
SELECT source, category, cnt
FROM combined
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# SQL constants — neighborhood_compare
# ---------------------------------------------------------------------------

NEIGHBORHOOD_COMPARE_SQL = """
WITH input_zips AS (
    SELECT UNNEST(?::VARCHAR[]) AS zip
),
-- 311 is the universal crosswalk: ZIP → precinct, ZIP → community district
zip_to_precinct AS (
    SELECT incident_zip AS zip,
           REGEXP_EXTRACT(police_precinct, '\\d+') AS precinct,
           COUNT(*) AS weight
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND police_precinct NOT IN ('Unspecified', '')
      AND police_precinct IS NOT NULL
    GROUP BY 1, 2
),
zip_primary_precinct AS (
    SELECT zip, precinct
    FROM (
        SELECT zip, precinct,
               ROW_NUMBER() OVER (PARTITION BY zip ORDER BY weight DESC) AS rn
        FROM zip_to_precinct
    ) WHERE rn = 1
),
zip_to_cd AS (
    SELECT incident_zip AS zip,
           CASE SPLIT_PART(community_board, ' ', 2)
               WHEN 'MANHATTAN' THEN '1'
               WHEN 'BRONX' THEN '2'
               WHEN 'BROOKLYN' THEN '3'
               WHEN 'QUEENS' THEN '4'
               WHEN 'STATEN ISLAND' THEN '5'
           END || LPAD(SPLIT_PART(community_board, ' ', 1), 2, '0') AS borocd,
           COUNT(*) AS weight
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND community_board NOT LIKE '%Unspecified%'
      AND community_board IS NOT NULL
      AND community_board != ''
    GROUP BY 1, 2
),
zip_primary_cd AS (
    SELECT zip, borocd
    FROM (
        SELECT zip, borocd,
               ROW_NUMBER() OVER (PARTITION BY zip ORDER BY weight DESC) AS rn
        FROM zip_to_cd
    ) WHERE rn = 1
),
-- Dimension: Crime (via precinct crosswalk, historic + YTD)
all_crimes AS (
    SELECT addr_pct_cd, law_cat_cd, ofns_desc, rpt_dt
    FROM lake.public_safety.nypd_complaints_historic
    WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    UNION ALL
    SELECT addr_pct_cd, law_cat_cd, ofns_desc, rpt_dt
    FROM lake.public_safety.nypd_complaints_ytd
),
crime AS (
    SELECT zp.zip,
           COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
           COUNT(*) FILTER (WHERE ofns_desc ILIKE '%assault%') AS assaults
    FROM all_crimes c
    JOIN zip_primary_precinct zp ON c.addr_pct_cd = zp.precinct
    WHERE TRY_CAST(c.rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY zp.zip
),
-- Dimension: Restaurants (direct ZIP join)
restaurants AS (
    SELECT zipcode AS zip,
           COUNT(DISTINCT camis) AS total_restaurants,
           ROUND(100.0 * COUNT(DISTINCT camis) FILTER (WHERE grade = 'A')
               / NULLIF(COUNT(DISTINCT camis) FILTER (WHERE grade IN ('A','B','C')), 0), 1) AS pct_grade_a
    FROM lake.health.restaurant_inspections
    WHERE zipcode IN (SELECT zip FROM input_zips)
    GROUP BY zipcode
),
-- Dimension: HPD housing complaints (direct ZIP join)
housing AS (
    SELECT post_code AS zip,
           COUNT(DISTINCT complaint_id) AS hpd_complaints_1yr
    FROM lake.housing.hpd_complaints
    WHERE post_code IN (SELECT zip FROM input_zips)
      AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY post_code
),
-- Dimension: 311 NYPD calls as noise/quality-of-life proxy (direct ZIP join)
noise AS (
    SELECT incident_zip AS zip,
           COUNT(*) AS nypd_311_calls_1yr
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT zip FROM input_zips)
      AND agency = 'NYPD'
      AND TRY_CAST(created_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY incident_zip
),
-- Dimension: ACS median income by ZCTA (direct ZIP join)
income AS (
    SELECT zcta AS zip,
           TRY_CAST(median_household_income AS DOUBLE) AS avg_agi
    FROM lake.federal.acs_zcta_demographics
    WHERE zcta IN (SELECT zip FROM input_zips)
)
SELECT z.zip,
       cr.felonies,
       cr.assaults,
       n.nypd_311_calls_1yr AS noise_calls,
       r.total_restaurants AS restaurants,
       r.pct_grade_a,
       h.hpd_complaints_1yr AS housing_complaints,
       i.avg_agi AS avg_income
FROM input_zips z
LEFT JOIN crime cr ON z.zip = cr.zip
LEFT JOIN restaurants r ON z.zip = r.zip
LEFT JOIN housing h ON z.zip = h.zip
LEFT JOIN noise n ON z.zip = n.zip
LEFT JOIN income i ON z.zip = i.zip
ORDER BY z.zip
"""

# ---------------------------------------------------------------------------
# SQL constants — restaurant_lookup
# ---------------------------------------------------------------------------

RESTAURANT_FUZZY_MATCH_SQL = """
WITH candidates AS (
    SELECT DISTINCT camis, dba, cuisine_description, boro,
           building || ' ' || street AS address, zipcode, phone,
           rapidfuzz_token_set_ratio(LOWER(?), LOWER(dba)) AS name_score
    FROM lake.health.restaurant_inspections
    WHERE rapidfuzz_token_set_ratio(LOWER(?), LOWER(dba)) >= 65
      {zip_filter}
    ORDER BY name_score DESC
    LIMIT 10
)
SELECT * FROM candidates
"""

RESTAURANT_INSPECTIONS_SQL = """
SELECT inspection_date, grade, score, inspection_type,
       STRING_AGG(DISTINCT violation_description, ' | ') AS violations
FROM lake.health.restaurant_inspections
WHERE camis = ?
  AND inspection_date IS NOT NULL
GROUP BY inspection_date, grade, score, inspection_type
ORDER BY inspection_date DESC
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL constants — area_snapshot
# ---------------------------------------------------------------------------

AREA_SNAPSHOT_SQL = """
WITH pt AS (
    SELECT ST_Point(?, ?) AS geom  -- lng, lat
),
-- Crimes within radius (historic + YTD, last 365 days with 730-day lookback on historic)
all_nearby_crimes AS (
    SELECT law_cat_cd, ofns_desc, rpt_dt
    FROM lake.spatial.nypd_crimes c, pt
    WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
      AND TRY_CAST(c.rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    UNION ALL
    SELECT law_cat_cd, ofns_desc, rpt_dt
    FROM lake.spatial.nypd_crimes_ytd c, pt
    WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
),
nearby_crimes AS (
    SELECT law_cat_cd, ofns_desc, COUNT(*) AS cnt
    FROM all_nearby_crimes
    WHERE TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY law_cat_cd, ofns_desc
),
crime_summary AS (
    SELECT
        SUM(cnt) AS total_crimes,
        SUM(cnt) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
        SUM(cnt) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors
    FROM nearby_crimes
),
crime_top AS (
    SELECT STRING_AGG(ofns_desc || ' (' || cnt || ')', ', ' ORDER BY cnt DESC) AS top_crimes
    FROM (SELECT ofns_desc, cnt FROM nearby_crimes ORDER BY cnt DESC LIMIT 5)
),
-- Restaurants within radius (latest inspection per restaurant)
nearby_restaurants AS (
    SELECT DISTINCT ON (r.camis) r.dba, r.cuisine_description, r.grade, r.score,
           ROUND(ST_Distance(r.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
    FROM lake.spatial.restaurant_inspections r, pt
    WHERE r.geom IS NOT NULL
      AND ST_DWithin(r.geom, pt.geom, ?)
      AND r.grade IN ('A', 'B', 'C')
    ORDER BY r.camis, r.inspection_date DESC
),
restaurant_summary AS (
    SELECT COUNT(*) AS total_restaurants,
           ROUND(100.0 * COUNT(*) FILTER (WHERE grade = 'A') / NULLIF(COUNT(*), 0), 0) AS pct_a,
           COUNT(*) FILTER (WHERE grade = 'C') AS grade_c_count
    FROM nearby_restaurants
),
-- Subway stops within radius
nearby_subway AS (
    SELECT s.name, s.line,
           ROUND(ST_Distance(s.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
    FROM lake.spatial.subway_stops s, pt
    WHERE s.geom IS NOT NULL
      AND ST_DWithin(s.geom, pt.geom, ?)
    ORDER BY dist_m
),
-- 311 complaints within radius (last 180 days, top categories)
nearby_311 AS (
    SELECT agency, COUNT(*) AS cnt
    FROM lake.spatial.n311_complaints c, pt
    WHERE c.geom IS NOT NULL
      AND ST_DWithin(c.geom, pt.geom, ?)
      AND TRY_CAST(c.created_date AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
    GROUP BY agency
),
-- Rat inspections within radius (geom is lat/lng swapped in this table)
nearby_rats AS (
    SELECT COUNT(*) AS total_inspections,
           COUNT(*) FILTER (WHERE "Result" ILIKE '%Rat Activity%') AS active_rats
    FROM lake.spatial.rat_inspections r, pt
    WHERE r.geom IS NOT NULL
      AND ST_DWithin(ST_Point(ST_Y(r.geom), ST_X(r.geom)), pt.geom, ?)
),
-- Trees within radius
nearby_trees AS (
    SELECT COUNT(*) AS tree_count,
           COUNT(*) FILTER (WHERE health = 'Good') AS healthy_trees
    FROM lake.spatial.street_trees t, pt
    WHERE t.geom IS NOT NULL
      AND ST_DWithin(t.geom, pt.geom, ?)
)
SELECT 'summary' AS section,
       cs.total_crimes, cs.felonies, cs.misdemeanors, ct.top_crimes,
       rs.total_restaurants, rs.pct_a, rs.grade_c_count,
       nr.total_inspections AS rat_inspections, nr.active_rats,
       nt.tree_count, nt.healthy_trees
FROM crime_summary cs, crime_top ct, restaurant_summary rs, nearby_rats nr, nearby_trees nt
"""

# ---------------------------------------------------------------------------
# SQL constants — gentrification_tracker
# ---------------------------------------------------------------------------

GENTRIFICATION_SIGNALS_SQL = """
WITH quarters AS (
    SELECT UNNEST(GENERATE_SERIES(
        DATE_TRUNC('quarter', DATE '2020-01-01'),
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL 90 DAY),
        INTERVAL 1 QUARTER
    )) AS q
),
-- New restaurants appearing per quarter (first inspection = proxy for opening)
new_restaurants AS (
    SELECT zipcode AS zip, DATE_TRUNC('quarter', MIN(TRY_CAST(inspection_date AS DATE))) AS q,
           COUNT(*) AS cnt
    FROM lake.health.restaurant_inspections
    WHERE zipcode IN (SELECT UNNEST(?::VARCHAR[]))
    GROUP BY zipcode, camis
),
new_rest_q AS (
    SELECT zip, q, COUNT(*) AS new_restaurants
    FROM new_restaurants
    WHERE q >= DATE '2020-01-01'
    GROUP BY zip, q
),
-- HPD complaints per quarter (tenant turnover signal)
hpd_q AS (
    SELECT post_code AS zip,
           DATE_TRUNC('quarter', TRY_CAST(received_date AS DATE)) AS q,
           COUNT(DISTINCT complaint_id) AS hpd_complaints
    FROM lake.housing.hpd_complaints
    WHERE post_code IN (SELECT UNNEST(?::VARCHAR[]))
      AND TRY_CAST(received_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- 311 NYPD calls per quarter (nightlife/noise proxy)
noise_q AS (
    SELECT incident_zip AS zip,
           DATE_TRUNC('quarter', TRY_CAST(created_date AS DATE)) AS q,
           COUNT(*) AS noise_calls
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND agency = 'NYPD'
      AND TRY_CAST(created_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- DOB ECB violations per quarter (construction boom proxy)
dob_q AS (
    SELECT respondent_zip AS zip,
           DATE_TRUNC('quarter', TRY_STRPTIME(issue_date, '%Y%m%d')) AS q,
           COUNT(*) AS dob_violations
    FROM lake.housing.dob_ecb_violations
    WHERE respondent_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND TRY_STRPTIME(issue_date, '%Y%m%d') >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- 311 DOB calls per quarter (construction complaint proxy)
dob_311_q AS (
    SELECT incident_zip AS zip,
           DATE_TRUNC('quarter', TRY_CAST(created_date AS DATE)) AS q,
           COUNT(*) AS construction_complaints
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IN (SELECT UNNEST(?::VARCHAR[]))
      AND agency = 'DOB'
      AND TRY_CAST(created_date AS DATE) >= DATE '2020-01-01'
    GROUP BY 1, 2
),
-- Cross join all ZIPs with all quarters to fill gaps
zip_list AS (SELECT UNNEST(?::VARCHAR[]) AS zip),
grid AS (
    SELECT z.zip, q.q FROM zip_list z CROSS JOIN quarters q
)
SELECT g.zip, g.q,
       COALESCE(r.new_restaurants, 0) AS new_restaurants,
       COALESCE(h.hpd_complaints, 0) AS hpd_complaints,
       COALESCE(n.noise_calls, 0) AS noise_calls,
       COALESCE(d.dob_violations, 0) AS dob_violations,
       COALESCE(c.construction_complaints, 0) AS construction_complaints
FROM grid g
LEFT JOIN new_rest_q r ON g.zip = r.zip AND g.q = r.q
LEFT JOIN hpd_q h ON g.zip = h.zip AND g.q = h.q
LEFT JOIN noise_q n ON g.zip = n.zip AND g.q = n.q
LEFT JOIN dob_q d ON g.zip = d.zip AND g.q = d.q
LEFT JOIN dob_311_q c ON g.zip = c.zip AND g.q = c.q
ORDER BY g.zip, g.q
"""

IRS_INCOME_TREND_SQL = """
SELECT zcta AS zip, acs_year AS tax_year,
       TRY_CAST(median_household_income AS DOUBLE) AS avg_agi
FROM lake.federal.acs_zcta_demographics
WHERE zcta IN (SELECT UNNEST(?::VARCHAR[]))
ORDER BY zcta
"""

GENT_HMDA_INVESTOR_SQL = """
SELECT NULL AS activity_year WHERE FALSE
"""

GENT_EVICTION_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(executed_date AS DATE)) AS yr,
       COUNT(*) AS evictions
FROM lake.housing.evictions
WHERE UPPER(borough) = UPPER(?)
  AND TRY_CAST(executed_date AS DATE) >= DATE '2018-01-01'
GROUP BY yr ORDER BY yr
"""

GENT_REZONING_SQL = """
SELECT NULL AS project_na WHERE FALSE
"""

GENT_AFFORDABLE_LOSS_SQL = """
SELECT COUNT(*) AS total_buildings,
       SUM(TRY_CAST(total_units AS INT)) AS total_units,
       SUM(TRY_CAST(counted_rental_units AS INT)) AS rental_units
FROM lake.housing.affordable_housing
WHERE postcode IN (SELECT UNNEST(?::VARCHAR[]))
"""

# ---------------------------------------------------------------------------
# SQL constants — environmental_justice
# ---------------------------------------------------------------------------

EJ_AIR_QUALITY_SQL = """
SELECT name, data_value, time_period, measure
FROM lake.environment.air_quality
WHERE geo_type_name = 'CD'
  AND geo_place_name ILIKE '%' || ? || '%'
  AND name IN ('Fine particles (PM 2.5)', 'Nitrogen dioxide (NO2)',
               'Ozone (O3)', 'Deaths due to PM2.5',
               'Asthma emergency department visits due to PM2.5')
ORDER BY name, time_period DESC
"""

EJ_AIR_CITY_AVG_SQL = """
SELECT name, AVG(TRY_CAST(data_value AS DOUBLE)) AS avg_val
FROM lake.environment.air_quality
WHERE geo_type_name = 'CD'
  AND name IN ('Fine particles (PM 2.5)', 'Nitrogen dioxide (NO2)')
  AND time_period = (
      SELECT MAX(time_period) FROM lake.environment.air_quality
      WHERE geo_type_name = 'CD' AND name = 'Fine particles (PM 2.5)'
  )
GROUP BY name
"""

EJ_AREAS_SQL = """
SELECT NULL AS NTAName, NULL AS EJDesignat WHERE FALSE
"""

EJ_WASTE_SQL = """
SELECT name, type, street_address, borough_city, zipcode
FROM lake.environment.waste_transfer
WHERE zipcode = ? OR borough_city ILIKE '%' || ? || '%'
"""

EJ_EPA_SQL = """
SELECT facility_name, registry_id
FROM lake.federal.epa_echo_facilities
WHERE zip_code = ?
"""

EJ_E_DESIGNATIONS_SQL = """
SELECT bbl, enumber, description
FROM lake.environment.e_designations
WHERE borocode = ?
LIMIT 50
"""

EJ_HEALTH_ASTHMA_SQL = """
SELECT measure AS indicator_name,
       data_value AS value, data_value AS display_value,
       locationname AS geo_name, 'Place' AS geo_type
FROM lake.health.cdc_places
WHERE locationname ILIKE '%' || ? || '%'
  AND measure IN ('Current asthma among adults', 'Mental health not good for >=14 days among adults')
ORDER BY measure
LIMIT 20
"""

EJ_HEALTH_CITY_AVG_SQL = """
SELECT measure AS indicator_name, AVG(TRY_CAST(data_value AS DOUBLE)) AS avg_val
FROM lake.health.cdc_places
WHERE measure IN ('Current asthma among adults', 'Mental health not good for >=14 days among adults')
GROUP BY measure
"""

EJ_RATS_SQL = """
SELECT result, COUNT(*) AS cnt
FROM lake.health.rodent_inspections
WHERE zip_code = ?
GROUP BY result ORDER BY cnt DESC
"""

EJ_TREES_SQL = """
SELECT COUNT(*) AS tree_count
FROM lake.environment.street_trees
WHERE zipcode = ?
"""

EJ_311_ENV_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND complaint_type IN ('Air Quality', 'Asbestos', 'Lead', 'Mold',
                          'Indoor Air Quality', 'Water Quality',
                          'Noise - Residential', 'Rodent', 'Unsanitary Condition',
                          'HEAT/HOT WATER', 'Industrial Waste')
GROUP BY complaint_type ORDER BY cnt DESC
"""

EJ_FLOOD_SQL = """
SELECT fvi_score, population, pct_nonwhite, pct_poverty
FROM lake.environment.flood_vulnerability
WHERE zip_code = ?
LIMIT 1
"""

EJ_HVI_SQL = """
SELECT hvi, zcta20
FROM lake.environment.heat_vulnerability
WHERE zcta20 = ?
LIMIT 1
"""

EJ_DSNY_SQL = """
SELECT communitydistrict,
       SUM(TRY_CAST(refusetonscollected AS DOUBLE)) AS refuse,
       SUM(TRY_CAST(papertonscollected AS DOUBLE)) AS paper,
       SUM(TRY_CAST(mgptonscollected AS DOUBLE)) AS mgp
FROM lake.environment.dsny_tonnage
WHERE communitydistrict ILIKE ?
GROUP BY communitydistrict
"""

# ---------------------------------------------------------------------------
# SQL constants — neighborhood_portrait
# ---------------------------------------------------------------------------

PORTRAIT_CUISINE_SQL = """
SELECT cuisine_description, COUNT(DISTINCT camis) AS restaurants
FROM lake.health.restaurant_inspections
WHERE zipcode = ?
  AND grade IS NOT NULL
GROUP BY cuisine_description
ORDER BY restaurants DESC
LIMIT 10
"""

PORTRAIT_CUISINE_CITY_SQL = """
SELECT cuisine_description, COUNT(DISTINCT camis) AS restaurants
FROM lake.health.restaurant_inspections
WHERE grade IS NOT NULL
GROUP BY cuisine_description
ORDER BY restaurants DESC
LIMIT 20
"""

PORTRAIT_GRADES_SQL = """
WITH latest AS (
    SELECT camis, grade,
           ROW_NUMBER() OVER (PARTITION BY camis ORDER BY grade_date DESC) AS rn
    FROM lake.health.restaurant_inspections
    WHERE zipcode = ? AND grade IN ('A','B','C')
)
SELECT grade, COUNT(*) AS cnt FROM latest WHERE rn = 1 GROUP BY grade ORDER BY grade
LIMIT 3
"""

PORTRAIT_BUILDINGS_SQL = """
SELECT
    COUNT(*) AS total,
    AVG(TRY_CAST(yearbuilt AS INT)) FILTER (WHERE TRY_CAST(yearbuilt AS INT) > 1800) AS avg_year,
    AVG(TRY_CAST(numfloors AS DOUBLE)) AS avg_floors,
    MEDIAN(TRY_CAST(assesstot AS DOUBLE)) AS median_assessed,
    0 AS landmarks,
    0 AS in_hist_district
FROM lake.city_government.pluto
WHERE zipcode = ?
"""

PORTRAIT_311_SQL = """
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
GROUP BY complaint_type ORDER BY cnt DESC LIMIT 10
"""

PORTRAIT_311_NOISE_SQL = """
SELECT COUNT(*) AS noise_total
FROM lake.social_services.n311_service_requests
WHERE incident_zip = ?
  AND complaint_type ILIKE '%noise%'
LIMIT 1
"""

PORTRAIT_311_CITY_NOISE_SQL = """
SELECT
    COUNT(*) FILTER (WHERE complaint_type ILIKE '%noise%') * 1.0 / NULLIF(COUNT(DISTINCT incident_zip), 0) AS avg_noise_per_zip
FROM (
    SELECT complaint_type, incident_zip
    FROM lake.social_services.n311_service_requests
    WHERE incident_zip IS NOT NULL AND LENGTH(incident_zip) = 5
      AND created_date >= CURRENT_DATE - INTERVAL '3 years'
) sub
LIMIT 1
"""

PORTRAIT_CRIME_SQL = """
WITH precinct AS (
    SELECT LPAD(CAST(policeprct AS VARCHAR), 3, '0') AS pct
    FROM lake.city_government.pluto
    WHERE zipcode = ?
      AND policeprct IS NOT NULL
    LIMIT 1
)
SELECT
    COUNT(*) FILTER (WHERE TRY_CAST(SUBSTRING(cmplnt_fr_dt, -4) AS INT) = 2024) AS crimes_2024,
    COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY' AND TRY_CAST(SUBSTRING(cmplnt_fr_dt, -4) AS INT) = 2024) AS felonies_2024
FROM lake.public_safety.nypd_complaints_ytd
WHERE SUBSTRING(CAST(cmplnt_num AS VARCHAR), 1, 3) = (SELECT pct FROM precinct)
LIMIT 1
"""

PORTRAIT_BIZ_SQL = """
SELECT industry, COUNT(*) AS cnt
FROM lake.business.issued_licenses
WHERE zip = ?
  AND license_status = 'Active'
GROUP BY industry ORDER BY cnt DESC LIMIT 8
"""


# ---------------------------------------------------------------------------
# Borough lookup from ZIP prefix
# ---------------------------------------------------------------------------

def _borough_from_zip(zipcode: str) -> tuple[str, str]:
    """Return (borough_name, boro_code) from numeric ZIP range."""
    z = int(zipcode)
    if 10001 <= z <= 10282:
        return "Manhattan", "1"
    elif 10451 <= z <= 10475:
        return "Bronx", "2"
    elif 11201 <= z <= 11256:
        return "Brooklyn", "3"
    elif (11001 <= z <= 11109) or (11351 <= z <= 11697):
        return "Queens", "4"
    elif 10301 <= z <= 10314:
        return "Staten Island", "5"
    return "NYC", "1"


def _boro_name_from_zip(zipcode: str) -> str:
    """Return friendly borough name for portrait header."""
    borough, _ = _borough_from_zip(zipcode)
    if borough == "Bronx":
        return "The Bronx"
    return borough


# ---------------------------------------------------------------------------
# View implementations
# ---------------------------------------------------------------------------

def _view_complaints(pool, zipcode: str, days: int = 365) -> ToolResult:
    """HPD + 311 complaints for a ZIP code."""
    cols, rows = execute(pool, COMPLAINTS_BY_ZIP_SQL, [zipcode, days, zipcode, days])

    total = sum(r[2] for r in rows) if rows else 0
    sources = sorted(set(r[0] for r in rows)) if rows else []
    top5 = rows[:5]
    top5_text = ", ".join(f"{r[1]} ({r[2]})" for r in top5) if top5 else "none"

    summary = (
        f"ZIP {zipcode} last {days} days: {total:,} complaints"
        f" from {', '.join(sources) if sources else 'no sources'}\n"
        f"Top: {top5_text}"
    )

    return make_result(
        summary,
        cols,
        rows,
        {"total_complaints": total, "days": days, "sources": sources},
    )


def _view_portrait(pool, zipcode: str) -> ToolResult:
    """Full neighborhood portrait — cuisine, buildings, 311, safety, business."""
    t0 = time.time()
    boro_name = _boro_name_from_zip(zipcode)

    lines = [
        f"NEIGHBORHOOD PORTRAIT: {zipcode}, {boro_name}",
        "=" * 60,
    ]

    # --- Cuisine fingerprint ---
    cols_c, rows_c = safe_query(pool, PORTRAIT_CUISINE_SQL, [zipcode])
    cols_city, rows_city = safe_query(pool, PORTRAIT_CUISINE_CITY_SQL)
    city_totals: dict[str, int] = {}
    city_zips = 200  # approximate number of NYC ZIPs
    if rows_city:
        for row in rows_city:
            r = dict(zip(cols_city, row))
            city_totals[r["cuisine_description"]] = int(r["restaurants"])

    if rows_c:
        lines.append("\nCUISINE FINGERPRINT")
        total_local = sum(int(dict(zip(cols_c, r))["restaurants"]) for r in rows_c)
        standout = None
        standout_ratio = 0.0
        for row in rows_c[:8]:
            r = dict(zip(cols_c, row))
            cuisine = r["cuisine_description"]
            count = int(r["restaurants"])
            pct = round(count / total_local * 100) if total_local else 0
            city_avg = city_totals.get(cuisine, 0) / city_zips if city_zips else 0
            ratio = count / city_avg if city_avg > 1 else 0
            marker = ""
            if ratio > 2:
                marker = f"  ({ratio:.1f}x city average)"
                if ratio > standout_ratio:
                    standout_ratio = ratio
                    standout = (cuisine, ratio)
            lines.append(f"  {cuisine}: {count} restaurants ({pct}%){marker}")

        if standout:
            lines.append(f"\n  Signature: {standout[0]} — {standout[1]:.1f}x the city average")

    # --- Restaurant grades ---
    cols_g, rows_g = safe_query(pool, PORTRAIT_GRADES_SQL, [zipcode])
    if rows_g:
        grades = {dict(zip(cols_g, r))["grade"]: int(dict(zip(cols_g, r))["cnt"]) for r in rows_g}
        total_graded = sum(grades.values())
        pct_a = round(grades.get("A", 0) / total_graded * 100) if total_graded else 0
        lines.append("\nFOOD SAFETY")
        lines.append(f"  {total_graded} graded restaurants: {pct_a}% Grade A")
        for g in ["A", "B", "C"]:
            if g in grades:
                lines.append(f"    {g}: {grades[g]}")

    # --- Building stock ---
    cols_b, rows_b = safe_query(pool, PORTRAIT_BUILDINGS_SQL, [zipcode])
    if rows_b:
        b = dict(zip(cols_b, rows_b[0]))
        total_bldg = int(b.get("total") or 0)
        avg_year = int(float(b.get("avg_year") or 0))
        avg_floors = round(float(b.get("avg_floors") or 0), 1)
        median_val = float(b.get("median_assessed") or 0)
        landmarks_ct = int(b.get("landmarks") or 0)
        hist_ct = int(b.get("in_hist_district") or 0)

        lines.append("\nBUILDING STOCK")
        lines.append(f"  {total_bldg:,} buildings")
        if avg_year > 1800:
            lines.append(f"  Average built: {avg_year}")
        lines.append(f"  Average height: {avg_floors} floors")
        lines.append(f"  Median assessed value: ${median_val:,.0f}")
        if landmarks_ct:
            lines.append(f"  Landmarks: {landmarks_ct}")
        if hist_ct:
            lines.append(f"  In historic district: {hist_ct}")

    # --- 311 character ---
    cols_311, rows_311 = safe_query(pool, PORTRAIT_311_SQL, [zipcode])
    if rows_311:
        lines.append("\n311 CHARACTER (since 2020)")
        for row in rows_311[:8]:
            r = dict(zip(cols_311, row))
            cnt = int(r.get("cnt") or 0)
            lines.append(f"  {r.get('complaint_type')}: {cnt:,}")

    # Noise comparison
    cols_noise, rows_noise = safe_query(pool, PORTRAIT_311_NOISE_SQL, [zipcode])
    cols_cn, rows_cn = safe_query(pool, PORTRAIT_311_CITY_NOISE_SQL)
    if rows_noise and rows_cn:
        local_noise = int(dict(zip(cols_noise, rows_noise[0])).get("noise_total") or 0)
        city_avg_noise = float(dict(zip(cols_cn, rows_cn[0])).get("avg_noise_per_zip") or 1)
        if city_avg_noise > 0:
            noise_ratio = local_noise / city_avg_noise
            if noise_ratio > 1.5:
                lines.append(f"\n  Noise level: {noise_ratio:.1f}x the city average — a loud neighborhood")
            elif noise_ratio < 0.5:
                lines.append(f"\n  Noise level: {noise_ratio:.1f}x the city average — notably quiet")
            else:
                lines.append("\n  Noise level: about average for NYC")

    # --- Business mix ---
    cols_biz, rows_biz = safe_query(pool, PORTRAIT_BIZ_SQL, [zipcode])
    if rows_biz:
        lines.append("\nBUSINESS MIX (active licenses)")
        for row in rows_biz[:6]:
            r = dict(zip(cols_biz, row))
            lines.append(f"  {r.get('industry')}: {int(r.get('cnt') or 0):,}")

    # H3-based safety snapshot
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        _, centroid = execute(pool, *h3_zip_centroid_sql(zipcode))
        if centroid and centroid[0][0] is not None:
            c_lat, c_lng = centroid[0][1], centroid[0][2]
            _, stats = execute(pool, *h3_neighborhood_stats_sql(c_lat, c_lng, radius_rings=6))
            if stats:
                s = dict(zip(
                    ["total_crimes", "total_arrests", "restaurants_h3", "n311_calls", "shootings", "street_trees"],
                    stats[0],
                ))
                lines.append("\nSAFETY SNAPSHOT (H3 hex radius)")
                lines.append(f"  Crimes nearby: {s.get('total_crimes', 0):,}")
                lines.append(f"  Arrests nearby: {s.get('total_arrests', 0):,}")
                lines.append(f"  Shootings nearby: {s.get('shootings', 0):,}")
                lines.append(f"  311 calls nearby: {s.get('n311_calls', 0):,}")
                lines.append(f"  Street trees: {s.get('street_trees', 0):,}")
    except Exception:
        pass

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"zipcode": zipcode, "borough": boro_name},
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


def _view_full(pool, zipcode: str) -> ToolResult:
    """Full view = portrait + complaints merged into one response."""
    portrait = _view_portrait(pool, zipcode)
    complaints = _view_complaints(pool, zipcode)

    merged_text = portrait.content + "\n\n" + complaints.content
    merged_structured = {
        **(portrait.structured_content or {}),
        "complaints": complaints.structured_content,
    }
    merged_meta = {**(portrait.meta or {}), **(complaints.meta or {})}

    return ToolResult(
        content=merged_text,
        structured_content=merged_structured,
        meta=merged_meta,
    )


def _view_compare(pool, zip_codes: list[str]) -> ToolResult:
    """Side-by-side comparison of 2-7 ZIP codes."""
    if len(zip_codes) < 2:
        raise ToolError("Provide at least 2 ZIP codes to compare")
    if len(zip_codes) > 7:
        raise ToolError("Maximum 7 ZIP codes per comparison")
    for z in zip_codes:
        if not _ZIP_PATTERN.match(z):
            raise ToolError(f"Invalid ZIP code: {z}. Must be exactly 5 digits.")

    # Try H3-based comparison first
    try:
        from spatial import h3_zip_centroid_sql, h3_neighborhood_stats_sql
        results = []
        for z in zip_codes:
            centroid_cols, centroid_rows = execute(pool, *h3_zip_centroid_sql(z))
            if not centroid_rows or centroid_rows[0][0] is None:
                continue
            center = dict(zip(centroid_cols, centroid_rows[0]))
            stats_cols, stats_rows = execute(
                pool,
                *h3_neighborhood_stats_sql(center["center_lat"], center["center_lng"], radius_rings=8),
            )
            if stats_rows:
                s = dict(zip(stats_cols, stats_rows[0]))
                s["zip"] = z
                results.append(s)

        if len(results) >= 2:
            # Supplement with ZIP-based income + housing (no H3 equivalent)
            for r in results:
                z = r["zip"]
                try:
                    _, inc = execute(pool, """
                        SELECT TRY_CAST(median_household_income AS DOUBLE)
                        FROM lake.federal.acs_zcta_demographics
                        WHERE zcta = ?
                    """, [z])
                    if inc and inc[0][0]:
                        r["avg_income"] = inc[0][0]
                except Exception:
                    pass
                try:
                    _, hpd = execute(pool, """
                        SELECT COUNT(DISTINCT complaint_id)
                        FROM lake.housing.hpd_complaints
                        WHERE post_code = ? AND TRY_CAST(received_date AS DATE) >= CURRENT_DATE - INTERVAL 365 DAY
                    """, [z])
                    if hpd and hpd[0][0]:
                        r["housing_complaints"] = hpd[0][0]
                except Exception:
                    pass

            lines = ["Neighborhood Comparison (H3 hex-based):"]
            for r in results:
                lines.append(f"\nZIP {r['zip']}:")
                lines.append(f"  Crime: {r.get('total_crimes', 0):,} incidents nearby")
                lines.append(f"  Arrests: {r.get('total_arrests', 0):,} nearby")
                lines.append(f"  Food: {r.get('restaurants', 0):,} restaurants")
                lines.append(f"  311 calls: {r.get('n311_calls', 0):,}")
                lines.append(f"  Shootings: {r.get('shootings', 0):,}")
                lines.append(f"  Trees: {r.get('street_trees', 0):,}")
                if r.get("avg_income"):
                    lines.append(f"  Income: ${r['avg_income']:,.0f} avg AGI")
                if r.get("housing_complaints"):
                    lines.append(f"  Housing: {r['housing_complaints']:,} HPD complaints/yr")

            return ToolResult(
                content="\n".join(lines),
                structured_content={"neighborhoods": results},
                meta={"zip_codes": zip_codes, "method": "h3"},
            )
    except Exception:
        pass  # Fall back to original SQL

    cols, rows = execute(pool, NEIGHBORHOOD_COMPARE_SQL, [zip_codes])

    if not rows:
        raise ToolError("No data found for the provided ZIP codes")

    lines = ["Neighborhood Comparison:"]
    for row in rows:
        d = dict(zip(cols, row))
        z = d["zip"]
        parts = [f"\nZIP {z}:"]
        if d.get("felonies") is not None:
            parts.append(f"  Crime: {d['felonies']} felonies, {d['assaults']} assaults/yr")
        if d.get("noise_calls") is not None:
            parts.append(f"  Noise: {d['noise_calls']:,} NYPD 311 calls/yr")
        if d.get("restaurants") is not None:
            parts.append(f"  Food: {d['restaurants']} restaurants ({d['pct_grade_a']}% A-grade)")
        if d.get("housing_complaints") is not None:
            parts.append(f"  Housing: {d['housing_complaints']:,} HPD complaints/yr")
        if d.get("avg_income") is not None:
            parts.append(f"  Income: ${d['avg_income']:,.0f} avg AGI")
        if d.get("commute_min") is not None:
            parts.append(f"  Commute: {d['commute_min']:.0f} min avg")
        if d.get("poverty_rate") is not None:
            parts.append(f"  Poverty: {d['poverty_rate']:.1f}%")
        if d.get("park_access_pct") is not None:
            parts.append(f"  Park access: {d['park_access_pct']:.0f}%")
        if d.get("bachelors_pct") is not None:
            parts.append(f"  College educated: {d['bachelors_pct']:.0f}%")
        lines.extend(parts)

    summary = "\n".join(lines)
    return ToolResult(
        content=summary + "\n\n" + format_text_table(cols, rows),
        structured_content={"neighborhoods": [dict(zip(cols, r)) for r in rows]},
        meta={"zip_codes": zip_codes, "dimensions": len(cols) - 1},
    )


def _view_gentrification(pool, zip_codes: list[str]) -> ToolResult:
    """Quarterly displacement signals since 2020 with 4-quarter forecasts."""
    if not zip_codes or len(zip_codes) > 5:
        raise ToolError("Provide 1-5 ZIP codes")
    for z in zip_codes:
        if not _ZIP_PATTERN.match(z):
            raise ToolError(f"Invalid ZIP: {z}")

    t0 = time.time()

    # 1. Get quarterly signals
    params = [zip_codes] * 6  # 6 references to zip list in SQL
    cols, rows = execute(pool, GENTRIFICATION_SIGNALS_SQL, params)
    if not rows:
        raise ToolError("No data found for these ZIP codes")

    # 2. Forecast each signal 4 quarters ahead
    signals = ["new_restaurants", "hpd_complaints", "noise_calls", "dob_violations", "construction_complaints"]
    forecasts: dict[str, list] = {}

    try:
        suffix = uuid.uuid4().hex[:8]
        with pool.cursor() as cur:
            tbl = f"_gent_tmp_{suffix}"
            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
            cur.execute(f"""CREATE TEMP TABLE {tbl} (
                zip VARCHAR, q TIMESTAMP,
                new_restaurants INTEGER, hpd_complaints INTEGER,
                noise_calls INTEGER, dob_violations INTEGER,
                construction_complaints INTEGER
            )""")
            if rows:
                cur.executemany(f"INSERT INTO {tbl} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

            for signal in signals:
                try:
                    pivot_tbl = f"_gent_{signal}_{suffix}"
                    cur.execute(f"DROP TABLE IF EXISTS {pivot_tbl}")
                    cur.execute(f"""
                        CREATE TEMP TABLE {pivot_tbl} AS
                        SELECT zip AS id, q AS ds, {signal} AS y
                        FROM {tbl}
                        ORDER BY zip, q
                    """)

                    fcst = cur.execute(f"""
                        SELECT * FROM ts_forecast_by('{pivot_tbl}', id, ds, y,
                            'AutoETS', 4, '1q', MAP{{}})
                    """).fetchall()
                    forecasts[signal] = fcst
                    cur.execute(f"DROP TABLE IF EXISTS {pivot_tbl}")
                except Exception as e:
                    print(f"Forecast failed for {signal}: {e}", flush=True)

            cur.execute(f"DROP TABLE IF EXISTS {tbl}")
    except Exception as e:
        print(f"Forecast setup failed: {e}", flush=True)

    # 3. Get IRS income trend
    inc_cols, inc_rows = execute(pool, IRS_INCOME_TREND_SQL, [zip_codes])

    # 4. Build readable summary per ZIP
    by_zip: dict[str, list] = {}
    for r in rows:
        by_zip.setdefault(r[0], []).append(r)

    signal_labels = {
        "new_restaurants": "Restaurants opened",
        "hpd_complaints": "HPD complaints",
        "noise_calls": "Noise/NYPD 311 calls",
        "dob_violations": "DOB violations",
        "construction_complaints": "Construction 311 calls",
    }

    lines = ["GENTRIFICATION PRESSURE TRACKER", "=" * 40]

    for zip_code in sorted(by_zip):
        zrows = by_zip[zip_code]
        lines.append(f"\nZIP {zip_code}:")
        lines.append("  TREND (2020-21 avg \u2192 2024-25 avg per quarter):")

        early = [r for r in zrows if str(r[1])[:4] in ("2020", "2021")]
        recent = [r for r in zrows if str(r[1])[:4] in ("2024", "2025")]

        if early and recent:
            for i, signal in enumerate(signals, 2):
                label = signal_labels.get(signal, signal)
                early_avg = sum(r[i] for r in early) / len(early)
                recent_avg = sum(r[i] for r in recent) / len(recent)
                if early_avg > 0:
                    pct = ((recent_avg - early_avg) / early_avg) * 100
                    arrow = "+" if pct > 0 else ""
                    lines.append(f"    {label}: {early_avg:.0f} \u2192 {recent_avg:.0f}/qtr ({arrow}{pct:.0f}%)")
                else:
                    lines.append(f"    {label}: 0 \u2192 {recent_avg:.0f}/qtr")

        # Forecasts
        fcst_lines = []
        for signal in signals:
            if signal in forecasts:
                label = signal_labels.get(signal, signal)
                zip_fcsts = [f for f in forecasts[signal] if f[0] == zip_code]
                if zip_fcsts:
                    last_fcst = zip_fcsts[-1]
                    fcst_lines.append(
                        f"    {label}: {last_fcst[3]:.0f} [{last_fcst[4]:.0f}\u2013{last_fcst[5]:.0f}]"
                    )
        if fcst_lines:
            lines.append("  FORECAST (Q+4, 95% CI):")
            lines.extend(fcst_lines)

        # Income trend
        zip_inc = [r for r in inc_rows if r[0] == zip_code]
        if zip_inc:
            first_inc = zip_inc[0]
            last_inc = zip_inc[-1]
            if first_inc[2] and last_inc[2]:
                inc_change = ((last_inc[2] - first_inc[2]) / first_inc[2]) * 100
                lines.append(
                    f"  Income: ${first_inc[2]:,.0f} ({first_inc[1]})"
                    f" \u2192 ${last_inc[2]:,.0f} ({last_inc[1]})"
                    f" ({'+' if inc_change > 0 else ''}{inc_change:.0f}%)"
                )

    # HMDA investor & lending activity (borough-level)
    boroughs_seen: set[str] = set()
    for z in zip_codes:
        pfx = z[:3]
        if pfx in ("100", "101", "102"):
            boro = "Manhattan"
        elif pfx in ("104", "105"):
            boro = "Bronx"
        elif pfx in ("112", "111"):
            boro = "Brooklyn"
        elif pfx in ("113", "114", "116"):
            boro = "Queens"
        elif pfx == "103":
            boro = "Staten Island"
        else:
            boro = None
        if boro and boro not in boroughs_seen:
            boroughs_seen.add(boro)
            _, hmda_rows = safe_query(pool, GENT_HMDA_INVESTOR_SQL, [boro])
            if hmda_rows:
                lines.append(f"\nHMDA LENDING \u2014 {boro}:")
                for r in hmda_rows:
                    inv_pct = f"{r[3]}%" if r[3] else "0%"
                    lines.append(f"  {r[0]}: {r[1]:,} loans ({inv_pct} investor)")
                latest = hmda_rows[-1]
                if latest[7] and latest[8]:
                    lines.append(f"  Denial rates (latest): Black {latest[7]}% vs White {latest[8]}%")
                    if latest[7] > latest[8]:
                        ratio = latest[7] / latest[8] if latest[8] > 0 else 0
                        lines.append(f"  Black applicants denied at {ratio:.1f}x the white rate")

            # Evictions
            _, evict_rows = safe_query(pool, GENT_EVICTION_SQL, [boro])
            if evict_rows:
                lines.append(f"\nEVICTIONS \u2014 {boro}:")
                for r in evict_rows:
                    if r[0]:
                        lines.append(f"  {int(r[0])}: {r[1]:,}")

    # Affordable housing in these ZIPs
    _, afford_rows = safe_query(pool, GENT_AFFORDABLE_LOSS_SQL, [zip_codes])
    if afford_rows and afford_rows[0][0]:
        total_bldg = int(afford_rows[0][0] or 0)
        total_units = int(afford_rows[0][1] or 0)
        rental_units = int(afford_rows[0][2] or 0)
        if total_bldg > 0:
            lines.append(f"\nAFFORDABLE HOUSING: {total_bldg} buildings, {total_units:,} units ({rental_units:,} rental)")

    # Recent rezonings (citywide, most recent)
    _, rezone_rows = safe_query(pool, GENT_REZONING_SQL, [])
    if rezone_rows:
        lines.append("\nRECENT REZONINGS (adopted, citywide):")
        for r in rezone_rows[:8]:
            eff = str(r[2])[:10] if r[2] else "N/A"
            lines.append(f"  {r[0]} \u2014 {eff} ({r[3]})")

    # Actionable steps
    lines.append("\nWHAT YOU CAN DO:")
    lines.append("  1. Right to Counsel: FREE lawyer for eviction if eligible \u2014 718-557-1379")
    lines.append("  2. Rent stabilization lookup: hcr.ny.gov/building-search")
    lines.append("  3. Report illegal rent increases: hcr.ny.gov or 718-739-6400")
    lines.append("  4. Attend Community Board land use hearings (rezonings must go through ULURP)")
    lines.append("  5. Churches United for Fair Housing: cuffh.org (anti-displacement organizing)")
    lines.append("  6. Right to the City Alliance: righttothecity.org")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)
    text += "\n\nQUARTERLY DETAIL:\n" + format_text_table(cols, rows, max_rows=40)

    structured = {
        "historical": [dict(zip(cols, r)) for r in rows],
        "income_trend": [dict(zip(inc_cols, r)) for r in inc_rows] if inc_rows else [],
        "forecasts": {
            signal: [{"zip": f[0], "horizon": f[1], "date": str(f[2]),
                       "forecast": f[3], "lo": f[4], "hi": f[5], "model": f[6]}
                      for f in flist]
            for signal, flist in forecasts.items()
        },
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"zip_codes": zip_codes, "query_time_ms": elapsed},
    )


def _view_environment(pool, zipcode: str) -> ToolResult:
    """Environmental justice report — pollution, air quality, EJ score."""
    if not _ZIP_PATTERN.match(zipcode):
        raise ToolError("ZIP code must be exactly 5 digits")

    t0 = time.time()
    borough, boro_code = _borough_from_zip(zipcode)

    lines = [
        f"ENVIRONMENTAL JUSTICE REPORT \u2014 ZIP {zipcode} ({borough})",
        "=" * 55,
    ]

    # Air quality
    _, aq_rows = safe_query(pool, EJ_AIR_QUALITY_SQL, [borough])
    _, aq_avg_rows = safe_query(pool, EJ_AIR_CITY_AVG_SQL, [])
    city_avgs = {r[0]: float(r[1]) for r in aq_avg_rows} if aq_avg_rows else {}

    if aq_rows:
        seen: dict[str, tuple] = {}
        for r in aq_rows:
            if r[0] not in seen:
                seen[r[0]] = r
        lines.append("\nAIR QUALITY:")
        for name, row in seen.items():
            val = row[1]
            period = row[2]
            avg = city_avgs.get(name)
            compare = f" (city avg: {avg:.1f})" if avg else ""
            lines.append(f"  {name}: {val} [{period}]{compare}")

    # EJ areas
    _, ej_rows = safe_query(pool, EJ_AREAS_SQL, [borough, zipcode])
    ej_count = sum(1 for r in ej_rows if r[1] and "EJ Area" in str(r[1]))
    uncertain = sum(1 for r in ej_rows if r[1] and "Uncertain" in str(r[1]))
    if ej_rows:
        lines.append(f"\nENVIRONMENTAL JUSTICE AREAS: {ej_count} EJ tracts, {uncertain} uncertain")
        for r in ej_rows[:5]:
            lines.append(f"  {r[0]}: {r[1]} (minority {r[2]}%, poverty {r[3]}%, pop {r[4]})")

    # Waste transfer stations
    _, waste_rows = safe_query(pool, EJ_WASTE_SQL, [zipcode, borough])
    if waste_rows:
        lines.append(f"\nWASTE FACILITIES: {len(waste_rows)} in area")
        for r in waste_rows[:8]:
            lines.append(f"  {r[0]} ({r[1]}) \u2014 {r[2]}")
    else:
        lines.append("\nWASTE FACILITIES: None in this ZIP")

    # EPA facilities
    _, epa_rows = safe_query(pool, EJ_EPA_SQL, [zipcode])
    if epa_rows:
        lines.append(f"\nEPA-REGULATED FACILITIES: {len(epa_rows)}")
        for r in epa_rows[:8]:
            lines.append(f"  {r[0]} (ID: {r[1]})")

    # E-designations (hazardous lots)
    _, edesig_rows = safe_query(pool, EJ_E_DESIGNATIONS_SQL, [boro_code])
    if edesig_rows:
        lines.append(f"\nE-DESIGNATIONS (known hazardous lots): {len(edesig_rows)} in borough")

    # Health outcomes
    _, health_rows = safe_query(pool, EJ_HEALTH_ASTHMA_SQL, [borough])
    _, health_avg_rows = safe_query(pool, EJ_HEALTH_CITY_AVG_SQL, [])
    health_avgs = {r[0]: float(r[1]) for r in health_avg_rows} if health_avg_rows else {}

    if health_rows:
        lines.append("\nHEALTH OUTCOMES:")
        seen_health: dict[str, tuple] = {}
        for r in health_rows:
            if r[0] not in seen_health:
                seen_health[r[0]] = r
        for name, row in seen_health.items():
            val = row[2] or row[1]
            avg = health_avgs.get(name)
            compare = ""
            if avg and row[1]:
                try:
                    ratio = float(row[1]) / avg
                    compare = f" ({ratio:.1f}x city avg)"
                except (ValueError, ZeroDivisionError):
                    pass
            lines.append(f"  {name}: {val}{compare}")

    # Rats
    _, rat_rows = safe_query(pool, EJ_RATS_SQL, [zipcode])
    total_inspections = 0
    active = 0
    if rat_rows:
        total_inspections = sum(int(r[1]) for r in rat_rows)
        active = sum(int(r[1]) for r in rat_rows if r[0] and "active" in str(r[0]).lower())
        lines.append(f"\nRAT INSPECTIONS: {total_inspections:,} total, {active:,} active signs")

    # Trees
    _, tree_rows = safe_query(pool, EJ_TREES_SQL, [zipcode])
    if tree_rows and tree_rows[0][0]:
        lines.append(f"\nSTREET TREES: {int(tree_rows[0][0]):,}")

    # 311 environmental complaints
    _, env311_rows = safe_query(pool, EJ_311_ENV_SQL, [zipcode])
    total_311 = 0
    if env311_rows:
        total_311 = sum(int(r[1]) for r in env311_rows)
        lines.append(f"\n311 ENVIRONMENTAL COMPLAINTS: {total_311:,}")
        for r in env311_rows[:6]:
            lines.append(f"  {r[0]}: {r[1]:,}")

    # Flood vulnerability
    _, flood_rows = safe_query(pool, EJ_FLOOD_SQL, [zipcode])
    if flood_rows and flood_rows[0][0]:
        lines.append(f"\nFLOOD VULNERABILITY: score {flood_rows[0][0]}")
        if flood_rows[0][2]:
            lines.append(f"  Non-white: {flood_rows[0][2]}%, Poverty: {flood_rows[0][3]}%")

    # Heat vulnerability
    _, hvi_rows = safe_query(pool, EJ_HVI_SQL, [zipcode])
    if hvi_rows and hvi_rows[0][0]:
        hvi = hvi_rows[0][0]
        lines.append(f"\nHEAT VULNERABILITY INDEX: {hvi}/5")
        if int(hvi) >= 4:
            lines.append("  HIGH RISK \u2014 prioritize cooling center access")

    # DSNY waste tonnage
    _, dsny_rows = safe_query(pool, EJ_DSNY_SQL, [f"%{boro_code}%"])
    if dsny_rows:
        total_refuse = sum(float(r[1] or 0) for r in dsny_rows)
        lines.append(f"\nWASTE BURDEN: {total_refuse:,.0f} tons refuse collected")

    # Compute burden score
    burden_score = 0
    if waste_rows:
        burden_score += min(20, len(waste_rows) * 5)
    if epa_rows:
        burden_score += min(15, len(epa_rows) * 3)
    if ej_count > 0:
        burden_score += min(15, ej_count * 3)
    if rat_rows:
        active_pct = active * 100 / total_inspections if total_inspections > 0 else 0
        if active_pct > 30:
            burden_score += 10
        elif active_pct > 15:
            burden_score += 5
    if env311_rows:
        if total_311 > 5000:
            burden_score += 15
        elif total_311 > 1000:
            burden_score += 10
        elif total_311 > 200:
            burden_score += 5
    if hvi_rows and hvi_rows[0][0]:
        burden_score += int(hvi_rows[0][0]) * 3
    if flood_rows and flood_rows[0][0]:
        try:
            burden_score += min(10, int(float(flood_rows[0][0]) * 2))
        except (ValueError, TypeError):
            pass

    if burden_score >= 50:
        risk = "SEVERE \u2014 environmental racism pattern"
    elif burden_score >= 35:
        risk = "HIGH \u2014 disproportionate environmental burden"
    elif burden_score >= 20:
        risk = "ELEVATED \u2014 above-average exposure"
    else:
        risk = "MODERATE \u2014 typical urban levels"

    lines.append(f"\nENVIRONMENTAL BURDEN SCORE: {burden_score}/100 \u2014 {risk}")

    lines.append("\nWHAT YOU CAN DO:")
    lines.append("  1. Report environmental hazards: call 311 (Air/Water/Lead/Asbestos)")
    lines.append("  2. NYC DEP complaints: nyc.gov/dep (water, sewer, noise)")
    lines.append("  3. EPA tip line: 1-888-372-7341 (illegal dumping, hazardous waste)")
    lines.append("  4. NYS DEC EJ community grants: dec.ny.gov/public/333.html")
    lines.append("  5. WE ACT for Environmental Justice: weact.org (Northern Manhattan)")
    lines.append("  6. UPROSE: uprose.org (Sunset Park, citywide EJ advocacy)")
    lines.append("  7. South Bronx Unite: southbronxunite.org")
    if burden_score >= 35:
        lines.append("  8. Contact your City Council member \u2014 demand EJ designation review")
        lines.append("  9. NYC Environmental Justice Advisory Board: nyc.gov/ej")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "zipcode": zipcode,
        "borough": borough,
        "air_quality": {r[0]: r[1] for r in aq_rows[:10]} if aq_rows else {},
        "ej_areas": {"ej_count": ej_count, "uncertain": uncertain},
        "waste_facilities": len(waste_rows) if waste_rows else 0,
        "epa_facilities": len(epa_rows) if epa_rows else 0,
        "e_designations": len(edesig_rows) if edesig_rows else 0,
        "rats": {"total": total_inspections, "active": active} if rat_rows else {},
        "trees": int(tree_rows[0][0]) if tree_rows and tree_rows[0][0] else 0,
        "env_311": total_311 if env311_rows else 0,
        "flood_vulnerability": str(flood_rows[0][0]) if flood_rows and flood_rows[0][0] else None,
        "heat_vulnerability": str(hvi_rows[0][0]) if hvi_rows and hvi_rows[0][0] else None,
        "burden_score": burden_score,
        "risk_level": risk,
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"zipcode": zipcode, "query_time_ms": elapsed},
    )


def _view_hotspot(pool, lat: float, lng: float, radius_rings: int = 3) -> ToolResult:
    """H3 hex heatmap around a point."""
    from spatial import h3_heatmap_sql

    TABLE_MAP = {
        "crime": "public_safety.nypd_complaints_ytd",
    }
    # Default to crime for hotspot view
    source = TABLE_MAP["crime"]

    sql, params = h3_heatmap_sql(
        source_table="lake.foundation.h3_index",
        filter_table=source,
        lat=lat, lng=lng,
        radius_rings=min(radius_rings, 5),
    )
    cols, raw_rows = execute(pool, sql, params)
    if not raw_rows:
        return ToolResult(
            content=f"No data found near ({lat}, {lng}). Foundation H3 index may need materialization.",
            meta={"lat": lat, "lng": lng},
        )

    rows = [dict(zip(cols, r)) for r in raw_rows]
    total = sum(r["count"] for r in rows)
    hottest = rows[0]
    text = (
        f"## Crime Hotspot Map ({len(rows)} hex cells)\n\n"
        f"**Center:** ({lat}, {lng}) | **Radius:** {radius_rings} rings (~{radius_rings * 200}m)\n"
        f"**Total events:** {total:,}\n"
        f"**Hottest cell:** {hottest['count']:,} events at ({hottest['cell_lat']:.4f}, {hottest['cell_lng']:.4f})\n\n"
        + "\n".join(
            f"| {r['h3_cell']} | {r['cell_lat']:.4f} | {r['cell_lng']:.4f} | {r['count']:,} |"
            for r in rows[:20]
        )
    )

    return ToolResult(
        content=text,
        structured_content={"cells": rows[:20], "total": total},
        meta={"lat": lat, "lng": lng, "radius_rings": radius_rings},
    )


def _view_area(pool, lat: float, lng: float, radius_m: int = 500) -> ToolResult:
    """Spatial radius query — crimes, restaurants, subway, 311, rats, trees."""
    if not (40.4 <= lat <= 41.0 and -74.3 <= lng <= -73.6):
        raise ToolError("Coordinates must be within NYC bounds (lat 40.4-41.0, lng -74.3 to -73.6)")
    if radius_m < 50 or radius_m > 2000:
        raise ToolError("Radius must be between 50 and 2000 meters")

    t0 = time.time()
    radius_deg = radius_m / 111139.0

    # Main summary query
    params = [lng, lat] + [radius_deg] * 7
    cols, rows = execute(pool, AREA_SNAPSHOT_SQL, params)

    # Subway stops
    sub_cols, sub_rows = execute(pool, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT s.name, s.line,
               ROUND(ST_Distance(s.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
        FROM lake.spatial.subway_stops s, pt
        WHERE s.geom IS NOT NULL AND ST_DWithin(s.geom, pt.geom, ?)
        ORDER BY dist_m LIMIT 5
    """, [lng, lat, radius_deg])

    # 311 breakdown
    svc_cols, svc_rows = execute(pool, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT agency, COUNT(*) AS cnt
        FROM lake.spatial.n311_complaints c, pt
        WHERE c.geom IS NOT NULL AND ST_DWithin(c.geom, pt.geom, ?)
          AND TRY_CAST(c.created_date AS DATE) >= CURRENT_DATE - INTERVAL 730 DAY
        GROUP BY agency ORDER BY cnt DESC LIMIT 8
    """, [lng, lat, radius_deg])

    # Closest restaurants
    rest_cols, rest_rows = execute(pool, """
        WITH pt AS (SELECT ST_Point(?, ?) AS geom)
        SELECT DISTINCT ON (r.camis) r.dba, r.cuisine_description, r.grade,
               ROUND(ST_Distance(r.geom, pt.geom)::NUMERIC * 111139, 0) AS dist_m
        FROM lake.spatial.restaurant_inspections r, pt
        WHERE r.geom IS NOT NULL AND ST_DWithin(r.geom, pt.geom, ?)
          AND r.grade IN ('A', 'B', 'C')
        ORDER BY r.camis, r.inspection_date DESC
    """, [lng, lat, radius_deg])
    rest_rows = sorted(rest_rows, key=lambda r: r[3])[:10]

    elapsed = round((time.time() - t0) * 1000)

    d = dict(zip(cols, rows[0])) if rows else {}

    lines = [
        f"AREA SNAPSHOT \u2014 {lat:.4f}, {lng:.4f} ({radius_m}m radius)",
        "=" * 45,
    ]

    # Safety
    total_crimes = d.get("total_crimes", 0) or 0
    felonies = d.get("felonies", 0) or 0
    misdemeanors = d.get("misdemeanors", 0) or 0
    lines.append("\nSAFETY (past year):")
    lines.append(f"  {total_crimes} crimes ({felonies} felonies, {misdemeanors} misdemeanors)")
    if d.get("top_crimes"):
        lines.append(f"  Top: {d['top_crimes']}")

    # Restaurants
    total_rest = d.get("total_restaurants", 0) or 0
    pct_a = d.get("pct_a", 0) or 0
    grade_c = d.get("grade_c_count", 0) or 0
    lines.append("\nFOOD:")
    lines.append(f"  {total_rest} restaurants ({pct_a:.0f}% grade A, {grade_c} grade C)")
    if rest_rows:
        lines.append("  Nearest:")
        for r in rest_rows[:5]:
            lines.append(f"    {r[0]} ({r[1]}) \u2014 grade {r[2]}, {r[3]:.0f}m")

    # Transit
    lines.append("\nTRANSIT:")
    if sub_rows:
        for s in sub_rows[:5]:
            lines.append(f"  {s[0]} [{s[1]}] \u2014 {s[2]:.0f}m")
    else:
        lines.append("  No subway stops within radius")

    # Environment
    rats = d.get("rat_inspections", 0) or 0
    active_rats = d.get("active_rats", 0) or 0
    trees = d.get("tree_count", 0) or 0
    healthy = d.get("healthy_trees", 0) or 0
    lines.append("\nENVIRONMENT:")
    lines.append(f"  Trees: {trees} ({healthy} healthy)")
    lines.append(f"  Rat inspections: {rats} ({active_rats} with active signs)")

    # 311 activity
    if svc_rows:
        total_311 = sum(r[1] for r in svc_rows)
        top_agencies = ", ".join(f"{r[0]}({r[1]})" for r in svc_rows[:5])
        lines.append("\n311 ACTIVITY (past 2 years):")
        lines.append(f"  {total_311} complaints \u2014 {top_agencies}")

    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)

    structured = {
        "summary": d,
        "subway_stops": [dict(zip(sub_cols, r)) for r in sub_rows] if sub_rows else [],
        "nearest_restaurants": [dict(zip(rest_cols, r)) for r in rest_rows[:10]] if rest_rows else [],
        "complaints_311": [dict(zip(svc_cols, r)) for r in svc_rows] if svc_rows else [],
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"lat": lat, "lng": lng, "radius_m": radius_m, "query_time_ms": elapsed},
    )


def _view_restaurants(pool, name: str, zipcode: str = "", borough: str = "") -> ToolResult:
    """Restaurant search by name with fuzzy matching."""
    if len(name.strip()) < 2:
        raise ToolError("Restaurant name must be at least 2 characters")

    t0 = time.time()

    # Build optional filters
    zip_filter = ""
    params = [name.strip(), name.strip()]
    if zipcode.strip():
        zip_filter += " AND zipcode = ?"
        params.append(zipcode.strip())
    if borough.strip():
        zip_filter += " AND LOWER(boro) = LOWER(?)"
        params.append(borough.strip())

    sql = RESTAURANT_FUZZY_MATCH_SQL.format(zip_filter=zip_filter)
    cols, matches = execute(pool, sql, params)

    if not matches:
        raise ToolError(
            f"No restaurant found matching '{name}'"
            + (f" in ZIP {zipcode}" if zipcode else "")
            + (f" in {borough}" if borough else "")
            + ". Try a shorter name or remove location filters."
        )

    # Take best match
    best = dict(zip(cols, matches[0]))
    camis = best["camis"]

    # Get inspection history
    insp_cols, inspections = execute(pool, RESTAURANT_INSPECTIONS_SQL, [camis])

    # Build summary
    current_grade = "Not yet graded"
    for insp in inspections:
        d = dict(zip(insp_cols, insp))
        if d.get("grade") and d["grade"] in ("A", "B", "C"):
            current_grade = d["grade"]
            break

    # Grade history timeline
    grades = []
    for insp in inspections:
        d = dict(zip(insp_cols, insp))
        if d.get("grade") and d["grade"] in ("A", "B", "C"):
            date_str = str(d["inspection_date"])[:10]
            grades.append(f"{d['grade']}({date_str})")

    # Recent violations (from most recent inspection)
    recent_violations = []
    if inspections:
        latest = dict(zip(insp_cols, inspections[0]))
        if latest.get("violations"):
            for v in latest["violations"].split(" | ")[:5]:
                recent_violations.append(v[:120])

    lines = [
        f"{best['dba']} \u2014 {best['cuisine_description']}",
        f"  {best['address']}, {best['boro']} {best['zipcode']}"
        + (f" | {best['phone']}" if best.get("phone") else ""),
        f"  Current grade: {current_grade}"
        + (f" | Score: {inspections[0][2]}" if inspections else ""),
        f"  Match confidence: {best['name_score']:.0f}%",
    ]

    if grades:
        lines.append(f"  Grade history: {' \u2192 '.join(grades[:8])}")

    if recent_violations:
        lines.append(f"  Recent violations ({str(inspections[0][0])[:10]}):")
        for v in recent_violations:
            lines.append(f"    - {v}")

    # Other matches
    if len(matches) > 1:
        lines.append("\n  Other matches:")
        for m in matches[1:5]:
            md = dict(zip(cols, m))
            lines.append(f"    {md['dba']} ({md['cuisine_description']}) \u2014 {md['boro']} {md['zipcode']} [{md['name_score']:.0f}%]")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)
    if inspections:
        text += "\n\nINSPECTION HISTORY:\n" + format_text_table(insp_cols, inspections)

    structured = {
        "restaurant": best,
        "inspections": [dict(zip(insp_cols, r)) for r in inspections],
        "other_matches": [dict(zip(cols, m)) for m in matches[1:5]],
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"camis": camis, "query_time_ms": elapsed, "match_score": best["name_score"]},
    )


# ---------------------------------------------------------------------------
# Input parsing helpers
# ---------------------------------------------------------------------------

def _parse_zips(location: str) -> list[str]:
    """Extract ZIP codes from a comma-separated location string."""
    parts = [p.strip() for p in location.split(",")]
    zips = [p for p in parts if _ZIP_PATTERN.match(p)]
    return zips


def _parse_coords(location: str) -> tuple[float, float] | None:
    """Extract lat,lng from location string. Returns (lat, lng) or None."""
    m = _COORDS_PATTERN.match(location.strip())
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if 40.4 <= lat <= 41.0 and -74.3 <= lng <= -73.6:
            return lat, lng
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def neighborhood(
    location: Annotated[str, Field(
        description="NYC ZIP code, lat/lng coordinates, or multiple ZIPs comma-separated, e.g. '10003', '40.7128,-74.0060', '10003,11201,11215'",
        examples=["10003", "11201", "40.7128,-74.0060", "10003,11201,11215"],
    )],
    view: Annotated[
        Literal["full", "compare", "gentrification", "environment", "hotspot", "area", "restaurants"],
        Field(
            default="full",
            description="'full' returns demographics + environment + complaints combined. 'compare' compares multiple ZIPs side-by-side. 'gentrification' shows quarterly displacement signals. 'environment' shows pollution burden and air quality. 'hotspot' shows H3 hex heatmap around coordinates. 'area' shows everything within a radius of coordinates. 'restaurants' searches restaurants by name in the area.",
        )
    ] = "full",
    name: Annotated[str, Field(
        description="Restaurant or resource name filter, only used with 'restaurants' view, e.g. 'Wo Hop', 'joes pizza'",
        examples=["Wo Hop", "joes pizza", "halal cart"],
    )] = "",
    radius_m: Annotated[int, Field(
        description="Search radius in meters for 'area' and 'hotspot' views, 50-2000, e.g. 500",
        ge=50, le=2000,
    )] = 500,
    ctx: Context = None,
) -> ToolResult:
    """Explore any NYC neighborhood by ZIP code or coordinates. Returns demographics, safety, climate risk, complaints, and local services. Use for any area-level question about quality of life, comparisons, gentrification, or restaurant searches. Do NOT use for specific building questions (use building) or person lookups (use entity). Default returns the full neighborhood portrait with demographics and environment."""
    pool = ctx.lifespan_context["pool"]
    location = location.strip()

    # Parse input
    coords = _parse_coords(location)
    zips = _parse_zips(location)

    # Auto-routing: coordinates → area or hotspot
    if coords and view in ("full", "area"):
        lat, lng = coords
        return _view_area(pool, lat, lng, radius_m)

    if coords and view == "hotspot":
        lat, lng = coords
        radius_rings = max(1, min(5, radius_m // 200))
        return _view_hotspot(pool, lat, lng, radius_rings)

    # Auto-routing: multiple ZIPs → compare or gentrification
    if len(zips) >= 2 and view == "full":
        return _view_compare(pool, zips)

    if view == "compare":
        if len(zips) < 2:
            raise ToolError("'compare' view requires at least 2 comma-separated ZIP codes")
        return _view_compare(pool, zips)

    if view == "gentrification":
        if not zips:
            raise ToolError("'gentrification' view requires at least 1 ZIP code")
        return _view_gentrification(pool, zips)

    # Single ZIP views
    if not zips:
        # Maybe it's coordinates for a non-area view — try to extract
        if coords:
            lat, lng = coords
            if view == "hotspot":
                radius_rings = max(1, min(5, radius_m // 200))
                return _view_hotspot(pool, lat, lng, radius_rings)
            return _view_area(pool, lat, lng, radius_m)
        raise ToolError(
            f"Could not parse '{location}' as a NYC ZIP code or coordinates. "
            "Examples: '10003', '40.7128,-74.0060', '10003,11201'"
        )

    zipcode = zips[0]

    if view == "environment":
        return _view_environment(pool, zipcode)

    if view == "restaurants":
        if not name.strip():
            raise ToolError("'restaurants' view requires a restaurant name in the 'name' parameter")
        return _view_restaurants(pool, name, zipcode=zipcode)

    if view == "hotspot":
        # Need coordinates for hotspot, try to get centroid from ZIP
        try:
            from spatial import h3_zip_centroid_sql
            _, centroid = execute(pool, *h3_zip_centroid_sql(zipcode))
            if centroid and centroid[0][0] is not None:
                lat, lng = centroid[0][1], centroid[0][2]
                radius_rings = max(1, min(5, radius_m // 200))
                return _view_hotspot(pool, lat, lng, radius_rings)
        except Exception:
            pass
        raise ToolError("'hotspot' view works best with coordinates (lat,lng). Try: '40.7128,-74.0060'")

    if view == "area":
        # Need coordinates for area, try to get centroid from ZIP
        try:
            from spatial import h3_zip_centroid_sql
            _, centroid = execute(pool, *h3_zip_centroid_sql(zipcode))
            if centroid and centroid[0][0] is not None:
                lat, lng = centroid[0][1], centroid[0][2]
                return _view_area(pool, lat, lng, radius_m)
        except Exception:
            pass
        raise ToolError("'area' view works best with coordinates (lat,lng). Try: '40.7128,-74.0060'")

    # Default: full portrait + complaints
    return _view_full(pool, zipcode)
