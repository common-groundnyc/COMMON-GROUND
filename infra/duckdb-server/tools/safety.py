"""safety() super tool — crime, crashes, shootings, use of force, hate crimes, summons.

Views:
  full      → precinct report — crimes, arrests, trends, year-over-year (absorbs safety_report)
  crashes   → motor vehicle collisions — injuries, fatalities, contributing factors
  shootings → shooting incidents — location, time, demographics
  force     → use of force incidents — subjects, officers, type of force
  hate      → hate crime incidents — bias type, offense, trends
  summons   → criminal court summons — charges, demographics
"""

import re
import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import execute, safe_query, parallel_queries
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS, ZIP_PATTERN, COORDS_PATTERN

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_PRECINCT_PATTERN = re.compile(r"^\d{1,3}$")
_ZIP_PATTERN = ZIP_PATTERN
_COORDS_PATTERN = COORDS_PATTERN

# ---------------------------------------------------------------------------
# Location resolution
# ---------------------------------------------------------------------------


def _parse_location(location: str) -> dict:
    """Parse location string into a typed dict with 'type' and value keys."""
    loc = location.strip()
    if _ZIP_PATTERN.match(loc):
        return {"type": "zip", "zip": loc}
    m = _COORDS_PATTERN.match(loc)
    if m:
        return {"type": "coords", "lat": float(m.group(1)), "lng": float(m.group(2))}
    if _PRECINCT_PATTERN.match(loc):
        pct = int(loc)
        if pct < 1 or pct > 123:
            raise ToolError("Precinct must be between 1 and 123")
        return {"type": "precinct", "precinct": str(pct)}
    raise ToolError(
        f"Cannot parse location '{loc}'. Provide a precinct number (1-123), "
        "ZIP code (5 digits), or lat,lng coordinates."
    )


def _resolve_precinct(pool, parsed: dict) -> str:
    """Resolve any location type to a precinct number string."""
    if parsed["type"] == "precinct":
        return parsed["precinct"]

    if parsed["type"] == "zip":
        _, rows = safe_query(pool, """
            SELECT REGEXP_EXTRACT(police_precinct, '\\d+') AS precinct,
                   COUNT(*) AS weight
            FROM lake.social_services.n311_service_requests
            WHERE incident_zip = ?
              AND police_precinct NOT IN ('Unspecified', '')
              AND police_precinct IS NOT NULL
            GROUP BY 1 ORDER BY weight DESC LIMIT 1
        """, [parsed["zip"]])
        if rows:
            return str(rows[0][0])
        raise ToolError(f"Could not resolve ZIP {parsed['zip']} to a precinct. Try a precinct number directly.")

    if parsed["type"] == "coords":
        lat, lng = parsed["lat"], parsed["lng"]
        _, rows = safe_query(pool, """
            SELECT CAST(policeprct AS VARCHAR) AS pct
            FROM lake.city_government.pluto
            WHERE TRY_CAST(latitude AS DOUBLE) BETWEEN ? - 0.005 AND ? + 0.005
              AND TRY_CAST(longitude AS DOUBLE) BETWEEN ? - 0.005 AND ? + 0.005
              AND policeprct IS NOT NULL
            GROUP BY policeprct
            ORDER BY COUNT(*) DESC LIMIT 1
        """, [lat, lat, lng, lng])
        if rows:
            return str(rows[0][0])
        raise ToolError(
            f"Could not resolve coordinates ({lat},{lng}) to a precinct. "
            "Try a precinct number or ZIP code."
        )

    raise ToolError("Unknown location type")


def _resolve_zip(parsed: dict) -> str | None:
    """Return the ZIP code if provided, else None."""
    if parsed["type"] == "zip":
        return parsed["zip"]
    return None


# ---------------------------------------------------------------------------
# SQL — full view (from safety_report)
# ---------------------------------------------------------------------------

CRIME_SQL = """
WITH crimes_hist AS (
    SELECT ofns_desc, law_cat_cd,
           EXTRACT(YEAR FROM TRY_CAST(rpt_dt AS DATE)) AS yr
    FROM lake.public_safety.nypd_complaints_historic
    WHERE addr_pct_cd = ?
      AND TRY_CAST(rpt_dt AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
crimes_ytd AS (
    SELECT ofns_desc, law_cat_cd,
           EXTRACT(YEAR FROM TRY_CAST(rpt_dt AS DATE)) AS yr
    FROM lake.public_safety.nypd_complaints_ytd
    WHERE addr_pct_cd = ?
),
crimes AS (SELECT * FROM crimes_hist UNION ALL SELECT * FROM crimes_ytd),
by_year AS (
    SELECT yr, COUNT(*) AS total,
           COUNT(*) FILTER (WHERE law_cat_cd = 'FELONY') AS felonies,
           COUNT(*) FILTER (WHERE law_cat_cd = 'MISDEMEANOR') AS misdemeanors,
           COUNT(*) FILTER (WHERE law_cat_cd = 'VIOLATION') AS violations
    FROM crimes WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_crimes AS (
    SELECT ofns_desc, COUNT(*) AS cnt
    FROM crimes WHERE yr = EXTRACT(YEAR FROM CURRENT_DATE) OR yr = EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY ofns_desc ORDER BY cnt DESC LIMIT 8
)
SELECT 'crime_by_year' AS section, yr, total, felonies, misdemeanors, violations, NULL AS ofns_desc
FROM by_year
UNION ALL
SELECT 'top_crimes', NULL, cnt, NULL, NULL, NULL, ofns_desc FROM top_crimes
"""

ARRESTS_SQL = """
WITH arrests_hist AS (
    SELECT ofns_desc, law_cat_cd, perp_race, perp_sex, age_group,
           EXTRACT(YEAR FROM TRY_CAST(arrest_date AS DATE)) AS yr
    FROM lake.public_safety.nypd_arrests_historic
    WHERE arrest_precinct = ?
      AND TRY_CAST(arrest_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
arrests_ytd AS (
    SELECT ofns_desc, law_cat_cd, perp_race, perp_sex, age_group,
           EXTRACT(YEAR FROM TRY_CAST(arrest_date AS DATE)) AS yr
    FROM lake.public_safety.nypd_arrests_ytd
    WHERE arrest_precinct = ?
),
arrests AS (SELECT * FROM arrests_hist UNION ALL SELECT * FROM arrests_ytd),
by_year AS (
    SELECT yr, COUNT(*) AS total,
           COUNT(*) FILTER (WHERE law_cat_cd = 'F') AS felonies,
           COUNT(*) FILTER (WHERE law_cat_cd = 'M') AS misdemeanors
    FROM arrests WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_charges AS (
    SELECT ofns_desc, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY ofns_desc ORDER BY cnt DESC LIMIT 8
),
demographics AS (
    SELECT perp_race, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND perp_race IS NOT NULL AND perp_race NOT IN ('UNKNOWN', 'OTHER', '(null)')
    GROUP BY perp_race ORDER BY cnt DESC
),
sex_breakdown AS (
    SELECT perp_sex, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND perp_sex IS NOT NULL
    GROUP BY perp_sex ORDER BY cnt DESC
),
age_breakdown AS (
    SELECT age_group, COUNT(*) AS cnt
    FROM arrests WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND age_group IS NOT NULL
    GROUP BY age_group ORDER BY cnt DESC
)
SELECT 'arrest_by_year' AS section, yr::VARCHAR AS key1, total::VARCHAR AS val1,
       felonies::VARCHAR AS val2, misdemeanors::VARCHAR AS val3
FROM by_year
UNION ALL SELECT 'top_charges', ofns_desc, cnt::VARCHAR, NULL, NULL FROM top_charges
UNION ALL SELECT 'race', perp_race, cnt::VARCHAR, NULL, NULL FROM demographics
UNION ALL SELECT 'sex', perp_sex, cnt::VARCHAR, NULL, NULL FROM sex_breakdown
UNION ALL SELECT 'age', age_group, cnt::VARCHAR, NULL, NULL FROM age_breakdown
"""

SHOOTINGS_YEARLY_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(occur_date AS DATE)) AS yr,
       COUNT(*) AS shootings
FROM lake.public_safety.shootings
WHERE precinct = ?
  AND TRY_CAST(occur_date AS DATE) >= CURRENT_DATE - INTERVAL 5 YEAR
GROUP BY yr ORDER BY yr
"""

SUMMONS_SQL = """
WITH summons AS (
    SELECT offense_description,
           EXTRACT(YEAR FROM TRY_CAST(summons_date AS DATE)) AS yr
    FROM lake.public_safety.criminal_court_summons
    WHERE precinct_of_occur = ?
      AND TRY_CAST(summons_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
),
by_year AS (
    SELECT yr, COUNT(*) AS total FROM summons WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr
),
top_types AS (
    SELECT offense_description, COUNT(*) AS cnt
    FROM summons WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY offense_description ORDER BY cnt DESC LIMIT 8
)
SELECT 'summons_by_year' AS section, yr::VARCHAR AS key1, total::VARCHAR AS val1
FROM by_year
UNION ALL SELECT 'top_summons', offense_description, cnt::VARCHAR FROM top_types
"""

HATE_SQL = """
SELECT bias_motive_description, offense_category, COUNT(*) AS cnt
FROM lake.public_safety.hate_crimes
WHERE complaint_precinct_code = ?
  AND TRY_CAST(complaint_year_number AS INT) >= EXTRACT(YEAR FROM CURRENT_DATE) - 2
GROUP BY bias_motive_description, offense_category
ORDER BY cnt DESC LIMIT 10
"""

CITY_AVERAGES_SQL = """
WITH arrest_totals AS (
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
    AVG(a.arrests) AS avg_arrests,
    AVG(c.crimes) AS avg_crimes,
    AVG(CASE WHEN c.crimes > 0 THEN a.arrests * 1000.0 / c.crimes END) AS avg_arrest_rate_per_1k
FROM arrest_totals a
JOIN crime_totals c ON a.pct = c.pct
"""

CCRB_SQL = """
WITH complaints AS (
    SELECT complaint_id, ccrb_complaint_disposition, reason_for_police_contact,
           EXTRACT(YEAR FROM TRY_CAST(incident_date AS DATE)) AS yr
    FROM lake.public_safety.ccrb_complaints
    WHERE precinct_of_incident_occurrence = ?
),
by_year AS (
    SELECT yr, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr IS NOT NULL GROUP BY yr ORDER BY yr DESC LIMIT 5
),
by_disposition AS (
    SELECT ccrb_complaint_disposition AS disposition, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
    GROUP BY ccrb_complaint_disposition ORDER BY cnt DESC
),
by_reason AS (
    SELECT reason_for_police_contact AS reason, COUNT(DISTINCT complaint_id) AS cnt
    FROM complaints WHERE yr >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
    GROUP BY reason_for_police_contact ORDER BY cnt DESC LIMIT 5
)
SELECT 'ccrb_by_year' AS section, yr::VARCHAR AS k1, cnt::VARCHAR AS k2, NULL AS k3
FROM by_year
UNION ALL SELECT 'ccrb_disposition', disposition, cnt::VARCHAR, NULL FROM by_disposition
UNION ALL SELECT 'ccrb_reason', reason, cnt::VARCHAR, NULL FROM by_reason
"""

CCRB_ALLEGATIONS_SQL = """
SELECT fado_type, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Substantiated') AS substantiated,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Exonerated') AS exonerated,
       COUNT(*) FILTER (WHERE ccrb_allegation_disposition = 'Unsubstantiated') AS unsubstantiated
FROM lake.public_safety.ccrb_allegations a
JOIN lake.public_safety.ccrb_complaints c
  ON a.complaint_id = c.complaint_id
WHERE c.precinct_of_incident_occurrence = ?
GROUP BY fado_type ORDER BY total DESC
"""

CCRB_VICTIM_RACE_SQL = """
SELECT COALESCE(a.victim_allegedvictim_race_legacy, 'Unknown') AS race, COUNT(*) AS cnt
FROM lake.public_safety.ccrb_allegations a
JOIN lake.public_safety.ccrb_complaints c
  ON a.complaint_id = c.complaint_id
WHERE c.precinct_of_incident_occurrence = ?
  AND a.victim_allegedvictim_race_legacy IS NOT NULL
  AND a.victim_allegedvictim_race_legacy NOT IN ('Unknown', 'Refused', '')
GROUP BY race ORDER BY cnt DESC
"""

UOF_SQL = """
SELECT forcetype, COUNT(*) AS cnt,
       COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM TRY_CAST(occurrence_date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)) AS this_year,
       COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM TRY_CAST(occurrence_date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE) - 1) AS last_year
FROM lake.public_safety.use_of_force_incidents
WHERE incident_pct = ?
GROUP BY forcetype ORDER BY cnt DESC
"""

UOF_SUBJECT_SQL = """
SELECT s.subject_race, COUNT(*) AS cnt,
       COUNT(*) FILTER (WHERE s.subject_injured = 'Y') AS injured,
       COUNT(*) FILTER (WHERE s.subject_injury_level NOT IN ('No Injury', 'N')) AS serious_injury
FROM lake.public_safety.use_of_force_subjects s
JOIN lake.public_safety.use_of_force_incidents i
  ON s.tri_incident_number = i.tri_incident_number
WHERE i.incident_pct = ?
  AND s.subject_race IS NOT NULL AND s.subject_race != ''
GROUP BY s.subject_race ORDER BY cnt DESC
"""

UOF_CITY_AVG_SQL = """
SELECT AVG(pct_cnt) AS avg_uof_per_pct
FROM (
    SELECT incident_pct, COUNT(*) AS pct_cnt
    FROM lake.public_safety.use_of_force_incidents
    WHERE incident_pct IS NOT NULL
    GROUP BY incident_pct
)
"""

# ---------------------------------------------------------------------------
# SQL — crashes view
# ---------------------------------------------------------------------------

CRASHES_SQL = """
SELECT crash_date, crash_time, borough, zip_code, on_street_name,
       number_of_persons_injured, number_of_persons_killed,
       number_of_pedestrians_injured, number_of_pedestrians_killed,
       number_of_cyclist_injured, number_of_cyclist_killed,
       contributing_factor_vehicle_1, vehicle_type_code1
FROM lake.public_safety.motor_vehicle_collisions
WHERE ({location_filter})
ORDER BY TRY_CAST(crash_date AS DATE) DESC
LIMIT 20
"""

CRASHES_SUMMARY_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(crash_date AS DATE)) AS yr,
       COUNT(*) AS total_crashes,
       SUM(TRY_CAST(number_of_persons_injured AS INT)) AS total_injured,
       SUM(TRY_CAST(number_of_persons_killed AS INT)) AS total_killed,
       SUM(TRY_CAST(number_of_pedestrians_injured AS INT)) AS ped_injured,
       SUM(TRY_CAST(number_of_pedestrians_killed AS INT)) AS ped_killed,
       SUM(TRY_CAST(number_of_cyclist_injured AS INT)) AS cyclist_injured,
       SUM(TRY_CAST(number_of_cyclist_killed AS INT)) AS cyclist_killed
FROM lake.public_safety.motor_vehicle_collisions
WHERE ({location_filter})
  AND TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
GROUP BY yr ORDER BY yr
"""

CRASHES_TOP_FACTORS_SQL = """
SELECT contributing_factor_vehicle_1 AS factor, COUNT(*) AS cnt
FROM lake.public_safety.motor_vehicle_collisions
WHERE ({location_filter})
  AND TRY_CAST(crash_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
  AND contributing_factor_vehicle_1 IS NOT NULL
  AND contributing_factor_vehicle_1 != 'Unspecified'
GROUP BY contributing_factor_vehicle_1
ORDER BY cnt DESC LIMIT 8
"""

# ---------------------------------------------------------------------------
# SQL — shootings view (detailed)
# ---------------------------------------------------------------------------

SHOOTINGS_DETAIL_SQL = """
SELECT occur_date, occur_time, boro, precinct,
       vic_age_group, vic_sex, vic_race,
       perp_age_group, perp_sex, perp_race,
       statistical_murder_flag
FROM lake.public_safety.shootings
WHERE precinct = ?
ORDER BY TRY_CAST(occur_date AS DATE) DESC
LIMIT 20
"""

SHOOTINGS_DEMOGRAPHICS_SQL = """
SELECT vic_race, COUNT(*) AS cnt,
       COUNT(*) FILTER (WHERE statistical_murder_flag = 'true') AS fatal
FROM lake.public_safety.shootings
WHERE precinct = ?
  AND TRY_CAST(occur_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
  AND vic_race IS NOT NULL AND vic_race != ''
GROUP BY vic_race ORDER BY cnt DESC
"""

SHOOTINGS_BY_TIME_SQL = """
SELECT EXTRACT(HOUR FROM TRY_CAST(occur_time AS TIME)) AS hour_of_day,
       COUNT(*) AS cnt
FROM lake.public_safety.shootings
WHERE precinct = ?
  AND TRY_CAST(occur_date AS DATE) >= CURRENT_DATE - INTERVAL 3 YEAR
GROUP BY hour_of_day ORDER BY hour_of_day
"""

# ---------------------------------------------------------------------------
# SQL — force view (detailed)
# ---------------------------------------------------------------------------

UOF_OFFICER_SQL = """
SELECT o.officer_rank, o.officer_in_uniform, COUNT(*) AS cnt
FROM lake.public_safety.use_of_force_officers o
JOIN lake.public_safety.use_of_force_incidents i
  ON o.tri_incident_number = i.tri_incident_number
WHERE i.incident_pct = ?
GROUP BY o.officer_rank, o.officer_in_uniform
ORDER BY cnt DESC LIMIT 10
"""

UOF_YEARLY_SQL = """
SELECT EXTRACT(YEAR FROM TRY_CAST(occurrence_date AS DATE)) AS yr,
       COUNT(*) AS cnt
FROM lake.public_safety.use_of_force_incidents
WHERE incident_pct = ?
  AND TRY_CAST(occurrence_date AS DATE) >= CURRENT_DATE - INTERVAL 5 YEAR
GROUP BY yr ORDER BY yr
"""

# ---------------------------------------------------------------------------
# SQL — summons view (detailed)
# ---------------------------------------------------------------------------

SUMMONS_DETAIL_SQL = """
SELECT summons_date, offense_description, law_category,
       borough, description_of_violation
FROM lake.public_safety.criminal_court_summons
WHERE precinct_of_occur = ?
ORDER BY TRY_CAST(summons_date AS DATE) DESC
LIMIT 20
"""

SUMMONS_DEMOGRAPHICS_SQL = """
SELECT offender_race_descent, COUNT(*) AS cnt
FROM lake.public_safety.criminal_court_summons
WHERE precinct_of_occur = ?
  AND TRY_CAST(summons_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR
  AND offender_race_descent IS NOT NULL
  AND offender_race_descent NOT IN ('', 'UNKNOWN')
GROUP BY offender_race_descent ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# SQL — hate view (detailed)
# ---------------------------------------------------------------------------

HATE_DETAIL_SQL = """
SELECT record_create_date, offense_description,
       bias_motive_description, offense_category,
       pd_desc, patrol_borough_name
FROM lake.public_safety.hate_crimes
WHERE complaint_precinct_code = ?
ORDER BY TRY_CAST(record_create_date AS DATE) DESC
LIMIT 20
"""

HATE_YEARLY_SQL = """
SELECT TRY_CAST(complaint_year_number AS INT) AS yr, COUNT(*) AS cnt
FROM lake.public_safety.hate_crimes
WHERE complaint_precinct_code = ?
GROUP BY yr ORDER BY yr DESC LIMIT 5
"""


# ---------------------------------------------------------------------------
# View builders
# ---------------------------------------------------------------------------


def _build_crash_filter(parsed: dict) -> tuple[str, list]:
    """Build WHERE clause fragment + params for crash queries."""
    if parsed["type"] == "zip":
        return "zip_code = ?", [parsed["zip"]]
    if parsed["type"] == "precinct":
        # Crashes don't have a precinct column — use ZIP crosswalk not available,
        # so we filter by contributing_factor... Actually crashes don't have precinct.
        # We'll note this limitation and return empty.
        return "1=0", []
    if parsed["type"] == "coords":
        lat, lng = parsed["lat"], parsed["lng"]
        return (
            "TRY_CAST(latitude AS DOUBLE) BETWEEN ? AND ? "
            "AND TRY_CAST(longitude AS DOUBLE) BETWEEN ? AND ?",
            [lat - 0.01, lat + 0.01, lng - 0.01, lng + 0.01],
        )
    return "1=0", []


def _view_full(pool, pct: str, parsed: dict) -> ToolResult:
    """Full precinct safety report — mirrors the original safety_report."""
    t0 = time.time()

    # Try materialized view first
    mv_crime_ok = False
    try:
        _mv_cols, _mv_rows = execute(
            pool,
            "SELECT * FROM lake.foundation.mv_crime_precinct WHERE precinct = ?::VARCHAR",
            [pct],
        )
        if _mv_rows:
            mv = dict(zip(_mv_cols, _mv_rows[0]))
            crime_rows = [
                ("crime_by_year", mv.get("yr1"), mv.get("yr1_total"), mv.get("yr1_felonies"),
                 mv.get("yr1_misdemeanors"), mv.get("yr1_violations"), None),
                ("crime_by_year", mv.get("yr2"), mv.get("yr2_total"), mv.get("yr2_felonies"),
                 mv.get("yr2_misdemeanors"), mv.get("yr2_violations"), None),
                ("crime_by_year", mv.get("yr3"), mv.get("yr3_total"), mv.get("yr3_felonies"),
                 mv.get("yr3_misdemeanors"), mv.get("yr3_violations"), None),
            ]
            for i in range(1, 9):
                offense = mv.get(f"top_offense_{i}")
                cnt = mv.get(f"top_offense_{i}_cnt")
                if offense and cnt:
                    crime_rows.append(("top_crimes", None, cnt, None, None, None, offense))
            mv_crime_ok = True
    except Exception:
        pass

    if not mv_crime_ok:
        _, crime_rows = execute(pool, CRIME_SQL, [pct, pct])

    _pq = parallel_queries(pool, [
        ("arrests", ARRESTS_SQL, [pct, pct]),
        ("shootings", SHOOTINGS_YEARLY_SQL, [pct]),
        ("summons", SUMMONS_SQL, [pct]),
        ("hate", HATE_SQL, [pct]),
        ("city_avg", CITY_AVERAGES_SQL, []),
        ("ccrb", CCRB_SQL, [pct]),
        ("ccrb_fado", CCRB_ALLEGATIONS_SQL, [pct]),
        ("ccrb_victim_race", CCRB_VICTIM_RACE_SQL, [pct]),
        ("uof", UOF_SQL, [pct]),
        ("uof_subj", UOF_SUBJECT_SQL, [pct]),
        ("uof_avg", UOF_CITY_AVG_SQL, []),
    ])
    _, arrest_rows = _pq.get("arrests", ([], []))
    _, shoot_rows = _pq.get("shootings", ([], []))
    _, summ_rows = _pq.get("summons", ([], []))
    _, hate_rows = _pq.get("hate", ([], []))
    _, avg_rows = _pq.get("city_avg", ([], []))
    _, ccrb_rows = _pq.get("ccrb", ([], []))
    _, fado_rows = _pq.get("ccrb_fado", ([], []))
    _, victim_race_rows = _pq.get("ccrb_victim_race", ([], []))
    _, uof_rows = _pq.get("uof", ([], []))
    _, uof_subj_rows = _pq.get("uof_subj", ([], []))
    _, uof_avg_rows = _pq.get("uof_avg", ([], []))

    # Parse crime data
    crime_years = [
        (r[1], int(r[2] or 0), int(r[3] or 0), int(r[4] or 0), int(r[5] or 0))
        for r in crime_rows if r[0] == "crime_by_year"
    ]
    top_crimes = [(r[6], int(r[2] or 0)) for r in crime_rows if r[0] == "top_crimes"]

    # Parse arrest data
    arrest_years = [
        (r[1], int(r[2]) if r[2] else 0, int(r[3]) if r[3] else 0, int(r[4]) if r[4] else 0)
        for r in arrest_rows if r[0] == "arrest_by_year"
    ]
    top_charges = [(r[1], int(r[2]) if r[2] else 0) for r in arrest_rows if r[0] == "top_charges"]
    race_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "race"]
    sex_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "sex"]
    age_data = [(r[1], int(r[2])) for r in arrest_rows if r[0] == "age"]

    lines = [
        f"SAFETY REPORT — Precinct {pct}",
        "=" * 45,
    ]

    # Crime overview
    lines.append("\nCRIME (3-year trend):")
    for yr, total, fel, misd, _viol in crime_years:
        yr_int = int(yr) if yr else 0
        total = int(total) if total else 0
        fel = int(fel) if fel else 0
        misd = int(misd) if misd else 0
        lines.append(f"  {yr_int}: {total:,} total ({fel:,} felony, {misd:,} misdemeanor)")
    if top_crimes:
        lines.append("  Top offenses (recent):")
        for desc, cnt in top_crimes[:6]:
            lines.append(f"    {desc}: {cnt:,}")

    # Shootings
    lines.append("\nGUN VIOLENCE (5-year):")
    if shoot_rows:
        for r in shoot_rows:
            lines.append(f"  {int(r[0])}: {r[1]} shootings")
    else:
        lines.append("  No shootings recorded")

    # Arrests
    lines.append("\nARRESTS (3-year trend):")
    for yr, total, fel, misd in arrest_years:
        lines.append(f"  {yr}: {total:,} total ({fel:,} felony, {misd:,} misdemeanor)")
    if top_charges:
        lines.append("  Top charges (recent):")
        for desc, cnt in top_charges[:6]:
            lines.append(f"    {desc}: {cnt:,}")

    # Enforcement intensity
    lines.append("\nENFORCEMENT INTENSITY:")
    if crime_years and arrest_years:
        last_crime_yr = crime_years[-2] if len(crime_years) > 1 else crime_years[-1]
        last_arrest_yr = arrest_years[-2] if len(arrest_years) > 1 else arrest_years[-1]
        if last_crime_yr[1] and last_crime_yr[1] > 0:
            total_arrests = int(last_arrest_yr[1])
            total_crimes = int(last_crime_yr[1])
            ratio = total_arrests * 1000.0 / total_crimes
            lines.append(f"  Arrests per 1,000 crimes: {ratio:.0f}")
            if avg_rows and avg_rows[0][2]:
                city_avg = float(avg_rows[0][2])
                pct_diff = ((ratio - city_avg) / city_avg) * 100
                qualifier = "above" if pct_diff > 0 else "below"
                lines.append(f"  City average: {city_avg:.0f} per 1,000 ({abs(pct_diff):.0f}% {qualifier})")
                if pct_diff > 30:
                    lines.append("  !! HIGH enforcement intensity — significantly above city average")
                elif pct_diff < -30:
                    lines.append("  LOW enforcement intensity — significantly below city average")

    # Summons
    summ_years = [(r[1], r[2]) for r in summ_rows if r[0] == "summons_by_year"]
    top_summons = [(r[1], r[2]) for r in summ_rows if r[0] == "top_summons"]
    if summ_years:
        lines.append("\nSUMMONS (3-year trend):")
        for yr, total in summ_years:
            lines.append(f"  {yr}: {total}")
        if top_summons:
            lines.append("  Top types:")
            for desc, cnt in top_summons[:5]:
                lines.append(f"    {desc}: {cnt}")

    # Arrest demographics
    if race_data:
        total_race = sum(cnt for _, cnt in race_data)
        lines.append("\nARREST DEMOGRAPHICS (recent):")
        lines.append("  Race:")
        for race, cnt in race_data:
            pct_val = cnt * 100.0 / total_race
            lines.append(f"    {race}: {cnt:,} ({pct_val:.1f}%)")
    if sex_data:
        total_sex = sum(cnt for _, cnt in sex_data)
        lines.append("  Sex:")
        for sex, cnt in sex_data:
            label = {"M": "Male", "F": "Female"}.get(sex, sex)
            lines.append(f"    {label}: {cnt:,} ({cnt * 100.0 / total_sex:.1f}%)")
    if age_data:
        lines.append("  Age:")
        for age, cnt in age_data:
            lines.append(f"    {age}: {cnt:,}")

    # Hate crimes
    if hate_rows:
        lines.append("\nHATE CRIMES (recent):")
        for r in hate_rows:
            lines.append(f"  {r[0]} — {r[1]} ({r[2]} incidents)")
    else:
        lines.append("\nHATE CRIMES: None reported in this precinct recently")

    # CCRB misconduct
    ccrb_years = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_by_year"]
    ccrb_dispositions = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_disposition"]
    ccrb_reasons = [(r[1], int(r[2])) for r in ccrb_rows if r[0] == "ccrb_reason"]

    if ccrb_years:
        lines.append("\nCCRB MISCONDUCT COMPLAINTS:")
        for yr, cnt in ccrb_years:
            lines.append(f"  {yr}: {cnt} complaints")
        if fado_rows:
            lines.append("  By type (FADO):")
            for r in fado_rows:
                sub_rate = f"{r[2] * 100 / r[1]:.0f}%" if r[1] > 0 else "0%"
                lines.append(f"    {r[0]}: {r[1]} allegations ({sub_rate} substantiated)")
        if ccrb_dispositions:
            lines.append("  Outcomes:")
            for disp, cnt in ccrb_dispositions[:5]:
                lines.append(f"    {disp}: {cnt}")
        if victim_race_rows:
            total_victims = sum(r[1] for r in victim_race_rows)
            lines.append("  Complainant race:")
            for race, cnt in victim_race_rows:
                lines.append(f"    {race}: {cnt} ({cnt * 100 / total_victims:.1f}%)")
        if ccrb_reasons:
            lines.append("  Reasons for contact:")
            for reason, cnt in ccrb_reasons:
                lines.append(f"    {reason}: {cnt}")

    # Use of Force
    if uof_rows:
        total_uof = sum(int(r[1]) for r in uof_rows)
        lines.append(f"\nUSE OF FORCE: {total_uof:,} incidents")
        city_avg_uof = float(uof_avg_rows[0][0]) if uof_avg_rows and uof_avg_rows[0][0] else 0
        if city_avg_uof > 0:
            uof_ratio = total_uof / city_avg_uof
            lines.append(f"  {uof_ratio:.1f}x city precinct average ({city_avg_uof:.0f})")
        for r in uof_rows:
            lines.append(f"  {r[0]}: {r[1]:,} total ({r[2]} this year, {r[3]} last year)")
        if uof_subj_rows:
            total_subj = sum(int(r[1]) for r in uof_subj_rows)
            total_injured = sum(int(r[2]) for r in uof_subj_rows)
            lines.append(f"  Subject demographics ({total_subj} subjects, {total_injured} injured):")
            for r in uof_subj_rows:
                subj_pct = int(r[1]) * 100 / total_subj if total_subj > 0 else 0
                lines.append(f"    {r[0]}: {r[1]} ({subj_pct:.1f}%)")

    # Know your rights
    lines.append("\nKNOW YOUR RIGHTS:")
    lines.append("  File CCRB complaint: ccrb.nyc.gov or 800-341-2272")
    lines.append("  You have the right to record police encounters")
    lines.append("  Request officer name, badge number, and command")
    lines.append("  Legal aid (misconduct): 212-577-3300 (Legal Aid Society)")
    lines.append("  Cop Accountability Project: communityresource.io")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    text = "\n".join(lines)
    precinct_int = int(pct)

    structured = {
        "precinct": precinct_int,
        "crime_by_year": [
            {"year": int(r[0]) if r[0] else None, "total": r[1], "felonies": r[2], "misdemeanors": r[3]}
            for r in crime_years
        ],
        "top_crimes": [{"offense": r[0], "count": r[1]} for r in top_crimes],
        "shootings_by_year": [{"year": int(r[0]), "count": r[1]} for r in shoot_rows] if shoot_rows else [],
        "arrests_by_year": [
            {"year": r[0], "total": r[1], "felonies": r[2], "misdemeanors": r[3]}
            for r in arrest_years
        ],
        "top_charges": [{"charge": r[0], "count": r[1]} for r in top_charges],
        "arrest_demographics": {
            "race": [{"group": r[0], "count": r[1]} for r in race_data],
            "sex": [{"group": r[0], "count": r[1]} for r in sex_data],
            "age": [{"group": r[0], "count": r[1]} for r in age_data],
        },
        "summons_by_year": [{"year": r[0], "total": r[1]} for r in summ_years],
        "hate_crimes": [{"bias": r[0], "category": r[1], "count": r[2]} for r in hate_rows] if hate_rows else [],
        "ccrb": {
            "by_year": [{"year": r[0], "count": r[1]} for r in ccrb_years],
            "fado": [{"type": r[0], "total": r[1], "substantiated": r[2]} for r in (fado_rows or [])],
            "victim_race": [{"race": r[0], "count": r[1]} for r in (victim_race_rows or [])],
            "dispositions": [{"outcome": r[0], "count": r[1]} for r in ccrb_dispositions],
        },
        "use_of_force": {
            "total": sum(int(r[1]) for r in uof_rows) if uof_rows else 0,
            "by_type": [{"type": r[0], "count": r[1]} for r in (uof_rows or [])],
            "subject_race": [{"race": r[0], "count": r[1], "injured": r[2]} for r in (uof_subj_rows or [])],
        },
    }

    return ToolResult(
        content=text,
        structured_content=structured,
        meta={"precinct": precinct_int, "query_time_ms": elapsed},
    )


def _view_crashes(pool, parsed: dict) -> ToolResult:
    """Motor vehicle collisions — injuries, fatalities, contributing factors."""
    t0 = time.time()
    filt, params = _build_crash_filter(parsed)

    if filt == "1=0":
        raise ToolError(
            "Crash data requires a ZIP code or coordinates. "
            "Precinct filtering is not available for collisions. "
            "Try a ZIP code like '10003' or coordinates like '40.71,-74.00'."
        )

    _, recent = safe_query(pool, CRASHES_SQL.format(location_filter=filt), params)
    _, summary = safe_query(pool, CRASHES_SUMMARY_SQL.format(location_filter=filt), params)
    _, factors = safe_query(pool, CRASHES_TOP_FACTORS_SQL.format(location_filter=filt), params)

    loc_label = parsed.get("zip") or f"({parsed.get('lat'):.4f}, {parsed.get('lng'):.4f})"
    lines = [
        f"MOTOR VEHICLE COLLISIONS — {loc_label}",
        "=" * 45,
    ]

    if summary:
        lines.append("\nYEARLY SUMMARY:")
        for r in summary:
            yr = int(r[0]) if r[0] else "?"
            lines.append(
                f"  {yr}: {int(r[1] or 0):,} crashes, "
                f"{int(r[2] or 0):,} injured, {int(r[3] or 0):,} killed "
                f"({int(r[4] or 0)} ped injured, {int(r[6] or 0)} cyclist injured)"
            )

    if factors:
        lines.append("\nTOP CONTRIBUTING FACTORS (past year):")
        for r in factors:
            lines.append(f"  {r[0]}: {int(r[1]):,}")

    if recent:
        lines.append(f"\nRECENT COLLISIONS ({len(recent)} most recent):")
        for r in recent:
            date_str = str(r[0])[:10] if r[0] else "?"
            street = r[4] or "unknown street"
            injured = int(r[5] or 0)
            killed = int(r[6] or 0)
            factor = r[11] or ""
            severity = f"{injured} injured, {killed} killed" if killed else f"{injured} injured"
            lines.append(f"  {date_str} | {street} | {severity} | {factor}")
    else:
        lines.append("\nNo collision records found for this location.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "location": loc_label,
        "yearly_summary": [
            {"year": int(r[0]) if r[0] else None, "crashes": int(r[1] or 0),
             "injured": int(r[2] or 0), "killed": int(r[3] or 0)}
            for r in (summary or [])
        ],
        "top_factors": [{"factor": r[0], "count": int(r[1])} for r in (factors or [])],
        "recent_count": len(recent) if recent else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"query_time_ms": elapsed},
    )


def _view_shootings(pool, pct: str) -> ToolResult:
    """Shooting incidents — location, time, demographics."""
    t0 = time.time()

    _, yearly = execute(pool, SHOOTINGS_YEARLY_SQL, [pct])
    _, detail = safe_query(pool, SHOOTINGS_DETAIL_SQL, [pct])
    _, demographics = safe_query(pool, SHOOTINGS_DEMOGRAPHICS_SQL, [pct])
    _, by_time = safe_query(pool, SHOOTINGS_BY_TIME_SQL, [pct])

    lines = [
        f"SHOOTING INCIDENTS — Precinct {pct}",
        "=" * 45,
    ]

    if yearly:
        lines.append("\nYEARLY TREND (5-year):")
        for r in yearly:
            lines.append(f"  {int(r[0])}: {int(r[1])} shootings")
    else:
        lines.append("\n  No shootings recorded in this precinct")

    if demographics:
        lines.append("\nVICTIM DEMOGRAPHICS (3-year):")
        total_vic = sum(int(r[1]) for r in demographics)
        for r in demographics:
            cnt = int(r[1])
            fatal = int(r[2])
            pct_val = cnt * 100.0 / total_vic if total_vic > 0 else 0
            lines.append(f"  {r[0]}: {cnt} victims ({pct_val:.1f}%), {fatal} fatal")

    if by_time:
        lines.append("\nBY HOUR OF DAY (3-year):")
        peak_hours = sorted(by_time, key=lambda r: int(r[1]), reverse=True)[:5]
        for r in peak_hours:
            hour = int(r[0])
            label = f"{hour:02d}:00-{hour:02d}:59"
            lines.append(f"  {label}: {int(r[1])} incidents")

    if detail:
        lines.append(f"\nRECENT INCIDENTS ({len(detail)} most recent):")
        for r in detail:
            date_str = str(r[0])[:10] if r[0] else "?"
            time_str = str(r[1])[:5] if r[1] else ""
            boro = r[2] or ""
            murder = "FATAL" if str(r[10]).lower() == "true" else ""
            vic_info = f"victim: {r[6] or '?'} {r[5] or '?'} {r[4] or '?'}"
            parts = [f"{date_str} {time_str}", boro, vic_info]
            if murder:
                parts.append(murder)
            lines.append(f"  {' | '.join(p for p in parts if p)}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "precinct": int(pct),
        "yearly": [{"year": int(r[0]), "count": int(r[1])} for r in (yearly or [])],
        "victim_demographics": [
            {"race": r[0], "count": int(r[1]), "fatal": int(r[2])}
            for r in (demographics or [])
        ],
        "recent_count": len(detail) if detail else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"precinct": int(pct), "query_time_ms": elapsed},
    )


def _view_force(pool, pct: str) -> ToolResult:
    """Use of force incidents — subjects, officers, type of force."""
    t0 = time.time()

    _, uof_rows = safe_query(pool, UOF_SQL, [pct])
    _, subj_rows = safe_query(pool, UOF_SUBJECT_SQL, [pct])
    _, avg_rows = safe_query(pool, UOF_CITY_AVG_SQL, [])
    _, yearly = safe_query(pool, UOF_YEARLY_SQL, [pct])
    _, officer_rows = safe_query(pool, UOF_OFFICER_SQL, [pct])

    lines = [
        f"USE OF FORCE — Precinct {pct}",
        "=" * 45,
    ]

    if yearly:
        lines.append("\nYEARLY TREND:")
        for r in yearly:
            lines.append(f"  {int(r[0])}: {int(r[1])} incidents")

    if uof_rows:
        total_uof = sum(int(r[1]) for r in uof_rows)
        lines.append(f"\nBY FORCE TYPE ({total_uof:,} total):")
        city_avg_uof = float(avg_rows[0][0]) if avg_rows and avg_rows[0][0] else 0
        if city_avg_uof > 0:
            uof_ratio = total_uof / city_avg_uof
            lines.append(f"  {uof_ratio:.1f}x city precinct average ({city_avg_uof:.0f})")
        for r in uof_rows:
            lines.append(f"  {r[0]}: {r[1]:,} total ({r[2]} this year, {r[3]} last year)")
    else:
        lines.append("\n  No use of force incidents recorded")

    if subj_rows:
        total_subj = sum(int(r[1]) for r in subj_rows)
        total_injured = sum(int(r[2]) for r in subj_rows)
        lines.append(f"\nSUBJECT DEMOGRAPHICS ({total_subj} subjects, {total_injured} injured):")
        for r in subj_rows:
            subj_pct = int(r[1]) * 100 / total_subj if total_subj > 0 else 0
            injured = int(r[2])
            lines.append(f"  {r[0]}: {r[1]} ({subj_pct:.1f}%), {injured} injured")

    if officer_rows:
        lines.append("\nOFFICER BREAKDOWN:")
        for r in officer_rows:
            rank = r[0] or "Unknown"
            uniform = "in uniform" if str(r[1]).upper() in ("Y", "YES", "TRUE") else "plainclothes"
            lines.append(f"  {rank} ({uniform}): {int(r[2])}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "precinct": int(pct),
        "total": sum(int(r[1]) for r in uof_rows) if uof_rows else 0,
        "by_type": [{"type": r[0], "count": int(r[1])} for r in (uof_rows or [])],
        "subject_race": [
            {"race": r[0], "count": int(r[1]), "injured": int(r[2])}
            for r in (subj_rows or [])
        ],
        "yearly": [{"year": int(r[0]), "count": int(r[1])} for r in (yearly or [])],
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"precinct": int(pct), "query_time_ms": elapsed},
    )


def _view_hate(pool, pct: str) -> ToolResult:
    """Hate crime incidents — bias type, offense, trends."""
    t0 = time.time()

    _, summary = safe_query(pool, HATE_SQL, [pct])
    _, yearly = safe_query(pool, HATE_YEARLY_SQL, [pct])
    _, detail = safe_query(pool, HATE_DETAIL_SQL, [pct])

    lines = [
        f"HATE CRIMES — Precinct {pct}",
        "=" * 45,
    ]

    if yearly:
        lines.append("\nYEARLY TREND:")
        for r in yearly:
            lines.append(f"  {r[0]}: {int(r[1])} incidents")

    if summary:
        lines.append("\nBY BIAS MOTIVE (recent):")
        for r in summary:
            lines.append(f"  {r[0]} — {r[1]}: {int(r[2])} incidents")
    else:
        lines.append("\n  No hate crimes reported in this precinct recently")

    if detail:
        lines.append(f"\nRECENT INCIDENTS ({len(detail)} most recent):")
        for r in detail:
            date_str = str(r[0])[:10] if r[0] else "?"
            offense = r[1] or ""
            bias = r[2] or ""
            lines.append(f"  {date_str} | {bias} | {offense}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "precinct": int(pct),
        "yearly": [{"year": r[0], "count": int(r[1])} for r in (yearly or [])],
        "by_bias": [
            {"bias": r[0], "category": r[1], "count": int(r[2])}
            for r in (summary or [])
        ],
        "recent_count": len(detail) if detail else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"precinct": int(pct), "query_time_ms": elapsed},
    )


def _view_summons(pool, pct: str) -> ToolResult:
    """Criminal court summons — charges, demographics."""
    t0 = time.time()

    _, summ_rows = execute(pool, SUMMONS_SQL, [pct])
    _, detail = safe_query(pool, SUMMONS_DETAIL_SQL, [pct])
    _, demographics = safe_query(pool, SUMMONS_DEMOGRAPHICS_SQL, [pct])

    summ_years = [(r[1], r[2]) for r in summ_rows if r[0] == "summons_by_year"]
    top_types = [(r[1], r[2]) for r in summ_rows if r[0] == "top_summons"]

    lines = [
        f"CRIMINAL COURT SUMMONS — Precinct {pct}",
        "=" * 45,
    ]

    if summ_years:
        lines.append("\nYEARLY TREND:")
        for yr, total in summ_years:
            lines.append(f"  {yr}: {total}")
    else:
        lines.append("\n  No summons data found")

    if top_types:
        lines.append("\nTOP OFFENSE TYPES (recent):")
        for desc, cnt in top_types[:8]:
            lines.append(f"  {desc}: {cnt}")

    if demographics:
        total_demo = sum(int(r[1]) for r in demographics)
        lines.append("\nDEMOGRAPHICS (past year):")
        for r in demographics:
            cnt = int(r[1])
            pct_val = cnt * 100.0 / total_demo if total_demo > 0 else 0
            lines.append(f"  {r[0]}: {cnt:,} ({pct_val:.1f}%)")

    if detail:
        lines.append(f"\nRECENT SUMMONS ({len(detail)} most recent):")
        for r in detail:
            date_str = str(r[0])[:10] if r[0] else "?"
            offense = r[1] or ""
            category = r[2] or ""
            lines.append(f"  {date_str} | {category} | {offense}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "precinct": int(pct),
        "yearly": [{"year": r[0], "total": r[1]} for r in summ_years],
        "top_offenses": [{"offense": r[0], "count": r[1]} for r in top_types],
        "recent_count": len(detail) if detail else 0,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"precinct": int(pct), "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "crashes": "_crashes",
    "shootings": "_shootings",
    "force": "_force",
    "hate": "_hate",
    "summons": "_summons",
}


def safety(
    location: Annotated[str, Field(
        description="Precinct number, ZIP code, or lat/lng coordinates, e.g. '75', '10003', '40.7128,-74.0060'",
        examples=["75", "10003", "40.6782,-73.9442", "14"],
    )],
    view: Annotated[
        Literal["full", "crashes", "shootings", "force", "hate", "summons"],
        Field(
            default="full",
            description=(
                "'full' returns precinct crime report with arrests and trends. "
                "'crashes' returns motor vehicle collisions with injuries and contributing factors. "
                "'shootings' returns shooting incidents by location and time. "
                "'force' returns use of force incidents. "
                "'hate' returns hate crime incidents by bias type. "
                "'summons' returns criminal court summons by charge type."
            ),
        ),
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Crime stats, crashes, shootings, and use-of-force data by precinct or area. Returns arrest trends, collision details, and year-over-year comparisons.

    GUIDELINES: Show the complete safety report. Use tables/charts for crime comparisons.
    Present the FULL response to the user. Do not omit any category.

    LIMITATIONS: Not for building-specific violations (use building). Not for officer misconduct (use entity with role='cop').

    RETURNS: Crime counts, arrest trends, shooting incidents, crash data grouped by precinct."""
    pool = ctx.lifespan_context["pool"]
    parsed = _parse_location(location)
    directive = "PRESENTATION: Show this complete safety report with all crime and crash data. Use interactive charts for comparisons. Do not omit any category.\n\n"

    if view == "full":
        pct = _resolve_precinct(pool, parsed)
        result = _view_full(pool, pct, parsed)
    elif view == "crashes":
        result = _view_crashes(pool, parsed)
    else:
        pct = _resolve_precinct(pool, parsed)
        if view == "shootings":
            result = _view_shootings(pool, pct)
        elif view == "force":
            result = _view_force(pool, pct)
        elif view == "hate":
            result = _view_hate(pool, pct)
        elif view == "summons":
            result = _view_summons(pool, pct)
        else:
            raise ToolError(f"Unknown view: {view}")

    return ToolResult(
        content=directive + (result.content or ""),
        structured_content=result.structured_content,
        meta=result.meta,
    )
