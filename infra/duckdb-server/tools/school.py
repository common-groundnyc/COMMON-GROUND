"""school() super tool — absorbs school_search, school_report, school_compare, district_report."""

import re
import time
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError

from shared.db import execute, safe_query, fill_placeholders, parallel_queries
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS, ZIP_PATTERN

# ---------------------------------------------------------------------------
# Input patterns
# ---------------------------------------------------------------------------

_DBN_PATTERN = re.compile(r"^\d{2}[MXKQR]\d{3}$", re.IGNORECASE)
_DISTRICT_PATTERN = re.compile(r"^(?:district\s*)?(\d{1,2})$", re.IGNORECASE)
_ZIP_PATTERN = ZIP_PATTERN

# ---------------------------------------------------------------------------
# SQL — school report
# ---------------------------------------------------------------------------

SCHOOL_DIRECTORY_SQL = """
SELECT NULL AS dbn WHERE FALSE
"""

SCHOOL_PERFORMANCE_SQL = """
SELECT NULL AS year WHERE FALSE
"""

SCHOOL_ELA_SQL = """
SELECT year, grade, number_tested, mean_scale_score,
       pct_level_3_and_4, level_3_4, level_3_4_1
FROM lake.education.ela_results
WHERE geographic_subdivision = ? AND report_category = 'School'
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_MATH_SQL = """
SELECT year, grade, number_tested, mean_scale_score,
       pct_level_3_and_4, num_level_3_and_4, num_level_3_and_4
FROM lake.education.math_results
WHERE geographic_division = ? AND report_category = 'School'
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_QUALITY_SQL = """
SELECT *
FROM lake.education.quality_reports
WHERE dbn = ?
LIMIT 1
"""

SCHOOL_DEMOGRAPHICS_SQL = """
SELECT year, total_enrollment, female, female_1, male, male_1,
       asian, asian_1, black, black_1, hispanic, hispanic_1,
       multi_racial, multi_racial_1, white, white_1
FROM lake.education.demographics_2020
WHERE dbn = ?
ORDER BY year DESC
LIMIT 1
"""

SCHOOL_ATTENDANCE_SQL = """
SELECT year, school_name, grade, attendance, chronically_absent_1
FROM lake.education.chronic_absenteeism
WHERE dbn = ?
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 3
"""

SCHOOL_CLASS_SIZE_SQL = """
SELECT grade_level, program_type,
       ROUND(AVG(TRY_CAST(average_class_size AS DOUBLE)), 1) AS avg_size,
       MIN(TRY_CAST(minimum_class_size AS INT)) AS min_size,
       MAX(TRY_CAST(maximum_class_size AS INT)) AS max_size,
       SUM(TRY_CAST(number_of_students AS INT)) AS students,
       SUM(TRY_CAST(number_of_classes AS INT)) AS classes
FROM lake.education.class_size
WHERE dbn = ?
GROUP BY grade_level, program_type
ORDER BY grade_level
"""

SCHOOL_SURVEY_SQL = """
SELECT
    p.school_name,
    p.collaborative_teachers_score AS p_teachers,
    p.effective_school_leadership AS p_leadership,
    p.rigorous_instruction_score AS p_rigor,
    p.supportive_environment_score AS p_environment,
    p.trust_score AS p_trust,
    p.total_parent_response_rate AS parent_rate,
    t.collaborative_teachers_score AS t_teachers,
    t.effective_school_leadership AS t_leadership,
    t.rigorous_instruction_score AS t_rigor,
    t.supportive_environment_score AS t_environment,
    t.trust_score AS t_trust,
    t.total_teacher_response_rate AS teacher_rate,
    s.collaborative_teachers_score AS s_teachers,
    s.effective_school_leadership AS s_leadership,
    s.rigorous_instruction_score AS s_rigor,
    s.supportive_environment_score AS s_environment,
    s.trust_score AS s_trust,
    s.total_student_response_rate AS student_rate
FROM lake.education.survey_parents p
LEFT JOIN lake.education.survey_teachers t ON p.dbn = t.dbn
LEFT JOIN lake.education.survey_students s ON p.dbn = s.dbn
WHERE p.dbn = ?
LIMIT 1
"""

SCHOOL_SAFETY_DETAIL_SQL = """
SELECT school_year, location_name, address, borough_name, postcode,
       register, major_n, oth_n, nocrim_n, prop_n, vio_n,
       engroupa, bbl, latitude, longitude
FROM lake.education.school_safety
WHERE dbn = ?
ORDER BY school_year DESC
LIMIT 3
"""

SCHOOL_REGENTS_SQL = """
SELECT regents_exam, year, total_tested, mean_score,
       percent_scoring_65_or_above, percent_scoring_80_or_above
FROM lake.education.regents_results
WHERE school_dbn = ?
  AND category = 'All Students'
ORDER BY year DESC, regents_exam
LIMIT 20
"""

SCHOOL_CAFETERIA_SQL = """
SELECT inspectiondate, level, code, violationdescription
FROM lake.health.school_cafeteria_inspections
WHERE entityid = ?
ORDER BY TRY_CAST(inspectiondate AS DATE) DESC NULLS LAST
LIMIT 10
"""

SCHOOL_SPECIALIZED_HS_SQL = """
SELECT year, count_of_testers, number_of_offers, number_of_discovery_participants
FROM lake.education.specialized_hs_tests
WHERE feeder_school_dbn = ?
ORDER BY year DESC
LIMIT 5
"""

SCHOOL_DISCHARGE_SQL = """
SELECT year, discharge_category, discharge_description,
       SUM(TRY_CAST(count_of_students AS INT)) AS students
FROM lake.education.discharge_reporting
WHERE report_category = 'School'
  AND geographic_unit = ?
  AND student_category = 'All Students'
GROUP BY year, discharge_category, discharge_description
ORDER BY year DESC, students DESC
LIMIT 15
"""

# ---------------------------------------------------------------------------
# SQL — school search
# ---------------------------------------------------------------------------

SCHOOL_SEARCH_BY_NAME_SQL = """
WITH latest AS (
    SELECT dbn, school_name,
           MAX(year) AS year,
           MAX(TRY_CAST(total_enrollment AS INT)) AS enrollment
    FROM lake.education.demographics_2020
    WHERE school_name ILIKE '%' || ? || '%' ESCAPE '\\'
    GROUP BY dbn, school_name
),
safety AS (
    SELECT DISTINCT ON (dbn) dbn, address, borough_name, postcode, geographical_district_code
    FROM lake.education.school_safety
    ORDER BY dbn
)
SELECT l.dbn, l.school_name, l.year, l.enrollment,
       s.address, s.borough_name AS borough, s.postcode AS zipcode,
       s.geographical_district_code AS district
FROM latest l
LEFT JOIN safety s ON l.dbn = s.dbn
ORDER BY l.enrollment DESC NULLS LAST
LIMIT 20
"""

SCHOOL_SEARCH_BY_ZIP_SQL = """
WITH latest AS (
    SELECT dbn, school_name,
           MAX(year) AS year,
           MAX(TRY_CAST(total_enrollment AS INT)) AS enrollment
    FROM lake.education.demographics_2020
    GROUP BY dbn, school_name
),
safety AS (
    SELECT DISTINCT ON (dbn) dbn, address, borough_name, postcode, geographical_district_code
    FROM lake.education.school_safety
    WHERE postcode = ?
    ORDER BY dbn
)
SELECT l.dbn, l.school_name, l.year, l.enrollment,
       s.address, s.borough_name AS borough, s.postcode AS zipcode,
       s.geographical_district_code AS district
FROM safety s
JOIN latest l ON s.dbn = l.dbn
ORDER BY l.enrollment DESC NULLS LAST
LIMIT 30
"""

SCHOOL_SEARCH_BY_DISTRICT_SQL = """
WITH latest AS (
    SELECT dbn, school_name,
           MAX(year) AS year,
           MAX(TRY_CAST(total_enrollment AS INT)) AS enrollment
    FROM lake.education.demographics_2020
    GROUP BY dbn, school_name
),
safety AS (
    SELECT DISTINCT ON (dbn) dbn, address, borough_name, postcode, geographical_district_code
    FROM lake.education.school_safety
    WHERE LPAD(CAST(geographical_district_code AS VARCHAR), 2, '0') = LPAD(?, 2, '0')
    ORDER BY dbn
)
SELECT l.dbn, l.school_name, l.year, l.enrollment,
       s.address, s.borough_name AS borough, s.postcode AS zipcode,
       s.geographical_district_code AS district
FROM safety s
JOIN latest l ON s.dbn = l.dbn
ORDER BY l.enrollment DESC NULLS LAST
LIMIT 40
"""

# ---------------------------------------------------------------------------
# SQL — school compare
# ---------------------------------------------------------------------------

SCHOOL_COMPARE_SCORES_SQL = """
SELECT
    d.dbn,
    d.school_name,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    (SELECT COALESCE(level_3_4_1, level_3_4)
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_mean,
    (SELECT COALESCE(pct_level_3_and_4, num_level_3_and_4)
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS math_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     -- NOTE: math_results uses geographic_division (not geographic_subdivision like ela_results)
     ORDER BY year DESC LIMIT 1) AS math_mean,
    (SELECT attendance
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS attendance_pct,
    (SELECT chronically_absent_1
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS chronic_absent_pct,
    (SELECT trust_score
     FROM lake.education.survey_parents
     WHERE dbn = d.dbn LIMIT 1) AS parent_trust,
    (SELECT COALESCE(TRY_CAST(major_n AS INT), 0) + COALESCE(TRY_CAST(vio_n AS INT), 0)
     FROM lake.education.school_safety
     WHERE dbn = d.dbn
     ORDER BY school_year DESC LIMIT 1) AS safety_incidents,
    (SELECT ROUND(AVG(TRY_CAST(average_class_size AS DOUBLE)), 1)
     FROM lake.education.class_size
     WHERE dbn = d.dbn) AS avg_class_size,
    d.asian_1 AS asian_pct,
    d.black_1 AS black_pct,
    d.hispanic_1 AS hispanic_pct,
    d.white_1 AS white_pct,
    s.postcode AS zipcode
FROM (
    SELECT DISTINCT ON (dbn) *
    FROM lake.education.demographics_2020
    WHERE dbn IN ({placeholders})
    ORDER BY dbn, year DESC
) d
LEFT JOIN (
    SELECT DISTINCT ON (dbn) *
    FROM lake.education.school_safety
    ORDER BY dbn, school_year DESC
) s ON d.dbn = s.dbn
ORDER BY d.dbn
"""

# ---------------------------------------------------------------------------
# SQL — district report
# ---------------------------------------------------------------------------

DISTRICT_SCHOOLS_SQL = """
SELECT
    d.dbn,
    d.school_name,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment
FROM lake.education.demographics_2020 d
WHERE SUBSTRING(d.dbn, 1, 2) = LPAD(?, 2, '0')
ORDER BY enrollment DESC NULLS LAST
"""

DISTRICT_AGGREGATE_SQL = """
SELECT
    COUNT(DISTINCT d.dbn) AS school_count,
    SUM(TRY_CAST(d.total_enrollment AS INT)) AS total_students,
    ROUND(AVG(TRY_CAST(d.total_enrollment AS INT)), 0) AS avg_enrollment,
    ROUND(AVG(TRY_CAST(d.asian_1 AS DOUBLE)), 1) AS avg_asian_pct,
    ROUND(AVG(TRY_CAST(d.black_1 AS DOUBLE)), 1) AS avg_black_pct,
    ROUND(AVG(TRY_CAST(d.hispanic_1 AS DOUBLE)), 1) AS avg_hispanic_pct,
    ROUND(AVG(TRY_CAST(d.white_1 AS DOUBLE)), 1) AS avg_white_pct
FROM lake.education.demographics_2020 d
WHERE SUBSTRING(d.dbn, 1, 2) = LPAD(?, 2, '0')
"""

DISTRICT_TEST_SCORES_SQL = """
SELECT
    year,
    'ELA' AS subject,
    COUNT(*) AS schools_tested,
    ROUND(AVG(TRY_CAST(mean_scale_score AS DOUBLE)), 1) AS avg_mean_score
FROM lake.education.ela_results
WHERE report_category = 'School'
  AND grade = 'All Grades'
  AND SUBSTRING(geographic_subdivision, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
UNION ALL
SELECT
    year,
    'Math' AS subject,
    COUNT(*) AS schools_tested,
    ROUND(AVG(TRY_CAST(mean_scale_score AS DOUBLE)), 1) AS avg_mean_score
FROM lake.education.math_results
WHERE report_category = 'School'
  AND grade = 'All Grades'
  AND SUBSTRING(geographic_division, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
ORDER BY year DESC, subject
"""

DISTRICT_ATTENDANCE_SQL = """
SELECT
    year,
    COUNT(DISTINCT dbn) AS schools,
    ROUND(AVG(TRY_CAST(attendance AS DOUBLE)), 1) AS avg_attendance,
    ROUND(AVG(TRY_CAST(chronically_absent_1 AS DOUBLE)), 1) AS avg_chronic_absent
FROM lake.education.chronic_absenteeism
WHERE grade = 'All Grades'
  AND SUBSTRING(dbn, 1, 2) = LPAD(?, 2, '0')
GROUP BY year
ORDER BY year DESC
LIMIT 3
"""

DISTRICT_SAFETY_SQL = """
SELECT
    school_year,
    COUNT(DISTINCT dbn) AS schools,
    SUM(TRY_CAST(major_n AS INT)) AS major_incidents,
    SUM(TRY_CAST(vio_n AS INT)) AS violent_incidents,
    SUM(TRY_CAST(prop_n AS INT)) AS property_incidents,
    SUM(TRY_CAST(register AS INT)) AS total_register
FROM lake.education.school_safety
WHERE CAST(geographical_district_code AS INT) = CAST(? AS INT)
GROUP BY school_year
ORDER BY school_year DESC
LIMIT 3
"""


# ---------------------------------------------------------------------------
# Private functions
# ---------------------------------------------------------------------------

def _report(pool: object, dbn: str) -> ToolResult:
    """Full school report by DBN."""
    t0 = time.time()

    results = parallel_queries(pool, [
        ("directory",   SCHOOL_DIRECTORY_SQL,       [dbn]),
        ("performance", SCHOOL_PERFORMANCE_SQL,     [dbn]),
        ("ela",         SCHOOL_ELA_SQL,             [dbn]),
        ("math",        SCHOOL_MATH_SQL,            [dbn]),
        ("quality",     SCHOOL_QUALITY_SQL,         [dbn]),
        ("demo",        SCHOOL_DEMOGRAPHICS_SQL,    [dbn]),
        ("attend",      SCHOOL_ATTENDANCE_SQL,      [dbn]),
        ("class_size",  SCHOOL_CLASS_SIZE_SQL,      [dbn]),
        ("survey",      SCHOOL_SURVEY_SQL,          [dbn]),
        ("safety",      SCHOOL_SAFETY_DETAIL_SQL,   [dbn]),
        ("regents",     SCHOOL_REGENTS_SQL,         [dbn]),
        ("cafe",        SCHOOL_CAFETERIA_SQL,       [dbn]),
        ("shsat",       SCHOOL_SPECIALIZED_HS_SQL,  [dbn]),
        ("discharge",   SCHOOL_DISCHARGE_SQL,       [dbn]),
    ])

    _, dir_rows      = results.get("directory",   ([], []))
    _, perf_rows     = results.get("performance", ([], []))
    _, ela_rows      = results.get("ela",         ([], []))
    _, math_rows     = results.get("math",        ([], []))
    _, qual_rows     = results.get("quality",     ([], []))
    _, demo_rows     = results.get("demo",        ([], []))
    _, attend_rows   = results.get("attend",      ([], []))
    _, class_rows    = results.get("class_size",  ([], []))
    _, survey_rows   = results.get("survey",      ([], []))
    _, safety_rows   = results.get("safety",      ([], []))
    _, regents_rows  = results.get("regents",     ([], []))
    _, cafe_rows     = results.get("cafe",        ([], []))
    _, shsat_rows    = results.get("shsat",       ([], []))
    _, discharge_rows = results.get("discharge",  ([], []))

    if not dir_rows and not perf_rows and not ela_rows and not demo_rows and not safety_rows:
        raise ToolError(f"No school found for DBN {dbn}. Try a name search instead.")

    lines = [f"SCHOOL REPORT — {dbn}", "=" * 55]

    # --- Directory ---
    if dir_rows:
        d = dir_rows[0]
        lines.append(f"\n{d[1]}")
        lines.append(f"  District {d[2]} | Principal: {d[3] or 'N/A'}")
        lines.append(f"  Admission: {d[4] or 'N/A'}")
        flags = []
        if d[5] == '1': flags.append("Community School")
        if d[6] == '1': flags.append("Gifted & Talented")
        if d[7] == '1': flags.append("CTE")
        if d[8] and d[8] != '0': flags.append("Dual Language/Transitional")
        if d[9]: flags.append(f"Federal: {d[9]}")
        if flags:
            lines.append(f"  Programs: {', '.join(flags)}")

    # --- Safety/Location ---
    if safety_rows:
        s = safety_rows[0]
        if not dir_rows:
            lines.append(f"\n{s[1]}")
            lines.append(f"  {s[2]}, {s[3]} {s[4]}")
        else:
            lines.append(f"\n  Address: {s[2]}, {s[3]} {s[4]}")
        if s[5]:
            lines.append(f"  Register: {s[5]} students")

    # --- Demographics ---
    if demo_rows:
        d = demo_rows[0]
        enrollment = d[1] or '?'
        lines.append(f"\nDEMOGRAPHICS ({d[0]})")
        lines.append(f"  Enrollment: {enrollment}")
        if d[2] and d[3]:
            lines.append(f"  Gender: {d[3]}% female, {d[5]}% male")
        race_parts = []
        for label, pct_idx in [("Asian", 7), ("Black", 9), ("Hispanic", 11), ("Multi-racial", 13), ("White", 15)]:
            pct = d[pct_idx] if len(d) > pct_idx and d[pct_idx] else None
            if pct:
                race_parts.append(f"{label} {pct}%")
        if race_parts:
            lines.append(f"  Race: {', '.join(race_parts)}")

    # --- Performance ---
    if perf_rows:
        lines.append("\nPERFORMANCE (DOE Framework):")
        for r in perf_rows:
            perf = float(r[1]) if r[1] else 0
            impact = float(r[2]) if r[2] else 0
            lines.append(f"  {r[0]}: Performance {perf:.2f} | Impact {impact:.2f} ({r[3]})")

    # --- ELA ---
    if ela_rows:
        lines.append("\nELA TEST RESULTS:")
        for r in ela_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Math ---
    if math_rows:
        lines.append("\nMATH TEST RESULTS:")
        for r in math_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Regents ---
    if regents_rows:
        lines.append("\nREGENTS EXAMS:")
        current_year = None
        for r in regents_rows:
            exam, year, tested, mean, pct65, pct80 = r
            if year != current_year:
                current_year = year
                lines.append(f"  {year}:")
            lines.append(f"    {exam}: {tested} tested, mean {mean}, >=65: {pct65 or '?'}%, >=80: {pct80 or '?'}%")

    # --- Attendance ---
    if attend_rows:
        lines.append("\nATTENDANCE:")
        for r in attend_rows:
            year, name, grade, attend_rate, chronic_pct = r
            lines.append(f"  {year}: {attend_rate}% attendance, {chronic_pct}% chronically absent")

    # --- Class Size ---
    if class_rows:
        lines.append("\nCLASS SIZE:")
        for r in class_rows:
            grade, prog, avg, mn, mx, students, classes = r
            avg_str = f"{avg}" if avg else "?"
            lines.append(f"  {grade} ({prog}): avg {avg_str} students, {classes or '?'} classes")

    # --- Surveys ---
    if survey_rows:
        s = survey_rows[0]
        lines.append("\nSURVEY SCORES:")
        lines.append(f"  Parents (response rate: {s[6] or '?'}%):")
        lines.append(f"    Teachers: {s[1] or '?'} | Leadership: {s[2] or '?'} | Rigor: {s[3] or '?'} | Environment: {s[4] or '?'} | Trust: {s[5] or '?'}")
        if s[7]:
            lines.append(f"  Teachers (response rate: {s[12] or '?'}%):")
            lines.append(f"    Teachers: {s[7] or '?'} | Leadership: {s[8] or '?'} | Rigor: {s[9] or '?'} | Environment: {s[10] or '?'} | Trust: {s[11] or '?'}")
        if s[13]:
            lines.append(f"  Students (response rate: {s[18] or '?'}%):")
            lines.append(f"    Teachers: {s[13] or '?'} | Leadership: {s[14] or '?'} | Rigor: {s[15] or '?'} | Environment: {s[16] or '?'} | Trust: {s[17] or '?'}")

    # --- Safety incidents ---
    if safety_rows:
        s = safety_rows[0]
        major = s[6] or 0
        other = s[7] or 0
        nocrim = s[8] or 0
        prop = s[9] or 0
        violent = s[10] or 0
        total = int(major or 0) + int(other or 0) + int(nocrim or 0) + int(prop or 0) + int(violent or 0)
        if total > 0:
            lines.append(f"\nSAFETY ({s[0]}):")
            lines.append(f"  Total incidents: {total}")
            if int(major or 0) > 0: lines.append(f"    Major criminal: {major}")
            if int(violent or 0) > 0: lines.append(f"    Violent: {violent}")
            if int(prop or 0) > 0: lines.append(f"    Property: {prop}")
            if int(other or 0) > 0: lines.append(f"    Other: {other}")
            if int(nocrim or 0) > 0: lines.append(f"    Non-criminal: {nocrim}")

    # --- Cafeteria inspections ---
    if cafe_rows:
        lines.append("\nCAFETERIA INSPECTIONS:")
        for r in cafe_rows[:5]:
            date, level, code, desc = r
            lines.append(f"  {date or '?'}: [{level or '?'}] {desc or code or 'N/A'}")

    # --- SHSAT feeder data ---
    if shsat_rows:
        lines.append("\nSPECIALIZED HS TEST (SHSAT) RESULTS:")
        for r in shsat_rows:
            year, testers, offers, discovery = r
            lines.append(f"  {year}: {testers} testers, {offers} offers" + (f", {discovery} discovery" if discovery and discovery != '0' else ""))

    # --- Composite quality ---
    quality_score = None
    if perf_rows:
        latest = perf_rows[0]
        perf_val = float(latest[1]) if latest[1] else 0
        impact_val = float(latest[2]) if latest[2] else 0
        quality_score = round((perf_val * 60 + impact_val * 40) * 100)
        if quality_score >= 70: rating = "STRONG"
        elif quality_score >= 50: rating = "GOOD"
        elif quality_score >= 30: rating = "FAIR"
        else: rating = "NEEDS IMPROVEMENT"
        lines.append(f"\nCOMPOSITE QUALITY: {quality_score}/100 — {rating}")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    structured = {
        "dbn": dbn,
        "directory": dir_rows[0] if dir_rows else None,
        "performance": [list(r) for r in perf_rows] if perf_rows else [],
        "quality_score": quality_score,
        "demographics": demo_rows[0] if demo_rows else None,
        "safety_incidents": safety_rows[0] if safety_rows else None,
    }

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"dbn": dbn, "query_time_ms": elapsed},
    )


def _search_by_name(pool: object, name: str) -> ToolResult:
    """Search schools by name."""
    name_query = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    t0 = time.time()
    _, rows = safe_query(pool, SCHOOL_SEARCH_BY_NAME_SQL, [name_query])

    if not rows:
        raise ToolError(f"No schools found matching '{name}'. Try a broader search term.")

    return _format_search_results(rows, f"name matching '{name}'", t0)


def _search_by_zip(pool: object, zipcode: str) -> ToolResult:
    """Search schools by ZIP code."""
    t0 = time.time()
    _, rows = safe_query(pool, SCHOOL_SEARCH_BY_ZIP_SQL, [zipcode])

    if not rows:
        raise ToolError(f"No schools found in ZIP {zipcode}.")

    return _format_search_results(rows, f"ZIP {zipcode}", t0)


def _search_by_district(pool: object, district: str) -> ToolResult:
    """Search schools by district number."""
    t0 = time.time()
    _, rows = safe_query(pool, SCHOOL_SEARCH_BY_DISTRICT_SQL, [district])

    if not rows:
        raise ToolError(f"No schools found in District {int(district)}.")

    return _format_search_results(rows, f"District {int(district)}", t0)


def _format_search_results(rows: list, search_type: str, t0: float) -> ToolResult:
    """Format school search results."""
    lines = [f"SCHOOL SEARCH — {search_type}", f"Found {len(rows)} schools", "=" * 50]

    for r in rows:
        dbn, name, year, enrollment, address, borough, zipcode, district = r[:8]
        try:
            enrollment_str = f"{int(enrollment):,}" if enrollment else "?"
        except (ValueError, TypeError):
            enrollment_str = str(enrollment) if enrollment else "?"
        addr_str = f"{address}, {borough}" if address else borough or ""
        zip_str = f" {zipcode}" if zipcode else ""
        dist_str = f"D{district}" if district else ""

        lines.append(f"\n  {name}")
        lines.append(f"    DBN: {dbn} | {dist_str} | {enrollment_str} students")
        if addr_str:
            lines.append(f"    {addr_str}{zip_str}")

    lines.append(f"\nUse school('DBN') for full details on any school.")
    lines.append(f"Use school('DBN1,DBN2') to compare schools side-by-side.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"search_type": search_type, "results": [{"dbn": r[0], "name": r[1], "enrollment": r[3], "zipcode": r[6], "district": r[7]} for r in rows]},
        meta={"query": search_type, "result_count": len(rows), "query_time_ms": elapsed},
    )


def _compare(pool: object, dbns: list[str]) -> ToolResult:
    """Compare multiple schools side-by-side."""
    if len(dbns) < 2:
        raise ToolError("Provide at least 2 DBNs to compare (comma-separated).")
    if len(dbns) > 7:
        raise ToolError("Maximum 7 schools per comparison.")

    dbns = [d.strip().upper() for d in dbns]
    for d in dbns:
        if not re.match(r'^[A-Z0-9]{5,6}$', d):
            raise ToolError(f"Invalid DBN format: '{d}'. Expected 5-6 alphanumeric chars like 02M001.")

    t0 = time.time()
    sql = fill_placeholders(SCHOOL_COMPARE_SCORES_SQL, dbns)
    _, rows = safe_query(pool, sql, dbns)

    if not rows:
        raise ToolError(f"No data found for DBNs: {', '.join(dbns)}. Verify with a name search.")

    lines = [f"SCHOOL COMPARISON — {len(rows)} schools", "=" * 60]

    names = [r[1][:25] for r in rows]
    lines.append(f"\n{'Metric':<25} | " + " | ".join(f"{n:<25}" for n in names))
    lines.append("-" * (27 + 28 * len(rows)))

    metrics = [
        ("Enrollment", 2, "{:,}"),
        ("ELA Proficient %", 3, "{}%"),
        ("ELA Mean Score", 4, "{}"),
        ("Math Proficient %", 5, "{}%"),
        ("Math Mean Score", 6, "{}"),
        ("Attendance %", 7, "{}%"),
        ("Chronic Absent %", 8, "{}%"),
        ("Parent Trust", 9, "{}"),
        ("Safety Incidents", 10, "{}"),
        ("Avg Class Size", 11, "{}"),
        ("Asian %", 12, "{}%"),
        ("Black %", 13, "{}%"),
        ("Hispanic %", 14, "{}%"),
        ("White %", 15, "{}%"),
    ]

    for label, idx, fmt in metrics:
        vals = []
        for r in rows:
            v = r[idx] if idx < len(r) and r[idx] is not None else "\u2014"
            try:
                vals.append(fmt.format(v))
            except (ValueError, TypeError):
                vals.append(str(v) if v != "\u2014" else "\u2014")
        lines.append(f"{label:<25} | " + " | ".join(f"{v:<25}" for v in vals))

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"dbns": dbns, "schools": [{"dbn": r[0], "name": r[1], "enrollment": r[2]} for r in rows]},
        meta={"dbn_count": len(dbns), "query_time_ms": elapsed},
    )


def _district(pool: object, district_num: str) -> ToolResult:
    """Aggregate district report."""
    district = district_num.strip().lstrip('0') or '0'
    if not district.isdigit() or int(district) < 1 or int(district) > 79:
        raise ToolError("District must be 1-32, 75, or 79.")

    padded = district.zfill(2)
    t0 = time.time()

    _, agg_rows = safe_query(pool, DISTRICT_AGGREGATE_SQL, [padded])
    _, test_rows = safe_query(pool, DISTRICT_TEST_SCORES_SQL, [padded, padded])
    _, attend_rows = safe_query(pool, DISTRICT_ATTENDANCE_SQL, [padded])
    _, safety_rows = safe_query(pool, DISTRICT_SAFETY_SQL, [padded])
    _, school_rows = safe_query(pool, DISTRICT_SCHOOLS_SQL, [padded])

    if not agg_rows or not agg_rows[0][0]:
        raise ToolError(f"No data found for District {district}. Valid districts: 1-32, 75, 79.")

    a = agg_rows[0]
    lines = [f"DISTRICT {district} REPORT", "=" * 50]

    lines.append(f"\nOVERVIEW")
    lines.append(f"  Schools: {a[0]}")
    lines.append(f"  Total students: {int(a[1] or 0):,}")
    lines.append(f"  Average enrollment: {int(a[2] or 0):,}")

    lines.append(f"\nDEMOGRAPHICS (district average)")
    for label, val in [("Asian", a[3]), ("Black", a[4]), ("Hispanic", a[5]), ("White", a[6])]:
        if val:
            lines.append(f"  {label}: {val}%")

    if test_rows:
        lines.append(f"\nTEST SCORES (district average):")
        for r in test_rows[:6]:
            year, subject, count, avg_score = r
            lines.append(f"  {year} {subject}: avg mean score {avg_score} ({count} schools)")

    if attend_rows:
        lines.append(f"\nATTENDANCE:")
        for r in attend_rows:
            year, schools, avg_attend, avg_chronic = r
            lines.append(f"  {year}: {avg_attend}% avg attendance, {avg_chronic}% chronic absent ({schools} schools)")

    if safety_rows:
        lines.append(f"\nSAFETY:")
        for r in safety_rows:
            sy, schools, major, violent, prop, register = r
            total = int(major or 0) + int(violent or 0) + int(prop or 0)
            rate = round(total / int(register) * 1000, 1) if register and int(register) > 0 else 0
            lines.append(f"  {sy}: {total} incidents across {schools} schools ({rate} per 1000 students)")

    if school_rows:
        lines.append(f"\nLARGEST SCHOOLS:")
        for r in school_rows[:10]:
            dbn, name, enrollment = r
            enr_str = f"{enrollment:,}" if enrollment else "?"
            lines.append(f"  {dbn} — {name} ({enr_str} students)")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"district": district, "school_count": a[0], "total_students": a[1]},
        meta={"district": district, "query_time_ms": elapsed},
    )


# ---------------------------------------------------------------------------
# Public super tool
# ---------------------------------------------------------------------------

def school(
    query: Annotated[str, Field(
        description="School name, DBN, ZIP, or district number. Auto-detected, e.g. 'Stuyvesant', '02M475', '10003', '2', '02M475,02M001'",
        examples=["Stuyvesant", "02M475", "10003", "district 2", "02M475,02M001"],
    )],
    ctx: Context = None,
) -> ToolResult:
    """Look up NYC public school profiles, test scores, and demographics by name, DBN, ZIP, or district. Returns ELA/math scores, absenteeism, class size, and survey results.

    GUIDELINES: Show complete school profile with all test scores and demographics. Use tables for score breakdowns.
    Present the FULL response to the user. Do not omit any metric.

    LIMITATIONS: Not for neighborhood-level education stats (use neighborhood).

    RETURNS: School profile with test scores, demographics, attendance, and quality ratings."""
    query = query.strip()
    if not query or len(query) < 1:
        raise ToolError("Query must not be empty.")

    pool = ctx.lifespan_context["pool"]
    directive = "PRESENTATION: Show the complete school profile with test scores and demographics. Use tables for score breakdowns. Do not omit any metric.\n\n"

    # Multiple DBNs → compare
    if "," in query:
        dbns = [d.strip() for d in query.split(",") if d.strip()]
        result = _compare(pool, dbns)
    elif _DBN_PATTERN.match(query):
        result = _report(pool, query.upper())
    elif _ZIP_PATTERN.match(query):
        result = _search_by_zip(pool, query)
    else:
        m = _DISTRICT_PATTERN.match(query)
        if m:
            result = _district(pool, m.group(1))
        else:
            if len(query) < 2:
                raise ToolError("Search query must be at least 2 characters.")
            result = _search_by_name(pool, query)

    return ToolResult(
        content=directive + (result.content if isinstance(result.content, str) else "\n".join(str(c) for c in result.content) if result.content else ""),
        structured_content=result.structured_content,
        meta=result.meta,
    )
