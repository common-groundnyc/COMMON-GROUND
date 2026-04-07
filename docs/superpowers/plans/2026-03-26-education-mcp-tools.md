# Education MCP Tools — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build 3 new MCP tools and enhance 1 existing tool to unlock 27 education tables (4.8M rows) + cross-schema school data that currently has only a single DBN-gated entry point.

**Architecture:** Add `school_search` (name/ZIP entry point), `school_compare` (side-by-side), `district_report` (aggregate), and enhance existing `school_report` with demographics, attendance, safety, surveys, cafeteria inspections, and Regents data. All tools follow the existing pattern: SQL constants → `_safe_query()` → formatted text + `ToolResult`.

**Tech Stack:** Python, FastMCP, DuckDB SQL, existing `mcp_server.py` patterns (`_safe_query`, `ToolResult`, `READONLY` annotations, `ZIP`/`BBL`/`NAME` type aliases)

---

## Data Inventory

Everything we have that touches education, across schemas:

### `education` schema (27 tables, 4.8M rows)

| Table | Rows | Key Columns | Join Key |
|-------|------|-------------|----------|
| `quality_reports` | 1.5M | dbn, school_name, metric_variable_name, metric_value, metric_score | dbn |
| `attendance_2015` | 843k | school_dbn, date, enrolled, present, absent | school_dbn |
| `attendance_2018` | 737k | school_dbn, date, enrolled, present, absent | school_dbn |
| `ela_results` | 626k | report_category, geographic_subdivision (=DBN), school_name, grade, year, number_tested, mean_scale_score, level_3_4_1 | geographic_subdivision |
| `math_results` | 625k | report_category, geographic_division (=DBN), school_name, grade, year, number_tested, mean_scale_score, pct_level_3_and_4 | geographic_division |
| `discharge_reporting` | 302k | year, school_name, student_category, discharge_category, count_of_students | geographic_unit |
| `chronic_absenteeism` | 53k | dbn, school_name, grade, year, attendance, chronically_absent_1 | dbn |
| `regents_results` | 33k | school_dbn, school_name, regents_exam, year, mean_score, percent_scoring_65_or_above | school_dbn |
| `class_size` | 12k | dbn, school_name, grade_level, program_type, average_class_size | dbn |
| `demographics_2020` | 9.2k | dbn, school_name, year, total_enrollment, grade_k..grade_12, female, male, asian, black, hispanic, multi_racial | dbn |
| `demographics_2019` | 9.1k | (same structure) | dbn |
| `demographics_2018` | 9.0k | (same structure) | dbn |
| `demographics_2013` | 9.0k | (same structure) | dbn |
| `school_safety` | 6.3k | dbn, location_name, address, borough, geographical_district_code, **postcode**, **bbl**, latitude, longitude, major_n, oth_n, vio_n | dbn, postcode |
| `survey_parents` | 2.9k | dbn, school_name, collaborative_teachers_score, effective_school_leadership, rigorous_instruction_score, supportive_environment_score, trust_score | dbn |
| `survey_teachers` | 1.9k | (same structure) | dbn |
| `survey_students` | 1.8k | (same structure) | dbn |
| `specialized_hs_tests` | 2.7k | year, feeder_school_dbn, feeder_school_name, count_of_testers, number_of_offers | feeder_school_dbn |
| `specialized_hs_summary` | 160 | year, type_of_summary, summary (school name + DBN), racial breakdown | (parse DBN from summary) |
| `quality_elem_middle` | 1.3k | dbn, school_name, enrollment, survey scores, quality review ratings | dbn |
| `quality_high_schools` | 485 | dbn, school_name, enrollment, survey scores, quality review ratings | dbn |
| `quality_early_childhood` | 51 | dbn, school_name | dbn |
| `school_programs` | 930 | district, borough, building_id, org_id, school, project, description | org_id |
| `capacity_projects` | 71 | (small) | — |

### Cross-schema education data

| Schema | Table | Rows | What It Adds | Join Key |
|--------|-------|------|--------------|----------|
| `health` | `school_cafeteria_inspections` | 24.5k | Food safety grades, violations at school cafeterias | entityid ≈ dbn, **zipcode** |
| `health` | `childcare_inspections` | 27.8k | Daycare/pre-K inspection results | zipcode |
| `health` | `childcare_programs` | 2.7k | Licensed childcare programs | zipcode |
| `city_government` | `pluto` | 858k | Building info for school properties (landuse='8') | bbl |
| `federal` | `college_scorecard_schools` | 70k | College outcomes, earnings, graduation rates | zipcode, name |

### Key join patterns

- **DBN** (e.g. `02M001`) is the universal school key — present in nearly all education tables
- **ZIP** is available in `school_safety.postcode` and `school_cafeteria_inspections.zipcode`
- **BBL** is available in `school_safety.bbl` (links to building/housing tools)
- **District** is the first 2 digits of the DBN (districts 1-32 + 75, 79)
- **Borough** encoded in DBN middle letter: M=Manhattan, X=Bronx, K=Brooklyn, Q=Queens, R=Staten Island

### Tables referenced by existing `school_report` that may not exist

The current `school_report` queries `lake.education.schools_directory` and `lake.education.schools_performance` — these don't appear in our education table listing. `_safe_query` degrades gracefully, returning empty results. Our new tools should NOT depend on these phantom tables; instead use `demographics_2020` + `school_safety` + `quality_elem_middle`/`quality_high_schools` as the school directory source.

### Known bugs in existing `school_report`

1. **`SCHOOL_MATH_SQL` uses wrong column name**: Line 5147 queries `WHERE geographic_subdivision = ?` but the actual column in `math_results` is `geographic_division` (no "sub"). This means math results have been silently returning empty. We fix this in Task 2.
2. **`school_safety` has both `borough` and `borough_name`**: The full column list is `borough` (single letter: M/K/X/Q/R) and `borough_name` (full name: Manhattan/Brooklyn/etc). We use `borough_name` in our SQL.
3. **`geographical_district_code` is unpadded**: Values are `1`, `15`, `32` — not `01`, `15`, `32`. SQL must handle both formats.

### Column name verification notes

- `math_results`: join key = `geographic_division` (NOT `geographic_subdivision`)
- `ela_results`: join key = `geographic_subdivision` (correct in existing code)
- `school_safety`: has `borough` (letter) AND `borough_name` (full name), `postcode` (ZIP), `bbl`, `geographical_district_code` (unpadded int)
- `discharge_reporting`: `geographic_unit` — **UNVERIFIED** whether this contains DBNs or district codes. Queries using this are wrapped in `_safe_query` for graceful degradation. Verify at implementation time.

---

## File Structure

All changes in a single file:

- **Modify:** `infra/duckdb-server/mcp_server.py` — add SQL constants and 3 new tool functions, enhance 1 existing tool
  - New SQL constants block: `SCHOOL_SEARCH_*`, `SCHOOL_COMPARE_*`, `DISTRICT_REPORT_*`, plus new `SCHOOL_REPORT_*` constants
  - New functions: `school_search()`, `school_compare()`, `district_report()`
  - Enhanced function: `school_report()` (add ~6 new query sections)

Insert location: After the existing school_report block (after line ~5298, where `return ToolResult(...)` ends) and before the commercial_vitality block (line ~5301). SQL constants go before the function at ~line 5167.

---

## Task 1: `school_search` — Find Schools by Name or ZIP

The critical missing entry point. Currently you must already know a DBN — nobody knows DBNs.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after line ~5298)

- [ ] **Step 1: Add SQL constants for school_search**

Insert after line ~5298 (after school_report function ends, before commercial_vitality):

```python
# ---------------------------------------------------------------------------
# School Search
# ---------------------------------------------------------------------------

SCHOOL_SEARCH_BY_NAME_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE d.school_name ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 20
"""

SCHOOL_SEARCH_BY_ZIP_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE s.postcode = ?
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 30
"""

SCHOOL_SEARCH_BY_DISTRICT_SQL = """
SELECT DISTINCT
    d.dbn,
    d.school_name,
    d.year,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    s.address,
    s.borough_name AS borough,
    s.postcode AS zipcode,
    s.geographical_district_code AS district
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE LPAD(CAST(s.geographical_district_code AS VARCHAR), 2, '0') = LPAD(?, 2, '0')
ORDER BY TRY_CAST(d.total_enrollment AS INT) DESC NULLS LAST
LIMIT 40
"""
```

- [ ] **Step 2: Add the school_search tool function**

```python
@mcp.tool(annotations=READONLY, tags={"education"})
def school_search(
    query: Annotated[str, Field(description="School name, 5-digit ZIP code, or 2-digit district number. Examples: 'Stuyvesant', '10003', '02'")],
    ctx: Context,
) -> ToolResult:
    """Find NYC public schools by name, ZIP code, or district number. Use this as the entry point when you don't know a school's DBN. Returns school names, DBNs, addresses, enrollment, and district. Follow up with school_report(dbn) for detailed performance data or school_compare([dbns]) for side-by-side comparison. Examples: school_search('Stuyvesant'), school_search('10003'), school_search('02')."""
    query = query.strip()
    if not query or len(query) < 2:
        raise ToolError("Search query must be at least 2 characters.")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # Detect query type: ZIP (5 digits), district (1-2 digits), or name
    if re.match(r"^\d{5}$", query):
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_ZIP_SQL, [query])
        search_type = f"ZIP {query}"
    elif re.match(r"^\d{1,2}$", query):
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_DISTRICT_SQL, [query])
        search_type = f"District {int(query)}"
    else:
        _, rows = _safe_query(db, SCHOOL_SEARCH_BY_NAME_SQL, [query])
        search_type = f"name matching '{query}'"

    if not rows:
        raise ToolError(f"No schools found for {search_type}. Try a broader search term.")

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

    lines.append(f"\nUse school_report(dbn) for full details on any school.")
    lines.append(f"Use school_compare([dbn1, dbn2, ...]) to compare schools side-by-side.")

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"search_type": search_type, "results": [{"dbn": r[0], "name": r[1], "enrollment": r[3], "zipcode": r[6], "district": r[7]} for r in rows]},
        meta={"query": query, "result_count": len(rows), "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Test school_search locally**

Run a quick smoke test via the MCP server:
```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python -c "
# Quick test: verify SQL runs without error
import duckdb
db = duckdb.connect('ducklake:...')  # use actual connection string
print(db.execute(\"SELECT DISTINCT d.dbn, d.school_name FROM lake.education.demographics_2020 d WHERE d.school_name ILIKE '%stuyvesant%' LIMIT 5\").fetchall())
"
```
Expected: Returns Stuyvesant High School rows

- [ ] **Step 4: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add school_search tool — find schools by name, ZIP, or district"
```

---

## Task 2: Enhance `school_report` — Add All Missing Dimensions

The existing tool queries 5 tables. We have 20+ education tables it ignores.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (lines ~5116-5298)

- [ ] **Step 0: Fix existing SCHOOL_MATH_SQL bug (line ~5147)**

The existing SQL uses the wrong column name. Fix:
```python
# BEFORE (broken — column doesn't exist, silently returns empty):
# WHERE geographic_subdivision = ? AND report_category = 'School'

# AFTER (correct column name):
SCHOOL_MATH_SQL = """
SELECT year, grade, number_tested, mean_scale_score,
       pct_level_3_and_4, num_level_3_and_4, num_level_3_and_4
FROM lake.education.math_results
WHERE geographic_division = ? AND report_category = 'School'
  AND grade = 'All Grades'
ORDER BY year DESC
LIMIT 5
"""
```

Also note: The existing `SCHOOL_SAFETY_SQL` (line ~5153, `SELECT * FROM lake.education.school_safety WHERE dbn = ?`) is being **superseded** by the new `SCHOOL_SAFETY_DETAIL_SQL` which selects specific columns. The old constant can be left in place (not referenced by the new function body) or removed.

- [ ] **Step 1: Add new SQL constants after existing SCHOOL_QUALITY_SQL (line ~5166)**

```python
SCHOOL_DEMOGRAPHICS_SQL = """
SELECT year, total_enrollment, female, female_1, male, male_1,
       asian, asian_1, black, black_1, hispanic, hispanic_1,
       multi_racial, multi_racial_1, white, white_1,
       grade_3k_pk_half_day_full, grade_k, grade_1, grade_2, grade_3, grade_4,
       grade_5, grade_6, grade_7, grade_8, grade_9, grade_10, grade_11, grade_12
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
```

- [ ] **Step 2: Enhance the school_report function body (lines 5169-5282)**

Replace the function body to add new query sections. Keep all existing sections and append:

```python
@mcp.tool(annotations=READONLY, tags={"education", "services"})
def school_report(dbn: Annotated[str, Field(description="School DBN: district(2) + borough letter(1) + number(3). Example: 02M001. Borough: M=Manhattan, X=Bronx, K=Brooklyn, Q=Queens, R=Staten Island")], ctx: Context) -> ToolResult:
    """Comprehensive school report for a NYC public school by DBN — test scores, demographics, attendance, safety, class size, surveys, cafeteria inspections, and Regents results. Use school_search(query) first to find DBNs if you don't know them. For comparing schools, use school_compare([dbns]). For district-level stats, use district_report(district). DBN format: district(2) + borough letter + number(3). Examples: 02M001 (PS 1, Manhattan), 13K330 (Brooklyn)."""
    dbn = dbn.strip().upper()
    if len(dbn) < 5 or len(dbn) > 6:
        raise ToolError("DBN must be 5-6 characters. Format: district(2) + borough(1) + number(3). Example: 02M001")

    db = ctx.lifespan_context["db"]
    t0 = time.time()

    # --- Existing queries (keep) ---
    _, dir_rows = _safe_query(db, SCHOOL_DIRECTORY_SQL, [dbn])
    _, perf_rows = _safe_query(db, SCHOOL_PERFORMANCE_SQL, [dbn])
    _, ela_rows = _safe_query(db, SCHOOL_ELA_SQL, [dbn])
    _, math_rows = _safe_query(db, SCHOOL_MATH_SQL, [dbn])
    _, qual_rows = _safe_query(db, SCHOOL_QUALITY_SQL, [dbn])

    # --- New queries ---
    _, demo_rows = _safe_query(db, SCHOOL_DEMOGRAPHICS_SQL, [dbn])
    _, attend_rows = _safe_query(db, SCHOOL_ATTENDANCE_SQL, [dbn])
    _, class_rows = _safe_query(db, SCHOOL_CLASS_SIZE_SQL, [dbn])
    _, survey_rows = _safe_query(db, SCHOOL_SURVEY_SQL, [dbn])
    _, safety_rows = _safe_query(db, SCHOOL_SAFETY_DETAIL_SQL, [dbn])
    _, regents_rows = _safe_query(db, SCHOOL_REGENTS_SQL, [dbn])
    _, cafe_rows = _safe_query(db, SCHOOL_CAFETERIA_SQL, [dbn])
    _, shsat_rows = _safe_query(db, SCHOOL_SPECIALIZED_HS_SQL, [dbn])
    _, discharge_rows = _safe_query(db, SCHOOL_DISCHARGE_SQL, [dbn])

    # Need at least something to report
    if not dir_rows and not perf_rows and not ela_rows and not demo_rows and not safety_rows:
        raise ToolError(f"No school found for DBN {dbn}. Use school_search(name) to find the correct DBN.")

    lines = [f"SCHOOL REPORT — {dbn}", "=" * 55]

    # --- Directory (existing) ---
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

    # --- Safety/Location (NEW — fallback directory if schools_directory missing) ---
    if safety_rows:
        s = safety_rows[0]
        # s: school_year, location_name, address, borough_name, postcode, register, major_n, ...
        if not dir_rows:
            lines.append(f"\n{s[1]}")
            lines.append(f"  {s[2]}, {s[3]} {s[4]}")
        else:
            lines.append(f"\n  Address: {s[2]}, {s[3]} {s[4]}")
        if s[5]:
            lines.append(f"  Register: {s[5]} students")

    # --- Demographics (NEW) ---
    if demo_rows:
        d = demo_rows[0]
        # d: year, total_enrollment, female, female_1, male, male_1, asian, asian_1, ...
        enrollment = d[1] or '?'
        lines.append(f"\nDEMOGRAPHICS ({d[0]})")
        lines.append(f"  Enrollment: {enrollment}")
        # Gender
        if d[2] and d[3]:
            lines.append(f"  Gender: {d[3]}% female, {d[5]}% male")
        # Race
        race_parts = []
        for label, pct_idx in [("Asian", 7), ("Black", 9), ("Hispanic", 11), ("Multi-racial", 13), ("White", 15)]:
            pct = d[pct_idx] if len(d) > pct_idx and d[pct_idx] else None
            if pct:
                race_parts.append(f"{label} {pct}%")
        if race_parts:
            lines.append(f"  Race: {', '.join(race_parts)}")

    # --- Performance (existing) ---
    if perf_rows:
        lines.append("\nPERFORMANCE (DOE Framework):")
        for r in perf_rows:
            perf = float(r[1]) if r[1] else 0
            impact = float(r[2]) if r[2] else 0
            lines.append(f"  {r[0]}: Performance {perf:.2f} | Impact {impact:.2f} ({r[3]})")

    # --- ELA (existing) ---
    if ela_rows:
        lines.append("\nELA TEST RESULTS:")
        for r in ela_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Math (existing) ---
    if math_rows:
        lines.append("\nMATH TEST RESULTS:")
        for r in math_rows:
            tested = r[2] or '?'
            score = r[3] or 'N/A'
            proficient = r[4] or r[5] or r[6] or 'N/A'
            lines.append(f"  {r[0]}: {tested} tested, mean {score}, proficient {proficient}")

    # --- Regents (NEW — high schools) ---
    if regents_rows:
        lines.append("\nREGENTS EXAMS:")
        current_year = None
        for r in regents_rows:
            exam, year, tested, mean, pct65, pct80 = r
            if year != current_year:
                current_year = year
                lines.append(f"  {year}:")
            lines.append(f"    {exam}: {tested} tested, mean {mean}, ≥65: {pct65 or '?'}%, ≥80: {pct80 or '?'}%")

    # --- Attendance (NEW) ---
    if attend_rows:
        lines.append("\nATTENDANCE:")
        for r in attend_rows:
            year, name, grade, attend_rate, chronic_pct = r
            lines.append(f"  {year}: {attend_rate}% attendance, {chronic_pct}% chronically absent")

    # --- Class Size (NEW) ---
    if class_rows:
        lines.append("\nCLASS SIZE:")
        for r in class_rows:
            grade, prog, avg, mn, mx, students, classes = r
            avg_str = f"{avg}" if avg else "?"
            lines.append(f"  {grade} ({prog}): avg {avg_str} students, {classes or '?'} classes")

    # --- Surveys (NEW) ---
    if survey_rows:
        s = survey_rows[0]
        # s: name, p_teachers..p_trust, parent_rate, t_teachers..t_trust, teacher_rate, s_teachers..s_trust, student_rate
        lines.append("\nSURVEY SCORES:")
        lines.append(f"  Parents (response rate: {s[6] or '?'}%):")
        lines.append(f"    Teachers: {s[1] or '?'} | Leadership: {s[2] or '?'} | Rigor: {s[3] or '?'} | Environment: {s[4] or '?'} | Trust: {s[5] or '?'}")
        if s[7]:
            lines.append(f"  Teachers (response rate: {s[12] or '?'}%):")
            lines.append(f"    Teachers: {s[7] or '?'} | Leadership: {s[8] or '?'} | Rigor: {s[9] or '?'} | Environment: {s[10] or '?'} | Trust: {s[11] or '?'}")
        if s[13]:
            lines.append(f"  Students (response rate: {s[18] or '?'}%):")
            lines.append(f"    Teachers: {s[13] or '?'} | Leadership: {s[14] or '?'} | Rigor: {s[15] or '?'} | Environment: {s[16] or '?'} | Trust: {s[17] or '?'}")

    # --- Safety incidents (NEW) ---
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

    # --- Cafeteria inspections (NEW) ---
    if cafe_rows:
        lines.append("\nCAFETERIA INSPECTIONS:")
        for r in cafe_rows[:5]:
            date, level, code, desc = r
            lines.append(f"  {date or '?'}: [{level or '?'}] {desc or code or 'N/A'}")

    # --- SHSAT feeder data (NEW) ---
    if shsat_rows:
        lines.append("\nSPECIALIZED HS TEST (SHSAT) RESULTS:")
        for r in shsat_rows:
            year, testers, offers, discovery = r
            lines.append(f"  {year}: {testers} testers, {offers} offers" + (f", {discovery} discovery" if discovery and discovery != '0' else ""))

    # --- Composite quality (existing) ---
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
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): enhance school_report with demographics, attendance, safety, surveys, Regents, cafeteria"
```

---

## Task 3: `school_compare` — Side-by-Side School Comparison

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after school_search)

- [ ] **Step 1: Add SQL constants for school_compare**

```python
# ---------------------------------------------------------------------------
# School Compare
# ---------------------------------------------------------------------------

SCHOOL_COMPARE_SCORES_SQL = """
SELECT
    d.dbn,
    d.school_name,
    TRY_CAST(d.total_enrollment AS INT) AS enrollment,
    -- Latest ELA
    (SELECT COALESCE(level_3_4_1, level_3_4)
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.ela_results
     WHERE geographic_subdivision = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS ela_mean,
    -- Latest Math
    (SELECT COALESCE(pct_level_3_and_4, num_level_3_and_4)
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS math_proficient_pct,
    (SELECT mean_scale_score
     FROM lake.education.math_results
     WHERE geographic_division = d.dbn AND report_category = 'School' AND grade = 'All Grades'
     -- NOTE: math_results uses geographic_division (not geographic_subdivision like ela_results)
     ORDER BY year DESC LIMIT 1) AS math_mean,
    -- Attendance
    (SELECT attendance
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS attendance_pct,
    (SELECT chronically_absent_1
     FROM lake.education.chronic_absenteeism
     WHERE dbn = d.dbn AND grade = 'All Grades'
     ORDER BY year DESC LIMIT 1) AS chronic_absent_pct,
    -- Survey trust
    (SELECT trust_score
     FROM lake.education.survey_parents
     WHERE dbn = d.dbn LIMIT 1) AS parent_trust,
    -- Safety
    (SELECT COALESCE(TRY_CAST(major_n AS INT), 0) + COALESCE(TRY_CAST(vio_n AS INT), 0)
     FROM lake.education.school_safety
     WHERE dbn = d.dbn
     ORDER BY school_year DESC LIMIT 1) AS safety_incidents,
    -- Class size
    (SELECT ROUND(AVG(TRY_CAST(average_class_size AS DOUBLE)), 1)
     FROM lake.education.class_size
     WHERE dbn = d.dbn) AS avg_class_size,
    -- Demographics
    d.asian_1 AS asian_pct,
    d.black_1 AS black_pct,
    d.hispanic_1 AS hispanic_pct,
    d.white_1 AS white_pct,
    s.postcode AS zipcode
FROM lake.education.demographics_2020 d
LEFT JOIN lake.education.school_safety s ON d.dbn = s.dbn
WHERE d.dbn IN ({placeholders})
ORDER BY d.dbn
"""
```

- [ ] **Step 2: Add the school_compare tool function**

```python
@mcp.tool(annotations=READONLY, tags={"education"})
def school_compare(
    dbns: Annotated[list[str], Field(description="2-7 school DBNs to compare. Example: ['02M475', '02M545', '13K330']")],
    ctx: Context,
) -> ToolResult:
    """Compare 2-7 NYC public schools side-by-side across test scores, attendance, safety, class size, demographics, and parent trust. Use school_search(query) first to find DBNs. Example: school_compare(['02M475', '02M545']). For a single school deep-dive, use school_report(dbn). For district-level comparison, use district_report(district)."""
    if len(dbns) < 2:
        raise ToolError("Provide at least 2 DBNs to compare. Use school_search(query) to find DBNs.")
    if len(dbns) > 7:
        raise ToolError("Maximum 7 schools per comparison.")

    dbns = [d.strip().upper() for d in dbns]
    for d in dbns:
        if not re.match(r'^[A-Z0-9]{5,6}$', d):
            raise ToolError(f"Invalid DBN format: '{d}'. Expected 5-6 alphanumeric chars like 02M001.")
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    placeholders = ",".join(["?"] * len(dbns))
    sql = SCHOOL_COMPARE_SCORES_SQL.replace("{placeholders}", placeholders)
    _, rows = _safe_query(db, sql, dbns)

    if not rows:
        raise ToolError(f"No data found for DBNs: {', '.join(dbns)}. Verify with school_search().")

    lines = [f"SCHOOL COMPARISON — {len(rows)} schools", "=" * 60]

    # Header row
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
            v = r[idx] if idx < len(r) and r[idx] is not None else "—"
            try:
                vals.append(fmt.format(v))
            except (ValueError, TypeError):
                vals.append(str(v) if v != "—" else "—")
        lines.append(f"{label:<25} | " + " | ".join(f"{v:<25}" for v in vals))

    elapsed = round((time.time() - t0) * 1000)
    lines.append(f"\n({elapsed}ms)")

    return ToolResult(
        content="\n".join(lines),
        structured_content={"dbns": dbns, "schools": [{"dbn": r[0], "name": r[1], "enrollment": r[2]} for r in rows]},
        meta={"dbn_count": len(dbns), "query_time_ms": elapsed},
    )
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add school_compare tool — side-by-side school comparison"
```

---

## Task 4: `district_report` — District-Level Aggregation

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py` (insert after school_compare)

- [ ] **Step 1: Add SQL constants for district_report**

```python
# ---------------------------------------------------------------------------
# District Report
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
    -- Demographics averages
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
```

- [ ] **Step 2: Add the district_report tool function**

```python
@mcp.tool(annotations=READONLY, tags={"education"})
def district_report(
    district: Annotated[str, Field(description="NYC school district number (1-32, 75, 79). Example: '2' or '02'")],
    ctx: Context,
) -> ToolResult:
    """Aggregate education report for a NYC school district — school count, enrollment, demographics, average test scores, attendance, and safety across all schools in the district. Districts 1-32 are geographic, 75 is citywide special education, 79 is alternative schools. Example: district_report('2'). For individual school detail, use school_report(dbn). To find schools in a district, use school_search(district_number)."""
    district = district.strip().lstrip('0') or '0'
    if not district.isdigit() or int(district) < 1 or int(district) > 79:
        raise ToolError("District must be 1-32, 75, or 79.")

    padded = district.zfill(2)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    _, agg_rows = _safe_query(db, DISTRICT_AGGREGATE_SQL, [padded])
    _, test_rows = _safe_query(db, DISTRICT_TEST_SCORES_SQL, [padded, padded])
    _, attend_rows = _safe_query(db, DISTRICT_ATTENDANCE_SQL, [padded])
    _, safety_rows = _safe_query(db, DISTRICT_SAFETY_SQL, [padded])
    _, school_rows = _safe_query(db, DISTRICT_SCHOOLS_SQL, [padded])

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
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add district_report tool — district-level education aggregation"
```

---

## Task 5: Update Tool Descriptions for Cross-References

Update existing tool descriptions to reference the new education tools.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Update neighborhood_portrait docstring**

Add reference to education tools. Find the docstring at line 5798 and append:
```
For education data by ZIP, use school_search(zipcode) to find schools in the area.
```

- [ ] **Step 2: Update MCP server instructions**

If there's a routing comment/instructions block at the top of the file, add education routing:
```
• SCHOOL by name/ZIP → school_search, then school_report
• DISTRICT by number → district_report
```

- [ ] **Step 3: Commit**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "docs(mcp): add education tool cross-references to routing and docstrings"
```

---

## Task 6: Deploy and Smoke Test

- [ ] **Step 1: Deploy to server**

```bash
cd ~/Desktop/dagster-pipeline
./deploy.sh  # or: rsync infra/duckdb-server/mcp_server.py to server + restart
```

- [ ] **Step 2: Smoke test school_search**

Via MCP: `school_search("Stuyvesant")` — expect Stuyvesant High School with DBN 02M475
Via MCP: `school_search("10003")` — expect schools in East Village
Via MCP: `school_search("02")` — expect District 2 schools

- [ ] **Step 3: Smoke test enhanced school_report**

Via MCP: `school_report("02M475")` — expect full report with demographics, safety, surveys, Regents, cafeteria

- [ ] **Step 4: Smoke test school_compare**

Via MCP: `school_compare(["02M475", "10X445", "13K430"])` — expect side-by-side table

- [ ] **Step 5: Smoke test district_report**

Via MCP: `district_report("2")` — expect District 2 aggregate with school list

- [ ] **Step 6: Commit final state**

```bash
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): education tools complete — search, compare, district, enhanced report"
```
