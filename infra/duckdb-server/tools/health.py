"""health() super tool — public health, COVID, facilities, inspections, environmental health.

Views:
  full          → CDC PLACES community health profile + key indicators
  covid         → COVID cases, outcomes, wastewater surveillance by ZIP
  facilities    → Hospitals, clinics, medicaid providers nearby
  inspections   → Cooling tower, drinking water, rodent inspection results
  environmental → Lead exposure, asthma, air quality health impacts
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

_ZIP_PATTERN = ZIP_PATTERN
_COORDS_PATTERN = COORDS_PATTERN

# ---------------------------------------------------------------------------
# ZIP resolution helpers
# ---------------------------------------------------------------------------


def _resolve_zip(pool: object, location: str) -> str:
    """Resolve location input to a 5-digit ZIP code.

    Accepts: ZIP code, neighborhood name, or lat/lng coordinates.
    """
    location = location.strip()

    if _ZIP_PATTERN.match(location):
        return location

    # Lat/lng → nearest ZIP via 311 data
    m = _COORDS_PATTERN.match(location)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        cols, rows = safe_query(pool, """
            SELECT incident_zip, COUNT(*) AS cnt
            FROM lake.social_services.n311_service_requests
            WHERE ABS(TRY_CAST(latitude AS DOUBLE) - ?) < 0.005
              AND ABS(TRY_CAST(longitude AS DOUBLE) - ?) < 0.005
              AND incident_zip IS NOT NULL
              AND incident_zip != ''
            GROUP BY incident_zip
            ORDER BY cnt DESC
            LIMIT 1
        """, [lat, lng])
        if rows:
            return str(rows[0][0])
        raise ToolError(f"No ZIP code found near coordinates {lat},{lng}.")

    # Neighborhood name → ZIP via 311 crosswalk
    cols, rows = safe_query(pool, """
        SELECT incident_zip, COUNT(*) AS cnt
        FROM lake.social_services.n311_service_requests
        WHERE (city ILIKE ? OR borough ILIKE ?)
          AND incident_zip IS NOT NULL
          AND incident_zip != ''
        GROUP BY incident_zip
        ORDER BY cnt DESC
        LIMIT 1
    """, [location, location])
    if rows:
        return str(rows[0][0])

    raise ToolError(
        f"Could not resolve '{location}' to a ZIP code. "
        "Try a 5-digit ZIP like '10003' or coordinates like '40.7128,-74.0060'."
    )


# ---------------------------------------------------------------------------
# SQL — full (CDC PLACES community health profile)
# ---------------------------------------------------------------------------

CDC_PLACES_SQL = """
SELECT measure, data_value, totalpopulation, short_question_text
FROM lake.health.cdc_places
WHERE TRY_CAST(locationid AS VARCHAR) = ?
ORDER BY measure
LIMIT 30
"""

CDC_PLACES_BY_NAME_SQL = """
SELECT measure, data_value, totalpopulation, short_question_text, locationname
FROM lake.health.cdc_places
WHERE locationname ILIKE '%' || ? || '%'
ORDER BY measure
LIMIT 30
"""

HIV_AIDS_ANNUAL_SQL = """
SELECT year, borough, uhf, gender, age, race_ethnicity,
       hiv_diagnoses, hiv_diagnosis_rate,
       aids_diagnoses, aids_diagnosis_rate
FROM lake.health.hiv_aids_annual
WHERE uhf ILIKE '%' || ? || '%'
   OR borough ILIKE '%' || ? || '%'
ORDER BY year DESC
LIMIT 20
"""

LEADING_CAUSES_SQL = """
SELECT year, leading_cause, sex, race_ethnicity, deaths, death_rate
FROM lake.health.leading_causes_of_death
ORDER BY TRY_CAST(year AS INT) DESC, TRY_CAST(deaths AS INT) DESC
LIMIT 20
"""

ED_FLU_SQL = """
SELECT extract_date, zip, total_ed_visits, ili_pne_visits, ili_pne_admissions
FROM lake.health.ed_flu_visits
WHERE zip = ?
ORDER BY TRY_CAST(extract_date AS DATE) DESC
LIMIT 20
"""

SPARCS_DISCHARGES_SQL = """
SELECT facility_name, age_group, gender, race, ethnicity,
       type_of_admission, apr_severity_of_illness_description,
       apr_medical_surgical_description, total_charges, total_costs
FROM lake.health.sparcs_discharges_{year}
WHERE zip_code_3_digits = LEFT(?, 3)
ORDER BY TRY_CAST(total_charges AS DOUBLE) DESC
LIMIT 20
"""

COMMUNITY_HEALTH_SURVEY_SQL = """
SELECT name, geo_type, geo_type_desc,
       number, percent, start_date, end_date
FROM lake.environment.community_health_survey
WHERE geo_type_desc ILIKE '%' || ? || '%'
   OR name ILIKE '%' || ? || '%'
ORDER BY name
LIMIT 20
"""

PREGNANCY_MORTALITY_SQL = """
SELECT year, related, race_ethnicity,
       borough, deaths, death_rate, death_rate_lower_ci, death_rate_upper_ci
FROM lake.health.pregnancy_mortality
ORDER BY TRY_CAST(year AS INT) DESC
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — covid
# ---------------------------------------------------------------------------

COVID_BY_ZIP_SQL = """
SELECT modified_zcta, neighborhood_name, borough_group,
       covid_case_count, covid_case_rate,
       covid_death_count, covid_death_rate,
       percent_positive, total_covid_tests
FROM lake.health.covid_by_zip
WHERE TRY_CAST(modified_zcta AS VARCHAR) = ?
LIMIT 20
"""

COVID_OUTCOMES_SQL = """
SELECT date_of_interest, case_count, hospitalized_count, death_count,
       case_count_7day_avg, hosp_count_7day_avg, death_count_7day_avg
FROM lake.health.covid_outcomes
ORDER BY TRY_CAST(date_of_interest AS DATE) DESC
LIMIT 20
"""

WASTEWATER_SQL = """
SELECT sample_date, wrrf_abbreviation, wrrf_name,
       concentration_sars_cov_2, test_type, annotation
FROM lake.health.wastewater_sarscov2
ORDER BY TRY_CAST(sample_date AS DATE) DESC
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — facilities
# ---------------------------------------------------------------------------

HEALTH_FACILITIES_SQL = """
SELECT facility_name, facility_type, description,
       street_address, city, state, zip_code, phone
FROM lake.health.health_facilities
WHERE TRY_CAST(zip_code AS VARCHAR) = ?
ORDER BY facility_name
LIMIT 20
"""

MEDICAID_PROVIDERS_SQL = """
SELECT provider_name, provider_specialty, facility_name,
       address, city, state, zip
FROM lake.health.medicaid_providers
WHERE TRY_CAST(zip AS VARCHAR) = ?
ORDER BY provider_name
LIMIT 20
"""

EPA_FACILITIES_SQL = """
SELECT facility_name, street_address, city_name, zip_code,
       primary_name, caa_flag, cwa_flag, rcra_flag, sdwa_flag
FROM lake.federal.epa_echo_facilities
WHERE TRY_CAST(zip_code AS VARCHAR) = ?
ORDER BY facility_name
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — inspections
# ---------------------------------------------------------------------------

COOLING_TOWER_SQL = """
SELECT building_name, street_address, city, zip,
       inspection_date, deficiency_status, findings
FROM lake.health.cooling_tower_inspections
WHERE TRY_CAST(zip AS VARCHAR) = ?
ORDER BY TRY_CAST(inspection_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

DRINKING_WATER_TANKS_SQL = """
SELECT site_name, street_name, borough, zip_code,
       deq_status, source_capacity_gal, permit_status
FROM lake.health.drinking_water_tanks
WHERE TRY_CAST(zip_code AS VARCHAR) = ?
ORDER BY site_name
LIMIT 20
"""

DRINKING_WATER_INSPECTIONS_SQL = """
SELECT site_name, street_name, borough, zip_code,
       inspection_date, inspection_result, violation_type
FROM lake.health.drinking_water_tank_inspections
WHERE TRY_CAST(zip_code AS VARCHAR) = ?
ORDER BY TRY_CAST(inspection_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

RODENT_INSPECTIONS_SQL = """
SELECT inspection_type, result, approved_date, zip_code,
       borough_code, community_district, concession_name
FROM lake.health.rodent_inspections
WHERE zip_code = ?
ORDER BY TRY_CAST(approved_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

RODENT_SUMMARY_SQL = """
SELECT result, COUNT(*) AS cnt
FROM lake.health.rodent_inspections
WHERE zip_code = ?
GROUP BY result
ORDER BY cnt DESC
"""

# ---------------------------------------------------------------------------
# SQL — environmental
# ---------------------------------------------------------------------------

ASTHMA_ED_SQL = """
SELECT geo_type, geo_type_name, time_period,
       age_adjusted_rate_per_10_000, estimated_annual_rate_per_10_000,
       number, name
FROM lake.environment.asthma_ed
WHERE geo_type_name ILIKE '%' || ? || '%'
   OR name ILIKE '%' || ? || '%'
ORDER BY time_period DESC
LIMIT 20
"""

LEAD_CHILDREN_SQL = """
SELECT geo_area_name, time_period, geo_type,
       bll5_estimated_number, bll5_rate_per_1000_tested,
       bll10_estimated_number, bll10_rate_per_1000_tested,
       total_tested, total_children
FROM lake.environment.lead_children
WHERE geo_area_name ILIKE '%' || ? || '%'
ORDER BY time_period DESC
LIMIT 20
"""

BEACH_WATER_SQL = """
SELECT sample_date, beach_name, sample_location,
       enterococcus_results, exceedance
FROM lake.environment.beach_water_samples
ORDER BY TRY_CAST(sample_date AS DATE) DESC
LIMIT 20
"""

AIR_QUALITY_SQL = """
SELECT name, geo_type_name, time_period,
       data_value, measure, geo_place_name
FROM lake.environment.air_quality
WHERE geo_type_name ILIKE '%' || ? || '%'
   OR geo_place_name ILIKE '%' || ? || '%'
ORDER BY time_period DESC
LIMIT 20
"""


# ---------------------------------------------------------------------------
# View dispatch functions
# ---------------------------------------------------------------------------


def _view_full(pool: object, zipcode: str, location: str) -> ToolResult:
    """Community health profile — CDC PLACES + key indicators."""
    t0 = time.time()
    sections = []

    results = parallel_queries(pool, [
        ("cdc_zip",    CDC_PLACES_SQL,             [zipcode]),
        ("cdc_name",   CDC_PLACES_BY_NAME_SQL,      [location]),
        ("hiv",        HIV_AIDS_ANNUAL_SQL,          [location, location]),
        ("deaths",     LEADING_CAUSES_SQL,           None),
        ("flu",        ED_FLU_SQL,                   [zipcode]),
        ("sparcs2024", SPARCS_DISCHARGES_SQL.format(year="2024"), [zipcode]),
        ("sparcs2023", SPARCS_DISCHARGES_SQL.format(year="2023"), [zipcode]),
        ("sparcs2022", SPARCS_DISCHARGES_SQL.format(year="2022"), [zipcode]),
        ("chs",        COMMUNITY_HEALTH_SURVEY_SQL,  [location, location]),
        ("pregnancy",  PREGNANCY_MORTALITY_SQL,      None),
    ])

    # CDC PLACES — prefer ZIP match, fall back to name
    cdc_cols, cdc_rows = results["cdc_zip"]
    if not cdc_rows:
        cdc_cols, cdc_rows = results["cdc_name"]
    if cdc_rows:
        sections.append("### CDC PLACES Health Metrics\n" + format_text_table(cdc_cols, cdc_rows))

    hiv_cols, hiv_rows = results["hiv"]
    if hiv_rows:
        sections.append("### HIV/AIDS Diagnoses\n" + format_text_table(hiv_cols, hiv_rows))

    death_cols, death_rows = results["deaths"]
    if death_rows:
        sections.append("### Leading Causes of Death (Citywide)\n" + format_text_table(death_cols, death_rows))

    flu_cols, flu_rows = results["flu"]
    if flu_rows:
        sections.append("### Emergency Dept Flu/Respiratory Visits\n" + format_text_table(flu_cols, flu_rows))

    # SPARCS — use most recent year that has data
    for year, key in [("2024", "sparcs2024"), ("2023", "sparcs2023"), ("2022", "sparcs2022")]:
        sparcs_cols, sparcs_rows = results[key]
        if sparcs_rows:
            sections.append(f"### Hospital Discharges ({year})\n" + format_text_table(sparcs_cols, sparcs_rows))
            break

    chs_cols, chs_rows = results["chs"]
    if chs_rows:
        sections.append("### Community Health Survey\n" + format_text_table(chs_cols, chs_rows))

    preg_cols, preg_rows = results["pregnancy"]
    if preg_rows:
        sections.append("### Pregnancy-Related Mortality (Citywide)\n" + format_text_table(preg_cols, preg_rows))

    elapsed = time.time() - t0
    if not sections:
        raise ToolError(f"No health data found for '{location}' (ZIP {zipcode}).")

    summary = f"## Health Profile: {location} (ZIP {zipcode}) — {len(sections)} sections, {elapsed:.1f}s"
    return ToolResult(
        content=summary + "\n\n" + "\n\n".join(sections),
        meta={"zip": zipcode, "sections": len(sections), "elapsed_s": round(elapsed, 2)},
    )


def _view_covid(pool: object, zipcode: str, location: str) -> ToolResult:
    """COVID cases, outcomes, wastewater surveillance."""
    t0 = time.time()
    sections = []

    # COVID by ZIP
    cols, rows = safe_query(pool, COVID_BY_ZIP_SQL, [zipcode])
    if rows:
        sections.append("### COVID-19 by ZIP\n" + format_text_table(cols, rows))

    # COVID outcomes (citywide time series)
    cols, rows = safe_query(pool, COVID_OUTCOMES_SQL)
    if rows:
        sections.append("### COVID-19 Outcomes (Citywide, Recent)\n" + format_text_table(cols, rows))

    # Wastewater surveillance
    cols, rows = safe_query(pool, WASTEWATER_SQL)
    if rows:
        sections.append("### Wastewater SARS-CoV-2 Surveillance\n" + format_text_table(cols, rows))

    elapsed = time.time() - t0
    if not sections:
        raise ToolError(f"No COVID data found for '{location}' (ZIP {zipcode}).")

    summary = f"## COVID-19 Data: {location} (ZIP {zipcode}) — {len(sections)} sections, {elapsed:.1f}s"
    return ToolResult(
        content=summary + "\n\n" + "\n\n".join(sections),
        meta={"zip": zipcode, "sections": len(sections), "elapsed_s": round(elapsed, 2)},
    )


def _view_facilities(pool: object, zipcode: str, location: str) -> ToolResult:
    """Hospitals, clinics, medicaid providers nearby."""
    t0 = time.time()
    sections = []

    # Health facilities
    cols, rows = safe_query(pool, HEALTH_FACILITIES_SQL, [zipcode])
    if rows:
        sections.append("### Health Facilities\n" + format_text_table(cols, rows))

    # Medicaid providers
    cols, rows = safe_query(pool, MEDICAID_PROVIDERS_SQL, [zipcode])
    if rows:
        sections.append("### Medicaid Providers\n" + format_text_table(cols, rows))

    # EPA-regulated facilities (environmental health)
    cols, rows = safe_query(pool, EPA_FACILITIES_SQL, [zipcode])
    if rows:
        sections.append("### EPA-Regulated Facilities\n" + format_text_table(cols, rows))

    elapsed = time.time() - t0
    if not sections:
        raise ToolError(f"No health facilities found for '{location}' (ZIP {zipcode}).")

    summary = f"## Health Facilities: {location} (ZIP {zipcode}) — {len(sections)} sections, {elapsed:.1f}s"
    return ToolResult(
        content=summary + "\n\n" + "\n\n".join(sections),
        meta={"zip": zipcode, "sections": len(sections), "elapsed_s": round(elapsed, 2)},
    )


def _view_inspections(pool: object, zipcode: str, location: str) -> ToolResult:
    """Cooling tower, drinking water, rodent inspections."""
    t0 = time.time()
    sections = []

    # Cooling tower inspections
    cols, rows = safe_query(pool, COOLING_TOWER_SQL, [zipcode])
    if rows:
        sections.append("### Cooling Tower Inspections\n" + format_text_table(cols, rows))

    # Drinking water tanks
    cols, rows = safe_query(pool, DRINKING_WATER_TANKS_SQL, [zipcode])
    if rows:
        sections.append("### Drinking Water Tanks\n" + format_text_table(cols, rows))

    # Drinking water tank inspections
    cols, rows = safe_query(pool, DRINKING_WATER_INSPECTIONS_SQL, [zipcode])
    if rows:
        sections.append("### Drinking Water Tank Inspections\n" + format_text_table(cols, rows))

    # Rodent inspections — summary first
    sum_cols, sum_rows = safe_query(pool, RODENT_SUMMARY_SQL, [zipcode])
    if sum_rows:
        sections.append("### Rodent Inspection Summary\n" + format_text_table(sum_cols, sum_rows))

    # Rodent inspections — recent detail
    cols, rows = safe_query(pool, RODENT_INSPECTIONS_SQL, [zipcode])
    if rows:
        sections.append("### Recent Rodent Inspections\n" + format_text_table(cols, rows))

    elapsed = time.time() - t0
    if not sections:
        raise ToolError(f"No inspection data found for '{location}' (ZIP {zipcode}).")

    summary = f"## Health Inspections: {location} (ZIP {zipcode}) — {len(sections)} sections, {elapsed:.1f}s"
    return ToolResult(
        content=summary + "\n\n" + "\n\n".join(sections),
        meta={"zip": zipcode, "sections": len(sections), "elapsed_s": round(elapsed, 2)},
    )


def _view_environmental(pool: object, zipcode: str, location: str) -> ToolResult:
    """Lead exposure, asthma, air quality health impacts."""
    t0 = time.time()
    sections = []

    # Asthma ED visits
    cols, rows = safe_query(pool, ASTHMA_ED_SQL, [location, location])
    if rows:
        sections.append("### Asthma Emergency Dept Visits\n" + format_text_table(cols, rows))

    # Lead in children
    cols, rows = safe_query(pool, LEAD_CHILDREN_SQL, [location])
    if rows:
        sections.append("### Childhood Lead Exposure\n" + format_text_table(cols, rows))

    # Beach water quality
    cols, rows = safe_query(pool, BEACH_WATER_SQL)
    if rows:
        sections.append("### Beach Water Quality (Recent)\n" + format_text_table(cols, rows))

    # Air quality
    cols, rows = safe_query(pool, AIR_QUALITY_SQL, [location, location])
    if rows:
        sections.append("### Air Quality\n" + format_text_table(cols, rows))

    # CDC PLACES — asthma/mental health subset
    cdc_cols, cdc_rows = safe_query(pool, """
        SELECT measure, data_value, locationname
        FROM lake.health.cdc_places
        WHERE TRY_CAST(locationid AS VARCHAR) = ?
          AND measure IN (
              'Current asthma among adults',
              'Mental health not good for >=14 days among adults',
              'COPD among adults',
              'Cancer (excluding skin cancer) among adults'
          )
        ORDER BY measure
        LIMIT 10
    """, [zipcode])
    if cdc_rows:
        sections.append("### CDC Health Indicators (Environmental)\n" + format_text_table(cdc_cols, cdc_rows))

    elapsed = time.time() - t0
    if not sections:
        raise ToolError(f"No environmental health data found for '{location}' (ZIP {zipcode}).")

    summary = f"## Environmental Health: {location} (ZIP {zipcode}) — {len(sections)} sections, {elapsed:.1f}s"
    return ToolResult(
        content=summary + "\n\n" + "\n\n".join(sections),
        meta={"zip": zipcode, "sections": len(sections), "elapsed_s": round(elapsed, 2)},
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "full": _view_full,
    "covid": _view_covid,
    "facilities": _view_facilities,
    "inspections": _view_inspections,
    "environmental": _view_environmental,
}


def health(
    location: Annotated[str, Field(
        description="ZIP code, neighborhood name, or lat/lng coordinates, e.g. '10003', 'East Harlem', '40.7128,-74.0060'",
        examples=["10003", "10456", "East Harlem", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "covid", "facilities", "inspections", "environmental"],
        Field(
            default="full",
            description="'full' returns community health profile with CDC PLACES metrics. 'covid' returns COVID cases, outcomes, and wastewater surveillance. 'facilities' returns hospitals, clinics, and medicaid providers nearby. 'inspections' returns cooling tower, drinking water, and rodent inspection results. 'environmental' returns lead exposure, asthma rates, and air quality impacts.",
        )
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Health data, COVID stats, hospital info, and restaurant inspections for any NYC location. Returns CDC PLACES metrics, facility locations, and environmental health indicators.

    GUIDELINES: Show the complete health profile. Use tables for health metrics and inspection data.
    Present the FULL response to the user. Do not omit any section.

    LIMITATIONS: Not for building violations (use building). Not for social services (use services).

    RETURNS: Community health indicators, COVID data, facility listings, and inspection results."""
    location_raw = location.strip()
    if not location_raw:
        raise ToolError("Location must not be empty.")

    pool = ctx.lifespan_context["pool"]
    zipcode = _resolve_zip(pool, location_raw)

    directive = "PRESENTATION: Show this complete health report. Use tables for health metrics and inspection data. Do not omit any section.\n\n"
    handler = _VIEW_DISPATCH[view]
    result = handler(pool, zipcode, location_raw)
    return ToolResult(
        content=directive + (result.content if isinstance(result.content, str) else "\n".join(str(c) for c in result.content) if result.content else ""),
        structured_content=result.structured_content,
        meta=result.meta,
    )
