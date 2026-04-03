"""civic() super tool — city government operations: contracts, permits, jobs, budget, events."""

import time
from typing import Annotated, Literal

from pydantic import Field
from fastmcp import Context
from fastmcp.tools.tool import ToolResult
from fastmcp.exceptions import ToolError

from shared.db import safe_query, parallel_queries
from shared.formatting import format_text_table
from shared.types import MAX_LLM_ROWS

# ---------------------------------------------------------------------------
# SQL — contracts view
# ---------------------------------------------------------------------------

CONTRACT_AWARDS_SQL = """
SELECT vendor_name, agency_name, short_title,
       TRY_CAST(contract_amount AS DOUBLE) AS amount,
       start_date, end_date
FROM lake.city_government.contract_awards
WHERE vendor_name ILIKE '%' || ? || '%'
   OR agency_name ILIKE '%' || ? || '%'
   OR short_title ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

DDC_VENDOR_PAYMENTS_SQL = """
SELECT vendor_name, agency, project_description,
       TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
       payment_date
FROM lake.city_government.ddc_vendor_payments
WHERE vendor_name ILIKE '%' || ? || '%'
   OR agency ILIKE '%' || ? || '%'
   OR project_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(amount_paid AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

COVID_CONTRACTS_SQL = """
SELECT vendor_name, agency_name, short_title,
       TRY_CAST(contract_amount AS DOUBLE) AS amount,
       start_date, end_date
FROM lake.city_government.covid_emergency_contracts
WHERE vendor_name ILIKE '%' || ? || '%'
   OR agency_name ILIKE '%' || ? || '%'
   OR short_title ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

USASPENDING_CONTRACTS_SQL = """
SELECT recipient_name, awarding_agency_name, award_description,
       TRY_CAST(total_obligation AS DOUBLE) AS obligation,
       period_of_performance_start_date AS start_date,
       period_of_performance_current_end_date AS end_date
FROM lake.federal.usaspending_contracts
WHERE recipient_name ILIKE '%' || ? || '%'
   OR awarding_agency_name ILIKE '%' || ? || '%'
   OR award_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(total_obligation AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

USASPENDING_GRANTS_SQL = """
SELECT recipient_name, awarding_agency_name, award_description,
       TRY_CAST(total_obligation AS DOUBLE) AS obligation,
       period_of_performance_start_date AS start_date,
       period_of_performance_current_end_date AS end_date
FROM lake.federal.usaspending_grants
WHERE recipient_name ILIKE '%' || ? || '%'
   OR awarding_agency_name ILIKE '%' || ? || '%'
   OR award_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(total_obligation AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

NYS_PROCUREMENT_STATE_SQL = """
SELECT vendor_name, contract_amount, contracting_agency, contract_description
FROM lake.financial.nys_procurement_state
WHERE vendor_name ILIKE '%' || ? || '%'
   OR contracting_agency ILIKE '%' || ? || '%'
   OR contract_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

NYS_PROCUREMENT_LOCAL_SQL = """
SELECT vendor_name, contract_amount, contracting_agency, contract_description
FROM lake.financial.nys_procurement_local
WHERE vendor_name ILIKE '%' || ? || '%'
   OR contracting_agency ILIKE '%' || ? || '%'
   OR contract_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(contract_amount AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — permits view
# ---------------------------------------------------------------------------

FILM_PERMITS_SQL = """
SELECT eventtype, parkingheld, borough, category,
       subcategoryname, country, zipcode_s,
       startdatetime, enddatetime
FROM lake.city_government.film_permits
WHERE parkingheld ILIKE '%' || ? || '%'
   OR borough ILIKE '%' || ? || '%'
   OR subcategoryname ILIKE '%' || ? || '%'
   OR category ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(startdatetime AS TIMESTAMP) DESC NULLS LAST
LIMIT 20
"""

LIQUOR_AUTHORITY_SQL = """
SELECT license_type_name, premises_name, doing_business_as,
       actual_address_of_premises, county, zone,
       license_effective_date, license_expiration_date
FROM lake.business.nys_liquor_authority
WHERE premises_name ILIKE '%' || ? || '%'
   OR doing_business_as ILIKE '%' || ? || '%'
   OR actual_address_of_premises ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(license_effective_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — jobs view
# ---------------------------------------------------------------------------

CITYWIDE_PAYROLL_SQL = """
SELECT last_name, first_name, agency_name, title_description,
       TRY_CAST(base_salary AS DOUBLE) AS salary,
       TRY_CAST(total_ot_paid AS DOUBLE) AS overtime,
       fiscal_year, work_location_borough
FROM lake.city_government.citywide_payroll
WHERE last_name ILIKE '%' || ? || '%'
   OR agency_name ILIKE '%' || ? || '%'
   OR title_description ILIKE '%' || ? || '%'
ORDER BY fiscal_year DESC, TRY_CAST(base_salary AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

CIVIL_SERVICE_ACTIVE_SQL = """
SELECT first_name, last_name, list_title_desc, exam_no,
       list_no, adj_fa, list_agency_desc, established_date
FROM lake.city_government.civil_service_active
WHERE last_name ILIKE '%' || ? || '%'
   OR list_title_desc ILIKE '%' || ? || '%'
   OR list_agency_desc ILIKE '%' || ? || '%'
ORDER BY established_date DESC NULLS LAST
LIMIT 20
"""

CIVIL_LIST_SQL = """
SELECT first_name, last_name, agency_name, title_description,
       TRY_CAST(salary AS DOUBLE) AS salary, pay_basis
FROM lake.city_government.civil_list
WHERE last_name ILIKE '%' || ? || '%'
   OR agency_name ILIKE '%' || ? || '%'
   OR title_description ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(salary AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

CIVIL_SERVICE_TITLES_SQL = """
SELECT title_code_no, title_description, salary_range_from,
       salary_range_to, title_classification
FROM lake.city_government.civil_service_titles
WHERE title_description ILIKE '%' || ? || '%'
   OR title_classification ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(salary_range_to AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

NYC_JOBS_SQL = """
SELECT business_title, agency, job_category, salary_range_from,
       salary_range_to, posting_date, posting_type
FROM lake.city_government.nyc_jobs
WHERE business_title ILIKE '%' || ? || '%'
   OR agency ILIKE '%' || ? || '%'
   OR job_category ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(posting_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — budget view
# ---------------------------------------------------------------------------

REVENUE_BUDGET_SQL = """
SELECT agency_name, revenue_category, revenue_source,
       TRY_CAST(adopted_budget AS DOUBLE) AS adopted,
       TRY_CAST(current_modified_budget AS DOUBLE) AS modified,
       fiscal_year
FROM lake.city_government.revenue_budget
WHERE agency_name ILIKE '%' || ? || '%'
   OR revenue_category ILIKE '%' || ? || '%'
   OR revenue_source ILIKE '%' || ? || '%'
ORDER BY fiscal_year DESC, TRY_CAST(adopted_budget AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

NYS_IDA_PROJECTS_SQL = """
SELECT project_name, ida_name, county, municipality,
       TRY_CAST(total_project_amount AS DOUBLE) AS project_amount,
       project_start_date
FROM lake.business.nys_ida_projects
WHERE project_name ILIKE '%' || ? || '%'
   OR ida_name ILIKE '%' || ? || '%'
   OR municipality ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(total_project_amount AS DOUBLE) DESC NULLS LAST
LIMIT 20
"""

# ---------------------------------------------------------------------------
# SQL — events view
# ---------------------------------------------------------------------------

PERMITTED_EVENTS_SQL = """
SELECT event_name, event_type, event_borough, event_location,
       start_date_time, end_date_time
FROM lake.city_government.permitted_events
WHERE event_name ILIKE '%' || ? || '%'
   OR event_type ILIKE '%' || ? || '%'
   OR event_borough ILIKE '%' || ? || '%'
   OR event_location ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(start_date_time AS TIMESTAMP) DESC NULLS LAST
LIMIT 20
"""

CITY_RECORD_SQL = """
SELECT agency, section_name, notice_type, short_title,
       start_date, end_date
FROM lake.city_government.city_record
WHERE agency ILIKE '%' || ? || '%'
   OR section_name ILIKE '%' || ? || '%'
   OR short_title ILIKE '%' || ? || '%'
   OR notice_type ILIKE '%' || ? || '%'
ORDER BY TRY_CAST(start_date AS DATE) DESC NULLS LAST
LIMIT 20
"""

# ---------------------------------------------------------------------------
# View dispatchers
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "contracts": "_view_contracts",
    "permits": "_view_permits",
    "jobs": "_view_jobs",
    "budget": "_view_budget",
    "events": "_view_events",
}


def _run_queries(pool: object, q: str, queries: list[tuple[str, str, int]]) -> list[tuple[str, list, list]]:
    """Run a list of (label, sql, param_count) queries in parallel, return [(label, cols, rows)]."""
    pq_input = [(label, sql, [q] * param_count) for label, sql, param_count in queries]
    raw = parallel_queries(pool, pq_input)
    # Preserve original order, drop empty results
    return [
        (label, raw[label][0], raw[label][1])
        for label, _, _ in queries
        if raw[label][1]
    ]


def _build_result(query: str, view: str, sections: list[tuple[str, list, list]], elapsed: float) -> ToolResult:
    """Build a ToolResult from multiple query sections."""
    if not sections:
        return ToolResult(content=f"No {view} data found for '{query}'. Try a broader search term.")

    lines = [f"CIVIC — {view.upper()} — '{query}'", f"({elapsed:.1f}s)", "=" * 50]
    structured: dict = {}

    for label, cols, rows in sections:
        lines.append(f"\n## {label} ({len(rows)} rows)")
        lines.append(format_text_table(cols, rows, max_rows=MAX_LLM_ROWS))
        structured[label.lower().replace(" ", "_")] = [dict(zip(cols, row)) for row in rows[:MAX_LLM_ROWS]]

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"total_sections": len(sections), "query": query, "view": view},
    )


# ---------------------------------------------------------------------------
# View implementations
# ---------------------------------------------------------------------------

def _view_contracts(pool: object, q: str) -> ToolResult:
    t0 = time.time()
    queries = [
        ("Contract Awards", CONTRACT_AWARDS_SQL, 3),
        ("DDC Vendor Payments", DDC_VENDOR_PAYMENTS_SQL, 3),
        ("COVID Emergency Contracts", COVID_CONTRACTS_SQL, 3),
        ("Federal Contracts (USASpending)", USASPENDING_CONTRACTS_SQL, 3),
        ("Federal Grants (USASpending)", USASPENDING_GRANTS_SQL, 3),
        ("NYS Procurement (State)", NYS_PROCUREMENT_STATE_SQL, 3),
        ("NYS Procurement (Local)", NYS_PROCUREMENT_LOCAL_SQL, 3),
    ]
    sections = _run_queries(pool, q, queries)
    return _build_result(q, "contracts", sections, time.time() - t0)


def _view_permits(pool: object, q: str) -> ToolResult:
    t0 = time.time()
    queries = [
        ("Film Permits", FILM_PERMITS_SQL, 4),
        ("Liquor Licenses (NYS)", LIQUOR_AUTHORITY_SQL, 3),
    ]
    sections = _run_queries(pool, q, queries)
    return _build_result(q, "permits", sections, time.time() - t0)


def _view_jobs(pool: object, q: str) -> ToolResult:
    t0 = time.time()
    queries = [
        ("Citywide Payroll", CITYWIDE_PAYROLL_SQL, 3),
        ("Civil Service Active Lists", CIVIL_SERVICE_ACTIVE_SQL, 3),
        ("Civil List", CIVIL_LIST_SQL, 3),
        ("Civil Service Titles", CIVIL_SERVICE_TITLES_SQL, 2),
        ("NYC Job Postings", NYC_JOBS_SQL, 3),
    ]
    sections = _run_queries(pool, q, queries)
    return _build_result(q, "jobs", sections, time.time() - t0)


def _view_budget(pool: object, q: str) -> ToolResult:
    t0 = time.time()
    queries = [
        ("Revenue Budget", REVENUE_BUDGET_SQL, 3),
        ("NYS IDA Projects", NYS_IDA_PROJECTS_SQL, 3),
        ("NYS Procurement (State)", NYS_PROCUREMENT_STATE_SQL, 3),
        ("NYS Procurement (Local)", NYS_PROCUREMENT_LOCAL_SQL, 3),
    ]
    sections = _run_queries(pool, q, queries)
    return _build_result(q, "budget", sections, time.time() - t0)


def _view_events(pool: object, q: str) -> ToolResult:
    t0 = time.time()
    queries = [
        ("Permitted Events", PERMITTED_EVENTS_SQL, 4),
        ("City Record Notices", CITY_RECORD_SQL, 4),
    ]
    sections = _run_queries(pool, q, queries)
    return _build_result(q, "events", sections, time.time() - t0)


# ---------------------------------------------------------------------------
# Main tool function
# ---------------------------------------------------------------------------

def civic(
    query: Annotated[str, Field(
        description="Company name, agency name, keyword, or location, e.g. 'AECOM', 'DOT', 'film permit Brooklyn'",
        examples=["AECOM", "DOT", "film permit Brooklyn", "sanitation", "lifeguard"],
    )],
    view: Annotated[
        Literal["contracts", "permits", "jobs", "budget", "events"],
        Field(
            default="contracts",
            description="'contracts' returns city and federal contracts with vendors and amounts. 'permits' returns film permits, liquor licenses, and event permits. 'jobs' returns city employment data, payroll, and civil service titles. 'budget' returns revenue budget, IDA projects, and procurement data. 'events' returns permitted events and city record notices.",
        )
    ] = "contracts",
    ctx: Context = None,
) -> ToolResult:
    """Look up NYC city government operations including contracts, permits, employment, budget, and events. Use for any question about city spending, vendor contracts, film permits, city jobs, or public events. Do NOT use for political donation networks (use network with type='political') or building permits (use building with view='enforcement'). Default returns contract data matching the query."""
    query = query.strip()
    if not query or len(query) < 2:
        raise ToolError("Query must be at least 2 characters.")

    pool = ctx.lifespan_context["pool"]

    dispatch = {
        "contracts": _view_contracts,
        "permits": _view_permits,
        "jobs": _view_jobs,
        "budget": _view_budget,
        "events": _view_events,
    }
    handler = dispatch[view]
    return handler(pool, query)
