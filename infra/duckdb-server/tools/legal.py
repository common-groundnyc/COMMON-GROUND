"""legal() super tool — litigation, settlements, hearings, inmates, claims.

Views:
  full        → all legal proceedings matching query
  litigation  → civil lawsuits (parties, outcomes, payouts)
  settlements → settlement payments by the city
  hearings    → OATH administrative hearings and trials
  inmates     → daily inmate population data
  claims      → claims filed against NYC
"""

import time
from typing import Annotated, Literal

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from pydantic import Field

from shared.db import safe_query
from shared.formatting import make_result, format_text_table
from shared.types import MAX_LLM_ROWS

# ---------------------------------------------------------------------------
# SQL constants — civil litigation
# ---------------------------------------------------------------------------

CIVIL_LITIGATION_SQL = """
    SELECT matter_name, court, judge,
           plaintiffs_petitioners AS plaintiffs,
           defendants_respondents_firms AS defendants,
           claim_type, case_status,
           total_city_payout_amt AS city_payout,
           docket_index, filed_date
    FROM lake.city_government.civil_litigation
    WHERE UPPER(matter_name) ILIKE ?
       OR UPPER(defendants_respondents_firms) ILIKE ?
       OR UPPER(plaintiffs_petitioners) ILIKE ?
       OR UPPER(judge) ILIKE ?
    ORDER BY filed_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — settlement payments
# ---------------------------------------------------------------------------

SETTLEMENT_PAYMENTS_SQL = """
    SELECT claim_number, agency_name, payment_amount,
           settlement_date, claim_category, description
    FROM lake.city_government.settlement_payments
    WHERE UPPER(agency_name) ILIKE ?
       OR UPPER(description) ILIKE ?
       OR UPPER(claim_category) ILIKE ?
    ORDER BY settlement_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — OATH hearings
# ---------------------------------------------------------------------------

OATH_HEARINGS_SQL = """
    SELECT respondent_first_name || ' ' || respondent_last_name AS respondent,
           issuing_agency, hearing_date, hearing_status, hearing_result,
           charge_1_code_description AS charge,
           TRY_CAST(total_violation_amount AS DOUBLE) AS penalty,
           TRY_CAST(paid_amount AS DOUBLE) AS paid,
           violation_location_house || ' ' || violation_location_street_name AS location
    FROM lake.city_government.oath_hearings
    WHERE UPPER(respondent_last_name) ILIKE ?
       OR UPPER(respondent_first_name || ' ' || respondent_last_name) ILIKE ?
       OR UPPER(issuing_agency) ILIKE ?
       OR UPPER(charge_1_code_description) ILIKE ?
    ORDER BY hearing_date DESC NULLS LAST
    LIMIT {limit}
"""

OATH_TRIALS_SQL = """
    SELECT respondent_first_name || ' ' || respondent_last_name AS respondent,
           issuing_agency, hearing_date, hearing_status, hearing_result,
           charge_1_code_description AS charge,
           TRY_CAST(total_violation_amount AS DOUBLE) AS penalty,
           TRY_CAST(paid_amount AS DOUBLE) AS paid,
           violation_location_house || ' ' || violation_location_street_name AS location
    FROM lake.city_government.oath_trials
    WHERE UPPER(respondent_last_name) ILIKE ?
       OR UPPER(respondent_first_name || ' ' || respondent_last_name) ILIKE ?
       OR UPPER(issuing_agency) ILIKE ?
       OR UPPER(charge_1_code_description) ILIKE ?
    ORDER BY hearing_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — daily inmates
# ---------------------------------------------------------------------------

DAILY_INMATES_SQL = """
    SELECT inmateid, admitted_dt, discharged_dt,
           top_charge, race, gender, age,
           custody_level, bradh
    FROM lake.public_safety.daily_inmates
    WHERE UPPER(top_charge) ILIKE ?
       OR UPPER(bradh) ILIKE ?
    ORDER BY admitted_dt DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — NYC claims report
# ---------------------------------------------------------------------------

NYC_CLAIMS_SQL = """
    SELECT agency, claim_type, claim_stage,
           claim_received_date, disposition_date,
           total_claim_amount, total_paid_amount
    FROM lake.public_safety.nyc_claims_report
    WHERE UPPER(agency) ILIKE ?
       OR UPPER(claim_type) ILIKE ?
    ORDER BY claim_received_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — CCRB penalties
# ---------------------------------------------------------------------------

CCRB_PENALTIES_SQL = """
    SELECT complaint_id, nypd_disposition, penalty_requested,
           penalty_received, penalty_desc
    FROM lake.public_safety.ccrb_penalties
    WHERE UPPER(penalty_desc) ILIKE ?
       OR UPPER(nypd_disposition) ILIKE ?
    ORDER BY complaint_id DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — NYS COELIG enforcement
# ---------------------------------------------------------------------------

NYS_COELIG_SQL = """
    SELECT respondent, agency, status,
           violation_type, penalty_amount, order_date
    FROM lake.city_government.nys_coelig_enforcement
    WHERE UPPER(respondent) ILIKE ?
       OR UPPER(agency) ILIKE ?
       OR UPPER(violation_type) ILIKE ?
    ORDER BY order_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — vacate/relocation
# ---------------------------------------------------------------------------

VACATE_RELOCATION_SQL = """
    SELECT primaryaddress, vacate_type, vacate_effective_date,
           number_of_vacated_units, primary_reason_for_vacate
    FROM lake.housing.vacate_relocation
    WHERE UPPER(primaryaddress) ILIKE ?
       OR UPPER(primary_reason_for_vacate) ILIKE ?
       OR UPPER(vacate_type) ILIKE ?
    ORDER BY vacate_effective_date DESC NULLS LAST
    LIMIT {limit}
"""

# ---------------------------------------------------------------------------
# SQL constants — emergency repair (HWO + OMO)
# ---------------------------------------------------------------------------

EMERGENCY_REPAIR_HWO_SQL = """
    SELECT housenumber || ' ' || streetname AS address,
           emergencyrepairtype, approveddate, closeddate,
           ownerfirstname || ' ' || ownerlastname AS owner
    FROM lake.housing.emergency_repair_hwo
    WHERE UPPER(ownerlastname) ILIKE ?
       OR UPPER(streetname) ILIKE ?
       OR UPPER(emergencyrepairtype) ILIKE ?
    ORDER BY approveddate DESC NULLS LAST
    LIMIT {limit}
"""

EMERGENCY_REPAIR_OMO_SQL = """
    SELECT housenumber || ' ' || streetname AS address,
           emergencyrepairtype, approveddate, closeddate,
           ownerfirstname || ' ' || ownerlastname AS owner
    FROM lake.housing.emergency_repair_omo
    WHERE UPPER(ownerlastname) ILIKE ?
       OR UPPER(streetname) ILIKE ?
       OR UPPER(emergencyrepairtype) ILIKE ?
    ORDER BY approveddate DESC NULLS LAST
    LIMIT {limit}
"""


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _like(query: str) -> str:
    """Wrap query in ILIKE wildcards."""
    return f"%{query.strip().upper()}%"


def _query_section(
    pool,
    label: str,
    sql_template: str,
    params: list,
    limit: int = MAX_LLM_ROWS,
) -> tuple[str, list, list]:
    """Run a safe_query and return (label, cols, rows)."""
    sql = sql_template.replace("{limit}", str(limit))
    cols, rows = safe_query(pool, sql, params)
    return label, cols, rows


# ---------------------------------------------------------------------------
# View dispatchers
# ---------------------------------------------------------------------------

def _view_litigation(pool, query: str) -> tuple[list, dict]:
    """Civil litigation + NYS COELIG enforcement."""
    pat = _like(query)
    sections = []
    structured = {}

    label, cols, rows = _query_section(
        pool, "Civil Litigation", CIVIL_LITIGATION_SQL,
        [pat, pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["civil_litigation"] = [dict(zip(cols, r)) for r in rows] if cols else []

    label, cols, rows = _query_section(
        pool, "NYS COELIG Enforcement", NYS_COELIG_SQL,
        [pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["nys_coelig_enforcement"] = [dict(zip(cols, r)) for r in rows] if cols else []

    return sections, structured


def _view_settlements(pool, query: str) -> tuple[list, dict]:
    """Settlement payments by the city."""
    pat = _like(query)
    sections = []
    structured = {}

    label, cols, rows = _query_section(
        pool, "Settlement Payments", SETTLEMENT_PAYMENTS_SQL,
        [pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["settlement_payments"] = [dict(zip(cols, r)) for r in rows] if cols else []

    return sections, structured


def _view_hearings(pool, query: str) -> tuple[list, dict]:
    """OATH hearings + trials."""
    pat = _like(query)
    sections = []
    structured = {}

    label, cols, rows = _query_section(
        pool, "OATH Hearings", OATH_HEARINGS_SQL,
        [pat, pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["oath_hearings"] = [dict(zip(cols, r)) for r in rows] if cols else []

    label, cols, rows = _query_section(
        pool, "OATH Trials", OATH_TRIALS_SQL,
        [pat, pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["oath_trials"] = [dict(zip(cols, r)) for r in rows] if cols else []

    return sections, structured


def _view_inmates(pool, query: str) -> tuple[list, dict]:
    """Daily inmate population."""
    pat = _like(query)
    sections = []
    structured = {}

    label, cols, rows = _query_section(
        pool, "Daily Inmates", DAILY_INMATES_SQL,
        [pat, pat],
    )
    sections.append((label, cols, rows))
    structured["daily_inmates"] = [dict(zip(cols, r)) for r in rows] if cols else []

    return sections, structured


def _view_claims(pool, query: str) -> tuple[list, dict]:
    """Claims against NYC + CCRB penalties."""
    pat = _like(query)
    sections = []
    structured = {}

    label, cols, rows = _query_section(
        pool, "NYC Claims Report", NYC_CLAIMS_SQL,
        [pat, pat],
    )
    sections.append((label, cols, rows))
    structured["nyc_claims_report"] = [dict(zip(cols, r)) for r in rows] if cols else []

    label, cols, rows = _query_section(
        pool, "CCRB Penalties", CCRB_PENALTIES_SQL,
        [pat, pat],
    )
    sections.append((label, cols, rows))
    structured["ccrb_penalties"] = [dict(zip(cols, r)) for r in rows] if cols else []

    # Also include vacate/relocation and emergency repairs as related legal actions
    label, cols, rows = _query_section(
        pool, "Vacate & Relocation Orders", VACATE_RELOCATION_SQL,
        [pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["vacate_relocation"] = [dict(zip(cols, r)) for r in rows] if cols else []

    label, cols, rows = _query_section(
        pool, "Emergency Repairs (HWO)", EMERGENCY_REPAIR_HWO_SQL,
        [pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["emergency_repair_hwo"] = [dict(zip(cols, r)) for r in rows] if cols else []

    label, cols, rows = _query_section(
        pool, "Emergency Repairs (OMO)", EMERGENCY_REPAIR_OMO_SQL,
        [pat, pat, pat],
    )
    sections.append((label, cols, rows))
    structured["emergency_repair_omo"] = [dict(zip(cols, r)) for r in rows] if cols else []

    return sections, structured


def _view_full(pool, query: str) -> tuple[list, dict]:
    """All legal proceedings — union of all views."""
    all_sections = []
    all_structured = {}

    for view_fn in (_view_litigation, _view_settlements, _view_hearings, _view_inmates, _view_claims):
        sections, structured = view_fn(pool, query)
        all_sections.extend(sections)
        all_structured.update(structured)

    return all_sections, all_structured


# ---------------------------------------------------------------------------
# Main tool
# ---------------------------------------------------------------------------

_VIEW_DISPATCH = {
    "full": _view_full,
    "litigation": _view_litigation,
    "settlements": _view_settlements,
    "hearings": _view_hearings,
    "inmates": _view_inmates,
    "claims": _view_claims,
}


async def legal(
    query: Annotated[str, Field(
        description="Person name, agency name, or keyword to search legal proceedings, e.g. 'NYPD', 'Steven Croman', 'lead paint'",
        examples=["NYPD", "Steven Croman", "lead paint", "DOE", "excessive force"],
    )],
    view: Annotated[
        Literal["full", "litigation", "settlements", "hearings", "inmates", "claims"],
        Field(
            default="full",
            description=(
                "'full' returns all legal proceedings matching the query. "
                "'litigation' returns civil lawsuits with parties and outcomes. "
                "'settlements' returns settlement payments by the city with amounts and reasons. "
                "'hearings' returns OATH administrative hearings and trials. "
                "'inmates' returns daily inmate population data. "
                "'claims' returns claims filed against NYC."
            ),
        ),
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Search NYC legal proceedings, court records, and city settlements. Returns litigation, OATH hearings, settlement payments, and claims against the city. Use for any question about lawsuits, court cases, administrative hearings, or city payouts. Do NOT use for individual person background checks (use entity with role='background') or police misconduct records (use entity with role='cop'). Default returns all legal proceedings matching the query."""
    t0 = time.perf_counter()

    if not query or len(query.strip()) < 2:
        raise ToolError("Query must be at least 2 characters.")

    pool = ctx.request_context.lifespan_context["pool"]
    dispatch_fn = _VIEW_DISPATCH[view]
    sections, structured = dispatch_fn(pool, query)

    # Build text output from sections
    lines = [f"## Legal proceedings for '{query}' (view={view})"]
    total_rows = 0

    for label, cols, rows in sections:
        if not rows:
            continue
        total_rows += len(rows)
        lines.append(f"\n### {label} ({len(rows)} results)")
        lines.append(format_text_table(cols, rows))

    if total_rows == 0:
        lines.append("\nNo legal proceedings found matching this query.")

    elapsed = time.perf_counter() - t0
    lines.append(f"\n_Completed in {elapsed:.1f}s_")

    return ToolResult(
        content="\n".join(lines),
        structured_content=structured,
        meta={"total_rows": total_rows, "view": view, "elapsed_seconds": round(elapsed, 2)},
    )
